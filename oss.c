// • 1Start by creating a Makefile that compiles and builds the two executables: oss and user_proc.
// • 2Implement clock in shared memory; possibly reuse the one from last project.
// • 3Have oss create resource descriptors and populate them with instances.
// • 4Use message queues to communicate requests, allocation, and release of resources to children. Start by testing one child
// process just requesting one resource and then terminating
// • 5Have the child processes now stick around until their time is up, requesting and releases
// • 6Now test for multiple children requesting and releases
// • 7If all is working now, implement deadlock detection to detect when a deadlock exists
// • 8Lastly, implement oss terminating one of the deadlocked processes
// • 9Keep track of output statistics in log file.
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <sys/msg.h>
#include <signal.h>
#include <errno.h>
#include <stdarg.h>
#include <time.h>

//assignment says there are 10 resource classes and 5 instances of each resource class
#define RESOURCE_CLASSES 10
#define INSTANCES_PER_RESOURCE 5 

#define NANOPERSEC 1000000000LL //1 second = 1 billion nanoseconds
#define TURN_INCREMENT_NS 10000000LL

#define MAX_PCB_SIZE 18
#define PRINT_INTERVAL_NS 500000000LL
#define DEADLOCK_CHECK_INTERVAL_NS NANOPERSEC
#define MAX_LOG_LINES 10000

const size_t BUFF_SZ = sizeof(unsigned int) * 2;

static int msg_id_global = -1;
// global variable to store the message queue id

static int shm_id_global = -1; 
//global variable to store the shared memory id

static unsigned int *clock_global = NULL;
// Global variable to store the pointer to the shared memory clock.

//used for logging statistics for log.txt later
static FILE *logFileGlobal = NULL;
static int logLineCount = 0;
static int logLimitMessagePrinted = 0;

static int totalProcessesLaunched = 0;
static int totalProcessesTerminatedNormally = 0;
static int totalProcessesKilledForDeadlock = 0;

static int totalRequests = 0;
static int totalGrantedImmediately = 0;
static int totalBlockedRequests = 0;
static int totalGrantedAfterWaiting = 0;
static int totalReleases = 0;

static int totalDeadlockDetectionRuns = 0;
static int totalDeadlocksDetected = 0;

static int totalGrantedRequests = 0;

struct Message {
    //used for communication between oss and worker processes. 
    //We will use this message structure to send 
    //requests, allocations, and releases of resources between oss and worker processes.
    long mtype; // message type, must be > 0
    int value; 
    // value stores the action/resource code.
    // In this step:
    //   value > 0 means a request or grant.
    //   value == 0 means termination.
    //   Later, value < 0 will mean release.
    // Because 0 is reserved for termination, resource messages use 1-based numbering:
    //   1 = R0, 2 = R1, ..., 10 = R9.

    int pid; // we will use this field to indicate the pid of the worker process that is sending the message, 
    //so that oss can know which worker process is making the request or release.
    int slot; //we will use this field to indicate the slot number in the PCB table of the worker process that is sending the message,
    //so that oss can update the PCB table accordingly when it receives the message from the worker process.
};

struct ResourceDescriptor {
    //total instances of this resource class in the system
    int totalInstances;
    int availableInstances;
};

struct PCB {
    int occupied;
    pid_t pid;
    int localPid;
    int startSeconds;
    int startNano;

    int blocked;
    int requestedResource; 
};

struct ResourceDescriptor resourceTable[RESOURCE_CLASSES];
// Keeps track of total and available instances for each resource class.

struct PCB pcbTable[MAX_PCB_SIZE];

int resourceAllocation[MAX_PCB_SIZE][RESOURCE_CLASSES];

void logBoth(const char *format, ...) {
    va_list args;

    va_start(args, format);
    vprintf(format, args);
    va_end(args);

    fflush(stdout);

    if (logFileGlobal != NULL) {
        if (logLineCount < MAX_LOG_LINES) {
            va_start(args, format);
            vfprintf(logFileGlobal, format, args);
            va_end(args);

            fflush(logFileGlobal);
            logLineCount++;
        } else if (!logLimitMessagePrinted) {
            fprintf(logFileGlobal, "OSS: Log line limit reached. Further file logging stopped.\n");
            fflush(logFileGlobal);
            logLimitMessagePrinted = 1;
        }
    }
}

long long secondsToNS(double seconds) {
    if (seconds <= 0) {
        return 0;
    }

    return (long long)(seconds * NANOPERSEC);
}


void clearPCBEntry(int slot) {
    pcbTable[slot].occupied = 0;
    pcbTable[slot].pid = 0;
    pcbTable[slot].localPid = 0;
    pcbTable[slot].startSeconds = 0;
    pcbTable[slot].startNano = 0;

    pcbTable[slot].blocked = 0;
    pcbTable[slot].requestedResource = -1;
    //requestedResource = -1 means the process is not currently waiting on any resource
}

void initPCBTable() {
    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        clearPCBEntry(i);
    }
}

int findFreePCBSlot() {
    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied == 0) {
            return i;
        }
    }

    return -1;
}


void cleanupIPC() {
    if (clock_global != NULL && clock_global != (void *)-1)  {
//only detach shm if we are currently attached to it and not an error value
//we do this because if we are not attached to shm, then we should not try to detach from it later in the program, 
//because that would cause an error. 
//We also check if clock_global is not (void *)-1, 
//which is the value returned by shmat when there is an error in attaching to shared memory. 
//If we are currently attached to shared memory and it is not an error value, then we can safely detach from it. 
        shmdt(clock_global); // Detach from shared memory
        clock_global = NULL;
    }
    if (shm_id_global != -1) {
        //Mark the shared memory segment for removal.
        //It will be deleted after all attached processes detach from it.
        shmctl(shm_id_global, IPC_RMID, NULL); // Mark the shared memory segment for deletion
        shm_id_global = -1;
    }
    if (msg_id_global != -1) {
        //Remove msg queue from system.
        msgctl(msg_id_global, IPC_RMID, NULL); // Mark the message queue for deletion
        msg_id_global = -1;
    }
}

void releaseAllResources(int slot) {
    if (slot < 0 || slot >= MAX_PCB_SIZE) {
        logBoth("OSS: invalid slot number %d\n", slot);
        return;
    }

    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        if (resourceAllocation[slot][i] > 0) {
            resourceTable[i].availableInstances += resourceAllocation[slot][i];
            resourceAllocation[slot][i] = 0;
        }
    }
}

void killAllRunningChildren() {
    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied && pcbTable[i].pid > 0) {
            kill(pcbTable[i].pid, SIGTERM);
            waitpid(pcbTable[i].pid, NULL, 0);

            releaseAllResources(i);
            clearPCBEntry(i);
        }
    }
}

void signal_handler(int sig) {
    logBoth("OSS: received signal %d, shutting down...\n", sig);

    killAllRunningChildren();
    cleanupIPC();

    if (logFileGlobal != NULL) {
        fclose(logFileGlobal);
        logFileGlobal = NULL;
    }

    exit(1);
}

long long getClockNS(unsigned int *clock) {
    // Convert the clock time to nanoseconds and return it as a long long value.
    // We will use this function to get the current time in nanoseconds from the shared memory clock, 
    // which is stored as two unsigned int values (seconds and nanoseconds).
    return ((long long)clock[0] * NANOPERSEC) + clock[1];
}

void setClockFromNS(unsigned int *clock, long long timeNS) {
    if (timeNS < 0) {
        timeNS = 0;
    }

    clock[0] = (unsigned int)(timeNS / NANOPERSEC); // Set seconds
    clock[1] = (unsigned int)(timeNS % NANOPERSEC); // Set nanoseconds
}

void addToClock(unsigned int *clock, long long timeToAddNS) {
    long long currentTimeNS = getClockNS(clock);
    long long newTimeNS = currentTimeNS + timeToAddNS;

    setClockFromNS(clock, newTimeNS);
}

void initializeResourceTable() {
    // Initialize the resource table with 5 total and 5 available instances
    // for each of the 10 resource classes.
    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        resourceTable[i].totalInstances = INSTANCES_PER_RESOURCE;
        resourceTable[i].availableInstances = INSTANCES_PER_RESOURCE;
    }
}

void printResourceTable() {
    logBoth("\nOSS: Current resource table.\n");
    logBoth("Resource:   ");

    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        logBoth("R%-3d", i);
    }

    logBoth("\nTotal:      ");

    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        logBoth("%-4d", resourceTable[i].totalInstances);
    }

    logBoth("\nAvailable:  ");

    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        logBoth("%-4d", resourceTable[i].availableInstances);
    }

    logBoth("\n\n");
}

void printProcessTable(unsigned int *clock) {
    logBoth("\nOSS PID:%d SysClockS: %u SysClockNano: %u\n",
            getpid(),
            clock[0],
            clock[1]);

    logBoth("Process Table:\n");
    logBoth("Entry Occ LocalPID RealPID StartS StartN Blocked ReqResource\n");

    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        logBoth("%5d %3d %8d %7d %6d %6d %7d %11d\n",
                i,
                pcbTable[i].occupied,
                pcbTable[i].localPid,
                (int)pcbTable[i].pid,
                pcbTable[i].startSeconds,
                pcbTable[i].startNano,
                pcbTable[i].blocked,
                pcbTable[i].requestedResource);
    }
}

void printBlockedProcesses() {
    logBoth("OSS: Blocked processes: [ ");

    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied && pcbTable[i].blocked) {
            logBoth("P%d(slot %d waiting R%d) ",
                    pcbTable[i].localPid,
                    i,
                    pcbTable[i].requestedResource);
        }
    }

    logBoth("]\n");
}

void printAllocatedResourcesTable() {
    logBoth("\nOSS: Current resources allocated to each process\n");
    logBoth("        ");

    for (int r = 0; r < RESOURCE_CLASSES; r++) {
        logBoth("R%-3d", r);
    }

    logBoth("\n");

    for (int slot = 0; slot < MAX_PCB_SIZE; slot++) {
        if (pcbTable[slot].occupied) {
            logBoth("P%-7d", pcbTable[slot].localPid);

            for (int r = 0; r < RESOURCE_CLASSES; r++) {
                logBoth("%-4d", resourceAllocation[slot][r]);
            }

            logBoth("\n");
        }
    }

    logBoth("\n");
}

void recordGrantedRequest() {
    totalGrantedRequests++;

    if (totalGrantedRequests % 20 == 0) {
        logBoth("OSS: %d total granted requests reached. Printing allocation table.\n",
                totalGrantedRequests);
        printAllocatedResourcesTable();
    }
}

void initResourceAllocation() {
        // Initialize the resource allocation table for every possible PCB slot.

    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        for (int j = 0; j < RESOURCE_CLASSES; j++) {
            resourceAllocation[i][j] = 0;
        }
    }
}

int grantResource(int slot, int resourceNumber) {
    if (slot < 0 || slot >= MAX_PCB_SIZE) {
        logBoth("OSS: invalid slot number %d\n", slot);
        return 0;
    }

    if (resourceNumber < 0 || resourceNumber >= RESOURCE_CLASSES) {
        logBoth("OSS: invalid resource number %d\n", resourceNumber);
        return 0;
    }

    if (resourceTable[resourceNumber].availableInstances > 0) {
        resourceTable[resourceNumber].availableInstances--;
        resourceAllocation[slot][resourceNumber]++;
        return 1;
    }

    return 0;
}

int releaseOneResource(int slot, int resourceNumber) {
    if (slot < 0 || slot >= MAX_PCB_SIZE) {
        logBoth("OSS: invalid slot number %d\n", slot);
        return 0;
    }

    if (resourceNumber < 0 || resourceNumber >= RESOURCE_CLASSES) {
        logBoth("OSS: invalid resource number %d\n", resourceNumber);
        return 0;
    }

    if (resourceAllocation[slot][resourceNumber] > 0) {
        resourceAllocation[slot][resourceNumber]--;
        resourceTable[resourceNumber].availableInstances++;
        return 1;
    }

    return 0;
}

int sendTurnMessage(int msg_id, pid_t childPid, int slot) {
    struct Message msgToChild;

    msgToChild.mtype = childPid;
    // Child receives messages where mtype equals its own pid.

    msgToChild.value = 1;
    // For Step 6, this value only means "take another turn."
    // user_proc does not treat this as a resource request.

    msgToChild.pid = getpid();
    msgToChild.slot = slot;

    if (msgsnd(msg_id, &msgToChild, sizeof(struct Message) - sizeof(long), 0) == -1) {
        perror("OSS: msgsnd failed when sending turn message");
        return 0;
    }

    return 1;
}

int sendGrantMessage(int msg_id, int childPid, int slot, int value) {
    struct Message grantMessage;

    grantMessage.mtype = childPid;
    // Child receives this grant/deny message using its own pid as mtype.

    grantMessage.value = value;
    // If value matches the child's request, user_proc treats it as granted.
    // If value is 0, user_proc treats the request as not granted.

    grantMessage.pid = getpid();
    grantMessage.slot = slot;

    if (msgsnd(msg_id, &grantMessage, sizeof(struct Message) - sizeof(long), 0) == -1) {
        perror("OSS: msgsnd failed when sending grant message");
        return 0;
    }

    return 1;
}

void blockProcess(int slot, int resourceNumber) {
    if (slot < 0 || slot >= MAX_PCB_SIZE) {
        logBoth("OSS: Error in blockProcess: invalid slot %d\n", slot);
        return;
    }

    pcbTable[slot].blocked = 1;
    pcbTable[slot].requestedResource = resourceNumber;

    logBoth("OSS: Blocking P%d PID %d in slot %d waiting for R%d\n",
            pcbTable[slot].localPid,
            pcbTable[slot].pid,
            slot,
            resourceNumber);
}

void checkBlockedProcesses(int msg_id) {
    for (int slot = 0; slot < MAX_PCB_SIZE; slot++) {
        if (!pcbTable[slot].occupied || !pcbTable[slot].blocked) {
            continue;
        }

        int resourceNumber = pcbTable[slot].requestedResource;

        if (resourceNumber < 0 || resourceNumber >= RESOURCE_CLASSES) {
            continue;
        }

        if (resourceTable[resourceNumber].availableInstances > 0) {
            if (grantResource(slot, resourceNumber)) {
                if (sendGrantMessage(msg_id,
                                     pcbTable[slot].pid,
                                     slot,
                                     resourceNumber + 1)) {
                    pcbTable[slot].blocked = 0;
                    pcbTable[slot].requestedResource = -1;

                    totalGrantedAfterWaiting++;
                    recordGrantedRequest();

                    logBoth("OSS: Unblocking P%d PID %d slot %d and granting R%d\n",
                            pcbTable[slot].localPid,
                            pcbTable[slot].pid,
                            slot,
                            resourceNumber);
                } else {
                    releaseOneResource(slot, resourceNumber);
                }
            }
        }
    }
}

int countBlockedProcesses() {
    int count = 0;

    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied && pcbTable[i].blocked) {
            count++;
        }
    }

    return count;
}

int runDeadlockDetection(unsigned int *clock, int deadlockedSlots[]) {
    int work[RESOURCE_CLASSES];
    int finish[MAX_PCB_SIZE];
    int deadlockedCount = 0;

    totalDeadlockDetectionRuns++;

    for (int r = 0; r < RESOURCE_CLASSES; r++) {
        work[r] = resourceTable[r].availableInstances;
    }

    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        if (!pcbTable[i].occupied) {
            finish[i] = 1;
        } else {
            finish[i] = 0;
        }
    }

    int changed = 1;

    while (changed) {
        changed = 0;

        for (int i = 0; i < MAX_PCB_SIZE; i++) {
            if (finish[i]) {
                continue;
            }

            int canFinish = 1;

            if (pcbTable[i].blocked) {
                int requested = pcbTable[i].requestedResource;

                if (requested < 0 ||
                    requested >= RESOURCE_CLASSES ||
                    work[requested] <= 0) {
                    canFinish = 0;
                }
            }

            if (canFinish) {
                for (int r = 0; r < RESOURCE_CLASSES; r++) {
                    work[r] += resourceAllocation[i][r];
                }

                finish[i] = 1;
                changed = 1;
            }
        }
    }

    logBoth("\nOSS: Deadlock detection run %d\n", totalDeadlockDetectionRuns);
    logBoth("OSS: Running deadlock detection at time %u:%u\n",
            clock[0],
            clock[1]);

    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied && !finish[i]) {
            if (deadlockedCount == 0) {
                logBoth("OSS: Deadlock detected involving:");
            }

            logBoth(" P%d(slot %d)",
                    pcbTable[i].localPid,
                    i);

            deadlockedSlots[deadlockedCount] = i;
            deadlockedCount++;
        }
    }

    if (deadlockedCount > 0) {
        totalDeadlocksDetected++;
        logBoth("\n");
    } else {
        logBoth("OSS: No deadlock detected\n");
    }

    return deadlockedCount;
}

void terminateDeadlockedProcess(int victimSlot, int *activeChildren, int msg_id) {
    if (victimSlot < 0 || victimSlot >= MAX_PCB_SIZE) {
        logBoth("OSS: Error in terminateDeadlockedProcess: invalid slot %d\n",
                victimSlot);
        return;
    }

    if (!pcbTable[victimSlot].occupied) {
        logBoth("OSS: Error in terminateDeadlockedProcess: slot %d is not occupied\n",
                victimSlot);
        return;
    }

    logBoth("OSS: Attempting to resolve deadlock...\n");
    logBoth("OSS: Killing process P%d PID %d in slot %d\n",
            pcbTable[victimSlot].localPid,
            pcbTable[victimSlot].pid,
            victimSlot);

    logBoth("OSS: Resources released are:");

    int releasedSomething = 0;

    for (int r = 0; r < RESOURCE_CLASSES; r++) {
        if (resourceAllocation[victimSlot][r] > 0) {
            logBoth(" R%d:%d", r, resourceAllocation[victimSlot][r]);
            releasedSomething = 1;
        }
    }

    if (!releasedSomething) {
        logBoth(" none");
    }

    logBoth("\n");

    kill(pcbTable[victimSlot].pid, SIGTERM);
    waitpid(pcbTable[victimSlot].pid, NULL, 0);

    releaseAllResources(victimSlot);
    clearPCBEntry(victimSlot);

    (*activeChildren)--;

    totalProcessesKilledForDeadlock++;

    checkBlockedProcesses(msg_id);
}

void printFinalReport(unsigned int *clock) {
    double immediateGrantPercent = 0.0;

    if (totalRequests > 0) {
        immediateGrantPercent =
            ((double)totalGrantedImmediately / (double)totalRequests) * 100.0;
    }

    logBoth("\nOSS: Final Report\n");
    logBoth("OSS: Final simulated time: %u:%u\n", clock[0], clock[1]);
    logBoth("OSS: Processes launched: %d\n", totalProcessesLaunched);
    logBoth("OSS: Processes terminated normally: %d\n", totalProcessesTerminatedNormally);
    logBoth("OSS: Processes killed to resolve deadlock: %d\n", totalProcessesKilledForDeadlock);
    logBoth("OSS: Total resource requests: %d\n", totalRequests);
    logBoth("OSS: Requests granted immediately: %d\n", totalGrantedImmediately);
    logBoth("OSS: Requests blocked: %d\n", totalBlockedRequests);
    logBoth("OSS: Requests granted after waiting: %d\n", totalGrantedAfterWaiting);
    logBoth("OSS: Total resource releases: %d\n", totalReleases);
    logBoth("OSS: Deadlock detection runs: %d\n", totalDeadlockDetectionRuns);
    logBoth("OSS: Deadlocks detected: %d\n", totalDeadlocksDetected);
    logBoth("OSS: Percentage of requests granted immediately: %.2f%%\n",
            immediateGrantPercent);
    logBoth("OSS: Log write calls used: %d\n", logLineCount);
}

int launchChildProcess(double t,
                       unsigned int *clock,
                       int *launchedChildren,
                       int *activeChildren) {
    int slot = findFreePCBSlot();

    if (slot == -1) {
        logBoth("OSS: No free PCB slot available\n");
        return 0;
    }

    long long lifetimeNS = secondsToNS(t);

    if (lifetimeNS <= 0) {
        lifetimeNS = NANOPERSEC;
    }

    int lifetimeSec = (int)(lifetimeNS / NANOPERSEC);
    int lifetimeNano = (int)(lifetimeNS % NANOPERSEC);

    char secStr[32];
    char nanoStr[32];

    snprintf(secStr, sizeof(secStr), "%d", lifetimeSec);
    snprintf(nanoStr, sizeof(nanoStr), "%d", lifetimeNano);

    pid_t pid = fork();

    if (pid == -1) {
        perror("OSS: fork failed");
        return 0;
    }

    if (pid == 0) {
        execl("./user_proc", "user_proc", secStr, nanoStr, (char *)NULL);

        perror("OSS: execl failed");
        exit(EXIT_FAILURE);
    }

    pcbTable[slot].occupied = 1;
    pcbTable[slot].pid = pid;
    pcbTable[slot].localPid = (*launchedChildren) + 1;
    pcbTable[slot].startSeconds = clock[0];
    pcbTable[slot].startNano = clock[1];
    pcbTable[slot].blocked = 0;
    pcbTable[slot].requestedResource = -1;

    logBoth("OSS: Forked child P%d with PID %d in slot %d at time %u:%u\n",
            pcbTable[slot].localPid,
            pid,
            slot,
            clock[0],
            clock[1]);

    (*launchedChildren)++;
    (*activeChildren)++;
    totalProcessesLaunched++;

    return 1;
}

int main(int argc, char *argv[]) {

    int n = 1;  
    int s = 1;  
    float t = 1.0f; 
    float i = 0.0f;
    char logFile[256] = "log.txt"; 

    int opt; 
    while ((opt = getopt(argc, argv, "hn:s:t:i:f:")) != -1) {
        switch (opt) {
            case 'h':
                printf("Usage: %s [-h] [-n proc] [-s simul] [-t timelimitForChildren] "
                       "[-i interval] [-f logfile]\n", argv[0]);
                return EXIT_SUCCESS;

            case 'n':
                n = atoi(optarg);
                break;

            case 's':
                s = atoi(optarg);
                break;

            case 't':
                t = atof(optarg);
                break;

            case 'i':
                i = atof(optarg);
                break;

            case 'f':
                strncpy(logFile, optarg, sizeof(logFile) - 1);
                logFile[sizeof(logFile) - 1] = '\0';
                break;

            default:
                printf("Usage: %s [-h] [-n proc] [-s simul] [-t timelimitForChildren] "
                       "[-i interval] [-f logfile]\n", argv[0]);
                return EXIT_FAILURE;
        }
    }

    if (n <= 0) {
        n = 1;
    }

    if (s <= 0) {
        s = 1;
    }

    if (t <= 0) {
        t = 1.0;
    }

    if (i < 0) {
        i = 0.0;
    }

    if (s > n) {
        s = n;
    }

    if (s > MAX_PCB_SIZE) {
        s = MAX_PCB_SIZE;
    }

    // Set up signal handlers for graceful shutdown in case of SIGINT or SIGTERM which is sent when the user presses Ctrl+C or when the process is terminated. 
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGALRM, signal_handler);

    alarm(5);

    logFileGlobal = fopen(logFile, "w");

    if (logFileGlobal == NULL) {
        perror("OSS: Error opening log file");
        return EXIT_FAILURE;
    }

    initPCBTable(); // Initialize the PCB table to mark all slots as unoccupied and reset all fields.

    logBoth("OSS: starting, PID:%d PPID:%d\n", getpid(), getppid());
    logBoth("OSS called with:\n");
    logBoth("-n %d\n", n);
    logBoth("-s %d\n", s);
    logBoth("-t %.3f\n", t);
    logBoth("-i %.3f\n", i);
    logBoth("-f %s\n", logFile);

    key_t shm_key = ftok("oss.c", 0); 
    // Generate a unique key for shared memory using ftok. 
    if (shm_key == (key_t)-1) { 
        fprintf(stderr,"OSS: Error in ftok for shared memory\n"); 
        fclose(logFileGlobal);
        return EXIT_FAILURE;
    }
    
    int shm_id = shmget(shm_key, BUFF_SZ, IPC_CREAT | 0700);
    // Create a shared memory segment with space for two unsigned ints:
    // clock[0] is seconds and clock[1] is nanoseconds 
    // permissions of 0700 (read/write/execute for owner only).
    if (shm_id == -1) {
        fprintf(stderr,"OSS: Error in shmget\n");
        fclose(logFileGlobal);
        return EXIT_FAILURE;
    }

    unsigned int *clock = (unsigned int *)shmat(shm_id, NULL, 0);
    //attach to the shared memory segment and get a pointer to it.
    //We will use this pointer to read and write the clock values in shared memory.
    if(clock == (void *)-1 ) {
        fprintf(stderr,"OSS: Error in shmat\n");
        shmctl(shm_id, IPC_RMID, NULL); // Mark the shared memory segment for deletion
        fclose(logFileGlobal);
        return EXIT_FAILURE;
    }

    shm_id_global = shm_id; // Store the shared memory id in the global variable for later cleanup
    clock_global = clock;   // Store the pointer to the shared memory clock in the global variable for later cleanup

    clock[0] = 0; // Initialize seconds to 0
    clock[1] = 0; // Initialize nanoseconds to 0

    logBoth("OSS: Shared memory clock initialized.\n");
    logBoth("OSS: Clock is %u:%u\n", clock[0], clock[1]);
    logBoth("OSS: Logging to %s with max %d log write calls\n", logFile, MAX_LOG_LINES);

    initializeResourceTable(); // Initialize the resource table with total and available instances
    initResourceAllocation(); // Initialize the resource allocation table

    logBoth("OSS: Resource descriptors initialized.\n");
    printResourceTable(); // Print the initialized resource table

    key_t msg_key = ftok("oss.c", 1);
    // Generate a unique key for message queue using ftok. 
    // We use a different id (1 instead of 0) to generate a different key for the message queue than the shared memory.

    if(msg_key == (key_t)-1) { 
        fprintf(stderr,"OSS: Error in ftok for message queue\n"); 
        cleanupIPC(); // Clean up any IPC resources oss has created so far.
        fclose(logFileGlobal);
        return EXIT_FAILURE;
    }

    int msg_id = msgget(msg_key, IPC_CREAT | 0700);
    // Create a message queue with the generated key and permissions of 0700 (read/write/execute for owner only).

    if(msg_id == -1) {
        fprintf(stderr,"OSS: Error in msgget\n");
        cleanupIPC(); // Clean up any IPC resources oss has created so far.
        fclose(logFileGlobal);
        return EXIT_FAILURE;
    }

    msg_id_global = msg_id; // Store the message queue id in the global variable for later cleanup

    logBoth("OSS: Message queue created with id %d\n", msg_id);

    int launchedChildren = 0;
    int activeChildren = 0;

    long long launchIntervalNS = secondsToNS(i);
    long long nextLaunchNS = 0;
    long long nextDeadlockCheckNS = DEADLOCK_CHECK_INTERVAL_NS;
    long long nextPrintNS = PRINT_INTERVAL_NS;

    while (launchedChildren < n || activeChildren > 0){
        long long currentNS = getClockNS(clock);

        while (launchedChildren < n &&
               activeChildren < s &&
               currentNS >= nextLaunchNS) {
            if (!launchChildProcess(t, clock, &launchedChildren, &activeChildren)) {
                killAllRunningChildren();
                cleanupIPC();
                fclose(logFileGlobal);
                return EXIT_FAILURE;
            }

            currentNS = getClockNS(clock);

            if (launchIntervalNS > 0) {
                nextLaunchNS = currentNS + launchIntervalNS;
                break;
            } else {
                nextLaunchNS = currentNS;
            }
        }

        checkBlockedProcesses(msg_id);

        int didWorkThisRun = 0;

        for (int slot = 0; slot < MAX_PCB_SIZE; slot++) {
            if (!pcbTable[slot].occupied || pcbTable[slot].blocked) {
                continue;
            }

            didWorkThisRun = 1;

            pid_t pid = pcbTable[slot].pid;

            if (!sendTurnMessage(msg_id, pid, slot)) {
                kill(pid, SIGTERM);
                waitpid(pid, NULL, 0);

                releaseAllResources(slot);
                clearPCBEntry(slot);
                activeChildren--;

                continue;
            }

            logBoth("OSS: Sent turn message to child P%d PID %d slot %d at time %u:%u\n",
                    pcbTable[slot].localPid,
                    pid,
                    slot,
                    clock[0],
                    clock[1]);

            struct Message msgFromChild;

            if (msgrcv(msg_id, &msgFromChild, sizeof(struct Message) - sizeof(long), 1, 0) == -1) {
                perror("OSS: msgrcv failed");

                kill(pid, SIGTERM);
                waitpid(pid, NULL, 0);

                releaseAllResources(slot);
                clearPCBEntry(slot);
                activeChildren--;

                continue;
            }

            logBoth("OSS: Received message from child PID %d slot %d with value %d\n",
                    msgFromChild.pid,
                    msgFromChild.slot,
                    msgFromChild.value);

            if (msgFromChild.value > 0) {
                int resourceNumber = msgFromChild.value - 1;

                totalRequests++;

                logBoth("OSS: Child PID %d slot %d is requesting R%d at time %u:%u\n",
                        msgFromChild.pid,
                        msgFromChild.slot,
                        resourceNumber,
                        clock[0],
                        clock[1]);

                if (grantResource(msgFromChild.slot, resourceNumber)) {
                    totalGrantedImmediately++;
                    recordGrantedRequest();

                    logBoth("OSS: Granting child PID %d slot %d request for R%d\n",
                            msgFromChild.pid,
                            msgFromChild.slot,
                            resourceNumber);

                    if (!sendGrantMessage(msg_id,
                                          msgFromChild.pid,
                                          msgFromChild.slot,
                                          msgFromChild.value)) {
                        kill(pid, SIGTERM);
                        waitpid(pid, NULL, 0);

                        releaseAllResources(slot);
                        clearPCBEntry(slot);
                        activeChildren--;

                        continue;
                    }
                } else {
                    totalBlockedRequests++;

                    logBoth("OSS: Could not grant request for R%d to child PID %d slot %d\n",
                            resourceNumber,
                            msgFromChild.pid,
                            msgFromChild.slot);

                    blockProcess(msgFromChild.slot, resourceNumber);
                }

                printResourceTable();
            } else if (msgFromChild.value < 0) {
                int resourceNumber = (-msgFromChild.value) - 1;

                logBoth("OSS: Child PID %d slot %d is releasing R%d at time %u:%u\n",
                        msgFromChild.pid,
                        msgFromChild.slot,
                        resourceNumber,
                        clock[0],
                        clock[1]);

                if (releaseOneResource(msgFromChild.slot, resourceNumber)) {
                    totalReleases++;

                    logBoth("OSS: Acknowledged release of R%d from child PID %d slot %d\n",
                            resourceNumber,
                            msgFromChild.pid,
                            msgFromChild.slot);
                } else {
                    logBoth("OSS: Could not release R%d from child PID %d slot %d\n",
                            resourceNumber,
                            msgFromChild.pid,
                            msgFromChild.slot);
                }

                checkBlockedProcesses(msg_id);
                printResourceTable();
            } else {
                logBoth("OSS: Child PID %d slot %d is terminating at time %u:%u\n",
                        msgFromChild.pid,
                        msgFromChild.slot,
                        clock[0],
                        clock[1]);

                releaseAllResources(msgFromChild.slot);
                checkBlockedProcesses(msg_id);

                printResourceTable();

                waitpid(pid, NULL, 0);
                clearPCBEntry(slot);
                activeChildren--;
                totalProcessesTerminatedNormally++;
            }

            if (activeChildren > 0 || launchedChildren < n) {
                addToClock(clock, TURN_INCREMENT_NS);
            }

            currentNS = getClockNS(clock);

            if (currentNS >= nextPrintNS) {
            printProcessTable(clock);
            printBlockedProcesses();
            printResourceTable();

            while (nextPrintNS <= currentNS) {
                nextPrintNS += PRINT_INTERVAL_NS;
            }
            }

            if (currentNS >= nextDeadlockCheckNS) {
                int deadlockedSlots[MAX_PCB_SIZE];

                int deadlocked = runDeadlockDetection(clock, deadlockedSlots);

                if (deadlocked > 0) {
                    terminateDeadlockedProcess(deadlockedSlots[0], &activeChildren, msg_id);
                    printResourceTable();
                }

                while (nextDeadlockCheckNS <= currentNS) {
                    nextDeadlockCheckNS += DEADLOCK_CHECK_INTERVAL_NS;
                }
            }
        }

        if (activeChildren > 0 && !didWorkThisRun) {
            int deadlockedSlots[MAX_PCB_SIZE];

            logBoth("\nOSS: No unblocked processes can run at time %u:%u\n",
                    clock[0],
                    clock[1]);

            int deadlocked = runDeadlockDetection(clock, deadlockedSlots);

            if (deadlocked > 0) {
                terminateDeadlockedProcess(deadlockedSlots[0], &activeChildren, msg_id);
                printResourceTable();
            } else {
                checkBlockedProcesses(msg_id);
                addToClock(clock, TURN_INCREMENT_NS);
            }
        }

        if (activeChildren == 0 && launchedChildren < n) {
            currentNS = getClockNS(clock);

            if (currentNS < nextLaunchNS) {
                addToClock(clock, nextLaunchNS - currentNS);
            }
        }
    }

    alarm(0); 
    printFinalReport(clock);

    cleanupIPC();

    logBoth("OSS: Shared memory and message queue cleaned up.\n");

    if (logFileGlobal != NULL) {
        fclose(logFileGlobal);
        logFileGlobal = NULL;
    }

    return EXIT_SUCCESS;
}