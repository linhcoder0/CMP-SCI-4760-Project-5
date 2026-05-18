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
#define TEST_CHILDREN 3 //number of children to test with in this early stage. We will set this to 3 for now, but we will eventually want to test with more children
const size_t BUFF_SZ = sizeof(unsigned int) * 2;

static int msg_id_global = -1;
// global variable to store the message queue id

// static pid_t child_pid_global = -1;
// // global variable to store one child pid for this early one-child test

static int shm_id_global = -1; 
//global variable to store the shared memory id

static unsigned int *clock_global = NULL;
// Global variable to store the pointer to the shared memory clock.

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
};

struct ResourceDescriptor resourceTable[RESOURCE_CLASSES];
// Keeps track of total and available instances for each resource class.

struct PCB pcbTable[MAX_PCB_SIZE];

int resourceAllocation[MAX_PCB_SIZE][RESOURCE_CLASSES];

void clearPCBEntry(int slot) {
    pcbTable[slot].occupied = 0;
    pcbTable[slot].pid = 0;
    pcbTable[slot].localPid = 0;
    pcbTable[slot].startSeconds = 0;
    pcbTable[slot].startNano = 0;
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

void signal_handler(int sig) {
    printf("OSS: received signal %d, shutting down...\n", sig);

    // In Step 6, there may be multiple children, so kill/reap all occupied slots.
    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied && pcbTable[i].pid > 0) {
            kill(pcbTable[i].pid, SIGTERM);
            waitpid(pcbTable[i].pid, NULL, 0);
            clearPCBEntry(i);
        }
    }

    cleanupIPC();
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
    //Print the resource table in a readable format.
    //We will print the total instances and available instances for each resource class 

    printf("\nOSS: Current resource table.\n");
    printf("Resource:   ");

    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        printf("R%-3d", i);
    }

    printf("\nTotal:      ");

    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        printf("%-4d", resourceTable[i].totalInstances);
    }

    printf("\nAvailable:  ");

    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        printf("%-4d", resourceTable[i].availableInstances);
    }

    printf("\n\n");
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
        // Attempt to grant one instance of resourceNumber to the child in the given slot.
    if (slot < 0 || slot >= MAX_PCB_SIZE) {
        fprintf(stderr, "OSS: invalid slot number %d\n", slot);
        return 0;
    }
    //This function attempts to grant a resource request from a child process. 
    //It takes in the slot number of the child process in the PCB table and the resource number being requested.
    //It checks if the requested resource is available in the resource table.
    //If the resource is available, it decrements the available instances in the resource table, 
    //increments the allocated instances in the resource allocation table for that child process, and returns 1 to indicate
    //that the resource request has been granted.
    if (resourceNumber < 0 || resourceNumber >= RESOURCE_CLASSES) {
        return 0; // Invalid resource number
    }
    if (resourceTable[resourceNumber].availableInstances > 0) {
        resourceTable[resourceNumber].availableInstances--;
        resourceAllocation[slot][resourceNumber]++;
        return 1; // Resource granted
    }
    return 0; // Resource not available
}

int releaseOneResource(int slot, int resourceNumber) {
        // Release one instance of resourceNumber from the child in the given slot.

    if (slot < 0 || slot >= MAX_PCB_SIZE) {
        fprintf(stderr, "OSS: invalid slot number %d\n", slot);
        return 0;
    }

    if (resourceNumber < 0 || resourceNumber >= RESOURCE_CLASSES) {
        fprintf(stderr, "OSS: Error in releaseOneResource: invalid resource number %d\n", resourceNumber);
        return 0;
    }

    if (resourceAllocation[slot][resourceNumber] > 0) {
        resourceAllocation[slot][resourceNumber]--;
        resourceTable[resourceNumber].availableInstances++;
        return 1;
    }

    return 0;
}

void releaseAllResources(int slot) {
    if (slot < 0 || slot >= MAX_PCB_SIZE) {
        fprintf(stderr, "OSS: invalid slot number %d\n", slot);
        return;
    }
    //This function releases all resources allocated to a child process when it terminates.
    //It takes in the slot number of the child process in the PCB table,
    //and for each resource class, it checks how many instances of that resource class are allocated
    //to that child process in the resource allocation table. 
    //It then increments the available instances in the resource table by that amount, and sets the allocated instances in the resource allocation table for that child process to 0.
    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        if (resourceAllocation[slot][i] > 0) {
            resourceTable[i].availableInstances += resourceAllocation[slot][i];
            resourceAllocation[slot][i] = 0;
        }
    }
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

int main(int argc, char *argv[]) {
    // Set up signal handlers for graceful shutdown in case of SIGINT or SIGTERM which is sent when the user presses Ctrl+C or when the process is terminated. 
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    initPCBTable(); // Initialize the PCB table to mark all slots as unoccupied and reset all fields.

    key_t shm_key = ftok("oss.c", 0); 
    // Generate a unique key for shared memory using ftok. 
    if (shm_key == (key_t)-1) { 
        fprintf(stderr,"OSS: Error in ftok for shared memory\n"); 
        return EXIT_FAILURE;
    }
    
    int shm_id = shmget(shm_key, BUFF_SZ, IPC_CREAT | 0700);
    // Create a shared memory segment with space for two unsigned ints:
    // clock[0] is seconds and clock[1] is nanoseconds 
    // permissions of 0700 (read/write/execute for owner only).
    if (shm_id == -1) {
        fprintf(stderr,"OSS: Error in shmget\n");
        return EXIT_FAILURE;
    }

    unsigned int *clock = (unsigned int *)shmat(shm_id, NULL, 0);
    //attach to the shared memory segment and get a pointer to it.
    //We will use this pointer to read and write the clock values in shared memory.
    if(clock == (void *)-1 ) {
        fprintf(stderr,"OSS: Error in shmat\n");
        shmctl(shm_id, IPC_RMID, NULL); // Mark the shared memory segment for deletion
        return EXIT_FAILURE;
    }

    shm_id_global = shm_id; // Store the shared memory id in the global variable for later cleanup
    clock_global = clock;   // Store the pointer to the shared memory clock in the global variable for later cleanup

    clock[0] = 0; // Initialize seconds to 0
    clock[1] = 0; // Initialize nanoseconds to 0

    printf("OSS: Shared memory clock initialized.\n");
    printf("OSS: Clock is %u:%u\n", clock[0], clock[1]);

    initializeResourceTable(); // Initialize the resource table with total and available instances
    initResourceAllocation(); // Initialize the resource allocation table

    printf("OSS: Resource descriptors initialized.\n");
    printResourceTable(); // Print the initialized resource table

    key_t msg_key = ftok("oss.c", 1);
    // Generate a unique key for message queue using ftok. 
    // We use a different id (1 instead of 0) to generate a different key for the message queue than the shared memory.

    if(msg_key == (key_t)-1) { 
        fprintf(stderr,"OSS: Error in ftok for message queue\n"); 
        cleanupIPC(); // Clean up any IPC resources oss has created so far.
        return EXIT_FAILURE;
    }

    int msg_id = msgget(msg_key, IPC_CREAT | 0700);
    // Create a message queue with the generated key and permissions of 0700 (read/write/execute for owner only).

    if(msg_id == -1) {
        fprintf(stderr,"OSS: Error in msgget\n");
        cleanupIPC(); // Clean up any IPC resources oss has created so far.
        return EXIT_FAILURE;
    }

    msg_id_global = msg_id; // Store the message queue id in the global variable for later cleanup

    printf("OSS: Message queue created with id %d\n", msg_id);

    int launchedChildren = 0;
    int activeChildren = 0;

    while (launchedChildren < TEST_CHILDREN) {
        int slot = findFreePCBSlot();
        
        if (slot == -1) {
            break;
        }

        pid_t pid = fork();

        // Fork a child process to run the user program.
        if (pid == -1) {
            fprintf(stderr, "OSS: Error in fork\n");
            cleanupIPC(); // Clean up any IPC resources oss has created so far.
            return EXIT_FAILURE;
        }

        if (pid == 0) {
            //For step 5, let's give the child process some time to stick around before it terminates. Later this should come from the -t command line opt
            execl("./user_proc", "user_proc", "0", "100000000",(char *)NULL); // Execute the user program in the child process

            perror("OSS: execl failed");
            exit(EXIT_FAILURE);
        } 

    // child_pid_global = pid; // Store the child pid in the global variable for later cleanup
        pcbTable[slot].occupied = 1;
        pcbTable[slot].pid = pid;
        pcbTable[slot].localPid = launchedChildren + 1;
        pcbTable[slot].startSeconds = clock[0];
        pcbTable[slot].startNano = clock[1];

        printf("OSS: Forked child P%d with PID %d in slot %d\n", pcbTable[slot].localPid, pid, slot);
        launchedChildren++;
        activeChildren++;
}

    while (activeChildren > 0) {
    for (int slot = 0; slot < MAX_PCB_SIZE; slot++) {
        if(!pcbTable[slot].occupied) {
            continue;
        }

        pid_t pid = pcbTable[slot].pid;

        if(!sendTurnMessage(msg_id, pid, slot)){
            kill(pid, SIGTERM);
            waitpid(pid, NULL, 0);
            releaseAllResources(slot); // Release all resources allocated to this child process in case of error, so that other child processes can continue to run without being blocked by this one.
            clearPCBEntry(slot);
            activeChildren--;
            continue;
        }

        printf("OSS: Sent turn message to child P%d PID %d slot %d at time %u:%u\n", pcbTable[slot].localPid, pid, slot, clock[0], clock[1]);

        struct Message msgFromChild;


        if (msgrcv(msg_id, &msgFromChild, sizeof(struct Message) - sizeof(long), 1, 0) == -1) {
            //OSS must receive messages from the child where mtype is 1.
            perror("OSS: msgrcv failed");
            kill(pid, SIGTERM);
            waitpid(pid, NULL, 0);
            releaseAllResources(slot); // Release all resources allocated to this child process in case of error, so that other child processes can continue to run without being blocked by this one.
            clearPCBEntry(slot);
            activeChildren--;
            continue;        
        }

        printf("OSS: Received message from child PID %d slot %d with value %d\n", msgFromChild.pid, msgFromChild.slot, msgFromChild.value);

        if (msgFromChild.value > 0) {
            int resourceNumber = msgFromChild.value - 1; 
            // Positive values are resource requests.
            // 1 = request R0, 2 = request R1, ..., 10 = request R9.

            printf("OSS: Child PID %d slot %d is requesting R%d at time %u:%u\n", msgFromChild.pid, msgFromChild.slot, resourceNumber, clock[0], clock[1]);

            if(grantResource(msgFromChild.slot, resourceNumber)){
                printf("OSS: Granting child PID %d slot %d request for R%d\n", msgFromChild.pid, msgFromChild.slot, resourceNumber);

                if(!sendGrantMessage(msg_id, msgFromChild.pid, msgFromChild.slot, msgFromChild.value)){
                    kill(pid, SIGTERM);
                    waitpid(pid, NULL, 0);
                    releaseAllResources(slot);
                    clearPCBEntry(slot);
                    activeChildren--;
                    continue;
                } 
            } else {
                printf("OSS: Could not grant request for R%d to child PID %d slot %d\n", resourceNumber, msgFromChild.pid, msgFromChild.slot);
                
                //Full blocking is not implemented yet, so we will just send a grant message with value 0 to indicate the request is not granted,
                // so that user_proc does not hang waiting for a grant message 
                //that will never come in this step. 
                if (!sendGrantMessage(msg_id, msgFromChild.pid, msgFromChild.slot, 0)) {
                    kill(pid, SIGTERM);
                    waitpid(pid, NULL, 0);
                    releaseAllResources(slot);
                    clearPCBEntry(slot);
                    activeChildren--;
                    continue;
                }
            }

            printResourceTable();
        } else if (msgFromChild.value < 0) {
            int resourceNumber = (-msgFromChild.value) - 1;
            // Negative values are resource releases.
            // -1 = release R0, -2 = release R1, ..., -10 = release R9.

            printf("OSS: Child PID %d slot %d is releasing R%d at time %u:%u\n", msgFromChild.pid, msgFromChild.slot, resourceNumber, clock[0], clock[1]);

            if (releaseOneResource(msgFromChild.slot, resourceNumber)) {
                printf("OSS: Acknowledged release of R%d from child PID %d slot %d\n", resourceNumber, msgFromChild.pid, msgFromChild.slot);
            } else {
                printf("OSS: Could not release R%d from child PID %d slot %d\n", resourceNumber, msgFromChild.pid, msgFromChild.slot);
            }

            printResourceTable();
        } else {
            //Value 0 from the child means the child is terminating.
            printf("OSS: Child PID %d slot %d is terminating at time %u:%u\n", msgFromChild.pid, msgFromChild.slot, clock[0], clock[1]);

            releaseAllResources(msgFromChild.slot); 

            printResourceTable();

            waitpid(pid, NULL, 0); // Wait for the child process to finish
            clearPCBEntry(slot);
            activeChildren--;
        }

        if (activeChildren > 0) {
            // Advance simulated time by 10ms after each completed child turn.
            addToClock(clock, TURN_INCREMENT_NS);
            }
        }
    }
        cleanupIPC(); // Detach from shared memory and mark shared memory and message queue for deletion
        printf("OSS: Shared memory and message queue cleaned up.\n");
        return EXIT_SUCCESS;
}
