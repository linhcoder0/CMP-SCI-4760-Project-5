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

const size_t BUFF_SZ = sizeof(unsigned int) * 2;

static int msg_id_global = -1;
// global variable to store the message queue id

static pid_t child_pid_global = -1;
// global variable to store one child pid for this early one-child test

static int shm_id_global = -1; 
//global variable to store the shared memory id

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

static unsigned int *clock_global = NULL; 
//global variable to store the pointer to the shared memory clock

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
    cleanupIPC(); // Detach from shared memory and mark shared memory for deletion
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

    clock[0] = timeNS / NANOPERSEC; // Set seconds
    clock[1] = timeNS % NANOPERSEC; // Set nanoseconds
}

void addToClock(unsigned int *clock, long long timeToAddNS) {
    long long currentTimeNS = getClockNS(clock);
    long long newTimeNS = currentTimeNS + timeToAddNS;
    setClockFromNS(clock, newTimeNS);
}

struct ResourceDescriptor {
    //total instances of this resource class in the system
    int totalInstances;
    int availableInstances;
};

struct ResourceDescriptor resourceTable[RESOURCE_CLASSES];
// We will use this resource table to keep track of the total instances and available instances for each resource class.

void initializeResourceTable() {
    // Initialize the resource table with the total instances and available instances for each resource class.
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

int resourceAllocation[1][RESOURCE_CLASSES];
//for step 4, we only have one child, so one row is enough. 
//later this should be something akin to resourceAllocation[MAX_PCB_SIZE][RESOURCE_CLASSES], 

void initResourceAllocation() {
    //This function initializes the resource allocation table, 
    //which keeps track of how many instances of each resource class are currently allocated to each child process.
    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        resourceAllocation[0][i] = 0;
    }
}

int grantResource(int slot, int resourceNumber) {
    if(slot != 0) {
        // In this step, we only have one child process, so the slot number must be 0. 
        // If we receive a request with a different slot number, that means there is an error in the program logic, because we should not have any other slot numbers being used in the messages between oss and worker processes at this point. 
        // We will print an error message and return 0 to indicate that the resource request cannot be granted due to invalid slot number.
        fprintf(stderr, "OSS: Error in grantResource: invalid slot number %d\n", slot);
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

void releaseAllResources(int slot) {
    if(slot != 0) {
        // In this step, we only have one child process, so the slot number must be 0. 
        // If we receive a request with a different slot number, that means there is an error in the program logic, because we should not have any other slot numbers being used in the messages between oss and worker processes at this point. 
        // We will print an error message and return without releasing any resources due to invalid slot number.
        fprintf(stderr, "OSS: Error in releaseAllResources: invalid slot number %d\n", slot);
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

int main(int argc, char *argv[]) {
    // Set up signal handlers for graceful shutdown in case of SIGINT or SIGTERM which is sent when the user presses Ctrl+C or when the process is terminated. 
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    key_t shm_key = ftok("oss.c", 0); 
    // Generate a unique key for shared memory using ftok. 
    if (shm_key == (key_t)-1) { 
        fprintf(stderr,"OSS: Error in ftok for shared memory\n"); 
        return EXIT_FAILURE;
    }
    
    int shm_id = shmget(shm_key, BUFF_SZ, IPC_CREAT | 0700);
    //create a shm segment with the generated key, size of 2 unsigned ints (for seconds and nanoseconds), and permissions of 0700 (read/write/execute for owner only).
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

    pid_t pid = fork();
    // Fork a child process to run the user program.
    if (pid == -1) {
        fprintf(stderr, "OSS: Error in fork\n");
        cleanupIPC(); // Clean up any IPC resources oss has created so far.
        return EXIT_FAILURE;
    }

    if (pid == 0) {
        execl("./user_proc", "user_proc", (char *)NULL); // Execute the user program in the child process
        perror("OSS: execl failed");
        exit(EXIT_FAILURE);
    } 

    child_pid_global = pid; // Store the child pid in the global variable for later cleanup

    printf("OSS: Forked one child with PID %d\n", pid);

    struct Message msgToChild;
    msgToChild.mtype = pid; // Set the message type to the child pid so 
    //that the child process can receive messages intended for it by checking the message type in msgrcv.
    //Example: when the child process calls msgrcv, it can specify the message type as its own pid to receive messages intended for it.  

    msgToChild.value = 1; 
    // This is only a start signal for this Step 4 test.
    // user_proc does not treat this value as a resource request.
    // The actual resource request happens when user_proc sends value 1 back to oss.

    msgToChild.pid = getpid(); // Set the pid field to the pid of the oss process, 
    //so that the child process can know which process is sending the message. 

    msgToChild.slot = 0; 
    // In this Step 4 test, we only have one child process, so we will use slot 0 in the PCB table for that child process.


    //Example: if the child process is in slot 3 of the PCB table, 
    //then it can set the slot field to 3 when sending a message to oss, so that
    //oss knows that the message is from the worker process in slot 3 and 
    //can update the PCB table for that slot accordingly. 

    if(msgsnd(msg_id, &msgToChild, sizeof(struct Message) - sizeof(long), 0) == -1) {
        //If we fail to send a message to the child process, we will kill the child process and clean up the shared memory and message queue before exiting with failure.
        perror("OSS: msgsnd failed");
        kill(pid, SIGKILL); // Kill the child process if we fail to send a message to it
        wait(NULL); // Wait for the child process to finish
        cleanupIPC(); // Clean up any IPC resources oss has created so far.
        return EXIT_FAILURE;
    }

    printf("OSS: Sent start message to child PID %d\n", pid);

    struct Message msgFromChild;
    if (msgrcv(msg_id, &msgFromChild, sizeof(struct Message) - sizeof(long), 1, 0) == -1) {
        //if OSS fails to receive a msg from the child process, cleanup then exit
        perror("OSS: msgrcv failed");
        cleanupIPC(); // Clean up any IPC resources oss has created so far.
        return EXIT_FAILURE;
    }

    printf("OSS: Received message from child PID %d with value %d\n",
           msgFromChild.pid,
           msgFromChild.value);

    if (msgFromChild.value > 0) {
        int resourceNumber = msgFromChild.value - 1; 
        // 1 = resource class 0, 2 = resource class 1, etc.
        // That means 0 is not a valid resource class, and the resource classes start from 1 in the messages, 
        // but we will convert it to 0-based index when we check the resource table. 

        printf("OSS: Child PID %d is requesting R%d\n",
                    msgFromChild.pid,
                    resourceNumber);

        if (grantResource(msgFromChild.slot, resourceNumber)) {
                    printf("OSS: Granting child PID %d request for R%d\n",
                        msgFromChild.pid,
                        resourceNumber);
        
        printResourceTable();

        struct Message grantMessage;
        grantMessage.mtype = msgFromChild.pid; 
        //Set the message type to the child pid so that the child process
        //can receive this grant message intended for it by checking the message type in msgrcv. 
        grantMessage.value = resourceNumber + 1;
        //We will use the value field to indicate the resource class being granted in the messages between oss and worker processes. 
        //Since we are using 1-based index for resource classes in the messages, we will add 1 to the resource number when we set the value field in the grant message.
        grantMessage.pid = getpid(); // Set the pid field to the pid of the oss 
        //process, so that the child process can know which process is sending the grant message.
        grantMessage.slot = msgFromChild.slot; 
        // Send the same slot back to the child so both processes keep referring to the same child slot.
        if (msgsnd (msg_id, &grantMessage, sizeof(struct Message) - sizeof(long), 0) == -1) {
            // If we fail to send the grant message, clean up IPC and exit with failure.            
            perror("OSS: msgsnd failed when sending grant message");
            cleanupIPC(); // Clean up any IPC resources oss has created so far.
            return EXIT_FAILURE;
        }
    } else {
        printf("OSS: Could not grant request for R%d to child PID %d\n",
                    msgFromChild.value - 1,
                    msgFromChild.pid);
    }
}
    struct Message terminationMessage;
    
    if (msgrcv(msg_id, &terminationMessage, sizeof(struct Message) - sizeof(long), 1, 0) == -1) {
        // If we fail to receive the termination message from the child process, we will 
        // exit with failure after cleaning up.
        perror("OSS: msgrcv failed when waiting for termination message");
        cleanupIPC(); // Clean up any IPC resources oss has created so far.
        return EXIT_FAILURE;
    }

    if (terminationMessage.value == 0) {
        printf("OSS: Child PID %d is terminating\n", terminationMessage.pid);

        releaseAllResources(terminationMessage.slot);

        printf("OSS: Released all resources for child PID %d\n",
               terminationMessage.pid);

        printResourceTable();

        waitpid(pid, NULL, 0);
        child_pid_global = -1;
    }

    cleanupIPC();

    printf("OSS: Shared memory and message queue cleaned up.\n");

    return EXIT_SUCCESS;    
}
