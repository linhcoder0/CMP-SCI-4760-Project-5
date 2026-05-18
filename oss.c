// • Start by creating a Makefile that compiles and builds the two executables: oss and user_proc.
// • Implement clock in shared memory; possibly reuse the one from last project.
// • Have oss create resource descriptors and populate them with instances.
// • Use message queues to communicate requests, allocation, and release of resources to children. Start by testing one child
// process just requesting one resource and then terminating
// • Have the child processes now stick around until their time is up, requesting and releases
// • Now test for multiple children requesting and releases
// • If all is working now, implement deadlock detection to detect when a deadlock exists
// • Lastly, implement oss terminating one of the deadlocked processes
// • Keep track of output statistics in log file.
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

static int shm_id_global = -1; 
//global variable to store the shared memory id

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
        //only detach from shm if we have a valid shared memory id, which means we successfully created the shared memory segment.
        shmctl(shm_id_global, IPC_RMID, NULL); // Mark the shared memory segment for deletion
        shm_id_global = -1;
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

    printf("\nOSS: Resource table initialized.\n");
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
    printResourceTable(); // Print the initialized resource table

    addToClock(clock, 10000000);
    printf("OSS: Clock after adding 10ms is %u:%u\n", clock[0], clock[1]);

    addToClock(clock, 10000000);
    printf("OSS: Clock after adding another 10ms is %u:%u\n", clock[0], clock[1]);

    addToClock(clock, 990000000);
    printf("OSS: Clock after adding 990ms is %u:%u\n", clock[0], clock[1]);

    cleanupIPC();

    printf("OSS: Shared memory cleaned up.\n");

    return EXIT_SUCCESS;    
}
