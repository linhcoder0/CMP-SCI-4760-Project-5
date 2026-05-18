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

#define RESOURCE_CLASSES 10
#define INSTANCES_PER_RESOURCE 5
#define NANOPERSEC 1000000000LL //1 second = 1 billion nanoseconds

const size_t BUFF_SZ = sizeof(unsigned int) * 2;

struct Message {
    long mtype;
    int value;
    int pid;
    int slot;
};

long long getClockNS(unsigned int *clock) {
    // Convert the shared memory clock into one total nanosecond value.
    // clock[0] stores seconds and clock[1] stores nanoseconds.
    return ((long long)clock[0] * NANOPERSEC) + clock[1];
}

int chooseRequestResource(int allocated[]) {
    // Pick a random resource class that this process is still allowed to request.
    // A child should never request more than the total system instances of a resource.
    // Since each resource class has 5 total instances, this process should not request
    // another R0 if it already has 5 of R0.

    int choices[RESOURCE_CLASSES];
    int count = 0;
    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        if (allocated[i] < INSTANCES_PER_RESOURCE) {
            choices[count] = i;
            count++;
        }
    }

    if (count == 0) {
        return -1; //no resource can be requested. 
    }

    return choices[rand() % count]; // Return a random available resource
}

int chooseReleaseResource(int allocated[]) {
    // Pick a random resource class that this process currently owns.
    // The child should not release a resource it does not have.
    int choices[RESOURCE_CLASSES];
    int count = 0;

    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        if (allocated[i] > 0) {
            choices[count] = i;
            count++;
        }
    }

    if (count == 0) {
        return -1; //no resource can be released. 
    }

    return choices[rand() % count];
}


int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "USER_PROC: Usage: %s maxSeconds maxNanoseconds\n", argv[0]);
        return EXIT_FAILURE;
    }

    int maxSec = atoi(argv[1]);
    int maxNano = atoi(argv[2]);

    if (maxSec < 0) maxSec = 0;
    if (maxNano < 0) maxNano = 0;

    while (maxNano >= NANOPERSEC) {
        //ensure nanoseconds is less than 1 second by converting excess nanoseconds into seconds and adding to maxSec. 
        // This makes sure the total lifetime of the child process is correctly represented by the maxSec and maxNano values passed in from the command line. 
        maxSec++;
        maxNano -= NANOPERSEC;
    }

    long long maxLifetimeNS = ((long long)maxSec * NANOPERSEC) + maxNano;

    if (maxLifetimeNS <= 0) {
        // If the calculated max lifetime in nanoseconds is 0 or negative, set it to a default value of 1 second (1 billion nanoseconds)
        //to ensure the child process has a positive lifetime.
        // This can happen if the user passes in 0 for both maxSeconds and maxNanoseconds, or if they pass in a negative value.
        maxLifetimeNS = 1; 
    }

    srand((unsigned int)(time(NULL) ^ getpid())); 

    key_t msg_key = ftok("oss.c", 1);
    // Generate the same message queue key that oss used.

    if (msg_key == (key_t)-1) {
        // If ftok fails here, it means user_proc has not accessed any IPC resources yet.
        fprintf(stderr,"USER_PROC: Error in ftok for message queue\n"); 
        return EXIT_FAILURE;
    }

    int msg_id = msgget(msg_key, 0700);
    // Get the id of the existing message queue created by oss.
    // oss already created the queue; user_proc is only looking it up.

    if(msg_id == -1) {
        // If we fail to get the message queue id, exit with failure.
        // user_proc does not remove the message queue because oss created it and owns cleanup.
        fprintf(stderr,"USER_PROC: Error in msgget\n");
        return EXIT_FAILURE;
    }

    key_t shm_key = ftok("oss.c", 0);
    //Generate the same shm key that oss used.

    if (shm_key == (key_t)-1) {
        //If ftok fails here, it means user_proc has not accessed any IPC resources yet. 
        fprintf(stderr,"USER_PROC: Error in ftok for shared memory\n"); 
        return EXIT_FAILURE;
    }

    int shm_id = shmget(shm_key, BUFF_SZ, 0700);
    //Get the id of the existing shared memory segment created by oss.

    if (shm_id == -1) {
        //If we fail to get the shared memory id, exit with failure. 
        //user_proc does not remove the shared memory segment because oss created it and owns cleanup. 
        fprintf(stderr,"USER_PROC: Error in shmget\n");
        return EXIT_FAILURE;
    }

    unsigned int *clock = (unsigned int *)shmat(shm_id, NULL, 0);
    // Attach to the shared memory clock so this process can check simulated time.

    if (clock == (void  *)-1) {
        fprintf(stderr,"USER_PROC: Error in shmat\n");
        return EXIT_FAILURE;
    }

    int allocated[RESOURCE_CLASSES];
    // Local record of how many resources this child believes it owns.
    // oss has the authoritative resource table, but the child still needs this
    // so it does not request more than 5 of any resource or release resources it does not own.
    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        allocated[i] = 0; // Initialize allocated resources to 0
    }

    long long startTimeNS = getClockNS(clock);
    int done = 0;
    int slot = 0;

    while (!done) {
        struct Message msgFromOSS;

        //Wait until OSS gives this child another turn.
        //The child receives messages where mtype equals its own pid.

        if(msgrcv(msg_id, &msgFromOSS, sizeof(struct Message) - sizeof(long), getpid(), 0) == -1) {
            //If we fail to receive a message from oss, exit with failure. 
            fprintf(stderr, "USER_PROC: Error receiving message from OSS\n");
            shmdt(clock); // Detach from shared memory before exiting
            return EXIT_FAILURE;
        }

        slot = msgFromOSS.slot; // Update the slot number based on the message from OSS. This is important for the child to know which slot it is in for future communication with OSS.

        long long currentTimeNS = getClockNS(clock); // Get the current time from the shared memory clock.

        if(currentTimeNS - startTimeNS >= maxLifetimeNS) {
            //If this child has reached its simulated lifetime, tell OSS it is done.
            //A value of 0 means termination.
            struct Message terminationMessage;

            terminationMessage.mtype = 1; // OSS receives messages using mtype 1
            terminationMessage.value = 0; // Value 0 means the child is terminating
            terminationMessage.pid = getpid();
            terminationMessage.slot = slot; // Include the slot number in the termination message as well, so OSS knows which slot in the PCB table to clean up for this child process.

            if (msgsnd(msg_id, &terminationMessage, sizeof(struct Message) - sizeof(long), 0) == -1) {
                fprintf(stderr, "USER_PROC: Error sending termination message\n");
                shmdt(clock); // Detach from shared memory before exiting
                return EXIT_FAILURE;
            }

            printf("USER_PROC: PID %d slot %d time is up, sent termination message to OSS\n", getpid(), slot);
            done = 1; // Exit the loop after sending termination message
        } else {
            int shouldRequest = (rand() % 100) < 70; // 70% chance to request, 30% chance to release
            //          As each process could request or release resources, we should prefer that processes request resources more than they release
            // them. This should be a parameter in your system for this. I would suggest starting at about 70% request and 30% release and
            // tune from there.

            int resourceNumber = -1;
            int messageValue = 0;

            if (shouldRequest) {
                resourceNumber = chooseRequestResource(allocated);

                if (resourceNumber == -1){
                    //If there is nothing valid to request, try to release instead.
                    resourceNumber = chooseReleaseResource(allocated);
                    shouldRequest = 0;
                    } 
                } else {
                    resourceNumber = chooseReleaseResource(allocated);
                    
                    if (resourceNumber == -1) {
                        //If there is nothing valid to release, try to request instead.
                        resourceNumber = chooseRequestResource(allocated);
                        shouldRequest = 1;
                    }
                }

                if (resourceNumber == -1) {
                    // This should rarely happen, but if no request or release is possible,
                    // terminate instead of sending a meaningless message.
                    messageValue = 0;
                } else if (shouldRequest) {
                    messageValue = resourceNumber + 1;
                    // Positive values mean request.
                    // 1 = request R0, 2 = request R1, ..., 10 = request R9.
                } else {
                    messageValue = -(resourceNumber + 1);
                    // Negative values mean release.
                    // -1 = release R0, -2 = release R1, ..., -10 = release R9.
                }

                struct Message msgToOSS; 

                msgToOSS.mtype = 1; // OSS receives child messages using mtype 1
                msgToOSS.value = messageValue; // Set the value to indicate the resource request or release
                msgToOSS.pid = getpid(); // Set the pid field to the pid of this child process, so OSS knows which process is making the request or release.
                msgToOSS.slot = slot; // Include the slot number in the message as well, so OSS knows which slot in the PCB table to update for this child process when it receives the message.

                if (msgsnd(msg_id, &msgToOSS, sizeof(struct Message) - sizeof(long), 0) == -1) {
                    fprintf(stderr, "USER_PROC: Error sending action message\n");
                    shmdt(clock); // Detach from shared memory before exiting
                    return EXIT_FAILURE;
                }

                if(messageValue > 0){
                    //This process requested a resource, so it must wait for OSS to either grant or deny the request. 
                    printf("USER_PROC: PID %d slot %d requested R%d\n", getpid(), slot, resourceNumber);

                    struct Message grantMessage;

                    if (msgrcv(msg_id, &grantMessage, sizeof(struct Message) - sizeof(long), getpid(), 0) == -1) {
                        fprintf(stderr, "USER_PROC: Error receiving grant message\n");
                        shmdt(clock); // Detach from shared memory before exiting
                        return EXIT_FAILURE;
                    }
                    
                    if(grantMessage.value == messageValue) {
                        //oss granted the exact resource that was requested.
                        allocated[resourceNumber]++; // Update the local record of allocated resources for this child process.
                        printf("USER_PROC: PID %d slot %d received granted resource R%d\n", getpid(), slot, resourceNumber);
                    } else {
                        printf("USER_PROC: PID %d slot %d did not receive grant for R%d\n", getpid(), slot, resourceNumber);
                    }
                } else if (messageValue < 0) {
                        // This process told oss it is releasing a resource.
                        // Since chooseReleaseResource only picks resources this child owns,
                        // it is safe to update the local allocation record here.

                        allocated[resourceNumber]--;

                        printf("USER_PROC: PID %d slot %d released R%d\n",
                            getpid(), 
                            slot,
                            resourceNumber);
                } else {
                    done = 1;
                }
            }
        }
        shmdt(clock); 
        // user_proc detaches from shared memory, but it does not remove shared memory
        // or the message queue. oss created those IPC resources and owns cleanup.

        return EXIT_SUCCESS;
    }
