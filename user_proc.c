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

struct Message {
    long mtype;
    int value;
    int pid;
    int slot;
};



int main(int argc, char *argv[]) {
    key_t msg_key = ftok("oss.c", 1);
    // Generate the same message queue key that oss used.

    if (msg_key == (key_t)-1) {
        // If we fail to generate the key for msg queue, we will exit with failure. We do not need to clean up shared memory or message queue in this case because we have not created or attached to any shared memory or message queue in the user process yet. 
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

    struct Message msgFromOSS;
    if (msgrcv(msg_id, &msgFromOSS, sizeof(struct Message) - sizeof(long), getpid(), 0) == -1) {
        // If we fail to receive the start message from oss, exit with failure.
        // user_proc does not remove the message queue because oss created it and owns cleanup.
        fprintf(stderr, "USER_PROC: Error receiving start message\n");
        return EXIT_FAILURE;
    }

    printf("USER_PROC: PID %d Received start message from OSS\n", getpid());

    struct Message msgToOSS;
    msgToOSS.mtype = 1;
        // oss receives messages using mtype 1.
    msgToOSS.value = 1;    
    // Positive 1 means request R0.
    // Later: 2 means R1, 3 means R2, etc.
    msgToOSS.pid = getpid();
    msgToOSS.slot = msgFromOSS.slot;

    if (msgsnd(msg_id, &msgToOSS, sizeof(struct Message) - sizeof(long), 0) == -1) {
        fprintf(stderr, "USER_PROC: Error sending resource request\n");
        return EXIT_FAILURE;
    }

    printf("USER_PROC: PID %d requested R0\n", getpid());

    struct Message grantMessage;

    if (msgrcv(msg_id, &grantMessage, sizeof(struct Message) - sizeof(long), getpid(), 0) == -1) {
        fprintf(stderr, "USER_PROC: Error receiving grant message\n");
        return EXIT_FAILURE;
    }

    printf("USER_PROC: PID %d received granted resource R0\n", getpid());

    struct Message terminateMessage;

    terminateMessage.mtype = 1;
    terminateMessage.value = 0;
    // Value 0 means the child is terminating.

    terminateMessage.pid = getpid();
    terminateMessage.slot = msgFromOSS.slot;

    if (msgsnd(msg_id, &terminateMessage, sizeof(struct Message) - sizeof(long), 0) == -1) {
        fprintf(stderr, "USER_PROC: Error sending termination message\n");
        return EXIT_FAILURE;
    }

    printf("USER_PROC: PID %d sent termination message\n", getpid());

    return EXIT_SUCCESS;
}