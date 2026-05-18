CC = gcc
CFLAGS = -g -Wall -std=c99

TARGETS = oss user_proc

all: $(TARGETS)

oss: oss.c
	$(CC) $(CFLAGS) -o oss oss.c

user_proc: user_proc.c
	$(CC) $(CFLAGS) -o user_proc user_proc.c

clean:
	rm -f $(TARGETS) *.o