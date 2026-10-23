CC      = gcc
CFLAGS  = -Wall -Wextra -g -pthread

all: chat_server chat_client

chat_server: chat_server.c common.h
	$(CC) $(CFLAGS) chat_server.c -o chat_server

chat_client: chat_client.c common.h
	$(CC) $(CFLAGS) chat_client.c -o chat_client

clean:
	rm -f chat_server chat_client

.PHONY: all clean