CC      = gcc
CFLAGS  = -Wall -Wextra -g -O2

all: chat_server chat_server_threads chat_client

chat_server: chat_server.c common.h
	$(CC) $(CFLAGS) chat_server.c -o chat_server

chat_server_threads: chat_server_threads.c common.h
	$(CC) $(CFLAGS) -pthread chat_server_threads.c -o chat_server_threads

chat_client: chat_client.c common.h
	$(CC) $(CFLAGS) -pthread chat_client.c -o chat_client

# compare both servers under the same load
bench: all
	python3 loadtest.py --server ./chat_server --clients 1000
	python3 loadtest.py --server ./chat_server_threads --clients 1000

clean:
	rm -f chat_server chat_server_threads chat_client

.PHONY: all bench clean