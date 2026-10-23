# Online Chat Server

A multi-client TCP chat server and client written in C using POSIX sockets and pthreads.

## Features
- Concurrent clients (one thread per client)
- Unique usernames with validation
- Newline-delimited message protocol (handles merged/split TCP packets)
- Join/leave announcements

## Build and run (Linux / WSL)
    make
    ./chat_server [port]
    ./chat_client [server_ip] [port]

Type `exit` in a client to disconnect.
