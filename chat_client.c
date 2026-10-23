// chat_client.c - TCP chat client with separate send and receive threads
// Build: make        Run: ./chat_client [server_ip] [port]

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include "common.h"

static int sockfd;
static LineReader reader;

// Receive thread: prints every line the server sends.
static void *receive_thread(void *arg) {
    (void)arg;
    char line[LINE_MAX_LEN];
    for (;;) {
        if (read_line(&reader, line, sizeof(line)) <= 0) {
            printf("\r\033[KServer closed the connection.\n");
            exit(0);
        }
        printf("\r\033[K%s\nYou: ", line);   // redraw the prompt below the message
        fflush(stdout);
    }
    return NULL;
}

// Send thread: reads your input and sends it to the server.
static void *send_thread(void *arg) {
    (void)arg;
    char buf[LINE_MAX_LEN];
    for (;;) {
        printf("You: ");
        fflush(stdout);
        if (fgets(buf, sizeof(buf), stdin) == NULL) strcpy(buf, "exit");   // Ctrl+D
        buf[strcspn(buf, "\r\n")] = '\0';
        if (buf[0] == '\0') continue;

        send_line(sockfd, buf);
        if (strcmp(buf, "exit") == 0) {
            printf("Disconnected.\n");
            shutdown(sockfd, SHUT_RDWR);
            close(sockfd);
            exit(0);
        }
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    const char *ip = (argc > 1) ? argv[1] : "127.0.0.1";
    int port       = (argc > 2) ? atoi(argv[2]) : DEFAULT_PORT;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); return 1; }

    struct sockaddr_in serv;
    memset(&serv, 0, sizeof(serv));
    serv.sin_family = AF_INET;
    serv.sin_port   = htons(port);
    if (inet_pton(AF_INET, ip, &serv.sin_addr) <= 0) {
        fprintf(stderr, "Invalid address: %s\n", ip);
        return 1;
    }
    if (connect(sockfd, (struct sockaddr *)&serv, sizeof(serv)) < 0) {
        perror("connect");
        return 1;
    }
    reader_init(&reader, sockfd);

    // Registration: keep asking until the server accepts a name.
    char name[LINE_MAX_LEN], reply[LINE_MAX_LEN];
    for (;;) {
        printf("Enter your name: ");
        fflush(stdout);
        if (fgets(name, sizeof(name), stdin) == NULL) return 1;
        name[strcspn(name, "\r\n")] = '\0';

        send_line(sockfd, name);
        if (read_line(&reader, reply, sizeof(reply)) <= 0) {
            printf("Server closed the connection.\n");
            return 1;
        }
        if (strcmp(reply, "OK") == 0) break;
        printf("%s\n", strncmp(reply, "ERR ", 4) == 0 ? reply + 4 : reply);
    }
    printf("Connected to Chat Server. Type \"exit\" to leave.\n");

    pthread_t tx, rx;
    pthread_create(&rx, NULL, receive_thread, NULL);
    pthread_create(&tx, NULL, send_thread, NULL);
    pthread_join(tx, NULL);
    return 0;
}