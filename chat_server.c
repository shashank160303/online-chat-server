// chat_server.c - multi-client TCP chat server (thread per client)
// Build: make        Run: ./chat_server [port]
//
// Commands (any line starting with '/'):
//   /help                show the command list
//   /list                list online users
//   /msg <user> <text>   private message
//   /quit                leave (same as typing "exit")

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <arpa/inet.h>
#include "common.h"

#define MAX_CLIENTS 100

typedef struct {
    int  sock;
    int  active;                    // slot in use?
    char name[NAME_MAX_LEN + 1];    // empty until the user picks a valid name
} Client;

static Client clients[MAX_CLIENTS];
static pthread_mutex_t clients_lock = PTHREAD_MUTEX_INITIALIZER;

// RULE: once a client is registered, every send to it happens while holding
// clients_lock, so lines from different threads can never interleave.

// Send a line to every registered client except `except_sock`.
static void broadcast(const char *msg, int except_sock) {
    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && clients[i].name[0] && clients[i].sock != except_sock)
            send_line(clients[i].sock, msg);
    }
    pthread_mutex_unlock(&clients_lock);
}

// Send a line to one client (used for replies like errors and /list).
static void send_to_slot(int slot, const char *msg) {
    pthread_mutex_lock(&clients_lock);
    send_line(clients[slot].sock, msg);
    pthread_mutex_unlock(&clients_lock);
}

static int valid_name(const char *s) {
    size_t n = strlen(s);
    if (n < 1 || n > NAME_MAX_LEN) return 0;
    for (size_t i = 0; i < n; i++)
        if (!isalnum((unsigned char)s[i]) && s[i] != '_') return 0;
    return 1;
}

// Claim `name` for the client in `slot` and confirm with "OK".
// The check, the claim and the "OK" all happen under ONE lock, so (1) two
// clients can't grab the same name, and (2) no broadcast can reach this client
// before its "OK" (which would confuse the client's registration step).
static int register_name(int slot, const char *name) {
    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && clients[i].name[0] &&
            strcasecmp(clients[i].name, name) == 0) {
            pthread_mutex_unlock(&clients_lock);
            return 0;
        }
    }
    strcpy(clients[slot].name, name);
    send_line(clients[slot].sock, "OK");
    pthread_mutex_unlock(&clients_lock);
    return 1;
}

// ---------------------------------------------------------------- commands

static void cmd_help(int slot) {
    static const char *lines[] = {
        "Commands:",
        "  /help               show this help",
        "  /list               list online users",
        "  /msg <user> <text>  send a private message",
        "  /quit               leave the chat (same as 'exit')",
    };
    pthread_mutex_lock(&clients_lock);          // one lock so the block stays together
    for (size_t i = 0; i < sizeof(lines) / sizeof(lines[0]); i++)
        send_line(clients[slot].sock, lines[i]);
    pthread_mutex_unlock(&clients_lock);
}

static void cmd_list(int slot) {
    char names[MAX_CLIENTS * (NAME_MAX_LEN + 8)];
    char out[sizeof(names) + 40];
    int count = 0;
    names[0] = '\0';

    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && clients[i].name[0]) {
            if (count++) strcat(names, ", ");
            strcat(names, clients[i].name);
            if (i == slot) strcat(names, " (you)");
        }
    }
    snprintf(out, sizeof(out), "Online users (%d): %s", count, names);
    send_line(clients[slot].sock, out);
    pthread_mutex_unlock(&clients_lock);
}

// `rest` is everything after "/msg ", i.e. "<user> <text>".
static void cmd_msg(int slot, const char *name, char *rest) {
    char out[LINE_MAX_LEN + 64];
    char *text = strchr(rest, ' ');
    if (rest[0] == '\0' || text == NULL) {
        send_to_slot(slot, "ERR Usage: /msg <user> <message>");
        return;
    }
    *text++ = '\0';
    while (*text == ' ') text++;
    if (*text == '\0') {
        send_to_slot(slot, "ERR Usage: /msg <user> <message>");
        return;
    }
    if (strcasecmp(rest, name) == 0) {
        send_to_slot(slot, "ERR You can't message yourself.");
        return;
    }

    pthread_mutex_lock(&clients_lock);
    int target = -1;
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && clients[i].name[0] &&
            strcasecmp(clients[i].name, rest) == 0) { target = i; break; }
    }
    if (target < 0) {
        snprintf(out, sizeof(out), "ERR No such user: %.*s", NAME_MAX_LEN, rest);
        send_line(clients[slot].sock, out);
    } else {
        snprintf(out, sizeof(out), "[private] %s: %s", name, text);
        send_line(clients[target].sock, out);
        snprintf(out, sizeof(out), "[private to %s] %s", clients[target].name, text);
        send_line(clients[slot].sock, out);
        printf("Private: %s -> %s\n", name, clients[target].name);   // content stays private
        fflush(stdout);
    }
    pthread_mutex_unlock(&clients_lock);
}

// Returns 1 if the client asked to leave, else 0.
static int handle_command(int slot, const char *name, char *line) {
    char *rest = strchr(line, ' ');
    if (rest) {
        *rest++ = '\0';
        while (*rest == ' ') rest++;
    } else {
        rest = line + strlen(line);             // empty string
    }

    if (strcasecmp(line, "/quit") == 0) return 1;
    if (strcasecmp(line, "/help") == 0) cmd_help(slot);
    else if (strcasecmp(line, "/list") == 0) cmd_list(slot);
    else if (strcasecmp(line, "/msg") == 0) cmd_msg(slot, name, rest);
    else send_to_slot(slot, "ERR Unknown command. Type /help for a list.");
    return 0;
}

// ------------------------------------------------------------ client thread

static void *handle_client(void *arg) {
    int slot = *(int *)arg;
    free(arg);

    int sock = clients[slot].sock;
    LineReader rd;
    reader_init(&rd, sock);

    char line[LINE_MAX_LEN];
    char msg[LINE_MAX_LEN + 64];
    char name[NAME_MAX_LEN + 1] = "";
    int  alive = 1, registered = 0;

    // Phase 1: keep asking until we get a valid, unused name.
    while (alive && !registered) {
        if (read_line(&rd, line, sizeof(line)) <= 0) { alive = 0; break; }
        if (!valid_name(line)) {
            send_line(sock, "ERR Name must be 1-15 letters, digits or underscores.");
        } else if (register_name(slot, line)) {     // also sends "OK"
            strcpy(name, line);
            registered = 1;
        } else {
            send_line(sock, "ERR That name is already taken.");
        }
    }

    if (registered) {
        printf("%s connected.\n", name);
        fflush(stdout);
        snprintf(msg, sizeof(msg), "*** %s joined the chat ***", name);
        broadcast(msg, sock);
    }

    // Phase 2: handle commands and relay chat messages.
    while (alive && registered) {
        if (read_line(&rd, line, sizeof(line)) <= 0) break;
        if (line[0] == '\0') continue;
        if (strcmp(line, "exit") == 0) break;

        if (line[0] == '/') {
            if (handle_command(slot, name, line)) break;
            continue;
        }

        snprintf(msg, sizeof(msg), "%s: %s", name, line);
        printf("Broadcast: %s\n", msg);
        fflush(stdout);
        broadcast(msg, sock);
    }

    // Cleanup: free the slot first so nobody writes to a closed socket.
    pthread_mutex_lock(&clients_lock);
    clients[slot].active = 0;
    clients[slot].name[0] = '\0';
    pthread_mutex_unlock(&clients_lock);
    close(sock);

    if (registered) {
        printf("%s disconnected.\n", name);
        fflush(stdout);
        snprintf(msg, sizeof(msg), "*** %s left the chat ***", name);
        broadcast(msg, -1);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    int port = (argc > 1) ? atoi(argv[1]) : DEFAULT_PORT;
    signal(SIGPIPE, SIG_IGN);

    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    if (bind(listenfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    if (listen(listenfd, 10) < 0) { perror("listen"); return 1; }

    printf("Chat Server started on port %d...\n", port);
    fflush(stdout);

    for (;;) {
        int connfd = accept(listenfd, NULL, NULL);
        if (connfd < 0) { perror("accept"); continue; }

        pthread_mutex_lock(&clients_lock);
        int slot = -1;
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (!clients[i].active) { slot = i; break; }
        if (slot >= 0) {
            clients[slot].sock    = connfd;
            clients[slot].active  = 1;
            clients[slot].name[0] = '\0';
        }
        pthread_mutex_unlock(&clients_lock);

        if (slot < 0) {
            send_line(connfd, "ERR Server is full.");
            close(connfd);
            continue;
        }

        int *arg = malloc(sizeof(int));
        *arg = slot;
        pthread_t tid;
        if (pthread_create(&tid, NULL, handle_client, arg) != 0) {
            perror("pthread_create");
            pthread_mutex_lock(&clients_lock);
            clients[slot].active = 0;
            pthread_mutex_unlock(&clients_lock);
            close(connfd);
            free(arg);
            continue;
        }
        pthread_detach(tid);
    }
}