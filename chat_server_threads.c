// chat_server_threads.c - thread-per-client version (kept to benchmark against the epoll server)
// Build: make        Run: ./chat_server [port] [logfile]
//        defaults: port 8080, logfile chat.log
//
// Commands (any line starting with '/'):
//   /help                show the command list
//   /list                list users in your current room
//   /rooms               list all rooms and their user counts
//   /join <room>         switch to a room (created on first use)
//   /msg <user> <text>   private message (works across rooms)
//   /quit                leave (same as typing "exit")

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <arpa/inet.h>
#include "common.h"

#define MAX_CLIENTS   2000
#define ROOM_MAX_LEN  15
#define DEFAULT_ROOM  "general"
#define DEFAULT_LOG   "chat.log"

typedef struct {
    int  sock;
    int  active;                    // slot in use?
    char name[NAME_MAX_LEN + 1];    // empty until the user picks a valid name
    char room[ROOM_MAX_LEN + 1];    // current room (lowercase)
} Client;

static Client clients[MAX_CLIENTS];
static pthread_mutex_t clients_lock = PTHREAD_MUTEX_INITIALIZER;

// LOCK RULES
//  1. Every send to a registered client happens while holding clients_lock,
//     so lines from different threads never interleave on a socket.
//  2. Lock order is always clients_lock -> log_lock (never the reverse).

// ------------------------------------------------------------------ logging

static FILE *log_fp = NULL;
static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;

// Prints to the console and appends "[timestamp] text" to the log file.
static void server_log(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void server_log(const char *fmt, ...) {
    char msg[LINE_MAX_LEN + 128];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    char ts[32];
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);

    pthread_mutex_lock(&log_lock);
    printf("%s\n", msg);
    fflush(stdout);
    if (log_fp) {
        fprintf(log_fp, "[%s] %s\n", ts, msg);
        fflush(log_fp);                         // flush every line: survives crashes, works with tail -f
    }
    pthread_mutex_unlock(&log_lock);
}

// -------------------------------------------------------------- sending

// Send to everyone in `room` except `except_sock`. Caller MUST hold clients_lock.
static void send_room_locked(const char *room, const char *msg, int except_sock) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && clients[i].name[0] &&
            clients[i].sock != except_sock && strcmp(clients[i].room, room) == 0)
            send_line(clients[i].sock, msg);
    }
}

static void send_room(const char *room, const char *msg, int except_sock) {
    pthread_mutex_lock(&clients_lock);
    send_room_locked(room, msg, except_sock);
    pthread_mutex_unlock(&clients_lock);
}

// Send a line to one client (errors, replies).
static void send_to_slot(int slot, const char *msg) {
    pthread_mutex_lock(&clients_lock);
    send_line(clients[slot].sock, msg);
    pthread_mutex_unlock(&clients_lock);
}

// ------------------------------------------------------------ validation

static int valid_name(const char *s) {
    size_t n = strlen(s);
    if (n < 1 || n > NAME_MAX_LEN) return 0;
    for (size_t i = 0; i < n; i++)
        if (!isalnum((unsigned char)s[i]) && s[i] != '_') return 0;
    return 1;
}

// Accepts "dev", "Dev", "#dev"; writes the lowercase form to `out`.
static int normalize_room(const char *arg, char *out) {
    if (*arg == '#') arg++;
    size_t n = strlen(arg);
    if (n < 1 || n > ROOM_MAX_LEN) return 0;
    for (size_t i = 0; i < n; i++) {
        if (!isalnum((unsigned char)arg[i]) && arg[i] != '_' && arg[i] != '-') return 0;
        out[i] = (char)tolower((unsigned char)arg[i]);
    }
    out[n] = '\0';
    return 1;
}

// Claim `name`, put the client in the default room, and confirm with "OK".
// Check + claim + "OK" happen under ONE lock, so (1) two clients can't take the
// same name and (2) no broadcast can reach this client before its "OK".
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
    strcpy(clients[slot].room, DEFAULT_ROOM);
    send_line(clients[slot].sock, "OK");
    pthread_mutex_unlock(&clients_lock);
    return 1;
}

// ---------------------------------------------------------------- chat

// Relay a chat message to everyone in the sender's current room.
static void relay_chat(int slot, const char *name, const char *text) {
    char out[LINE_MAX_LEN + 64];
    pthread_mutex_lock(&clients_lock);
    snprintf(out, sizeof(out), "%s: %s", name, text);
    send_room_locked(clients[slot].room, out, clients[slot].sock);
    server_log("Broadcast [#%s] %s", clients[slot].room, out);
    pthread_mutex_unlock(&clients_lock);
}

// ------------------------------------------------------------- commands

static void cmd_help(int slot) {
    static const char *lines[] = {
        "Commands:",
        "  /help               show this help",
        "  /list               list users in your current room",
        "  /rooms              list all rooms and their user counts",
        "  /join <room>        switch to a room (created on first use)",
        "  /msg <user> <text>  send a private message (works across rooms)",
        "  /quit               leave the chat (same as 'exit')",
        "You start in #" DEFAULT_ROOM ".",
    };
    pthread_mutex_lock(&clients_lock);          // one lock so the block stays together
    for (size_t i = 0; i < sizeof(lines) / sizeof(lines[0]); i++)
        send_line(clients[slot].sock, lines[i]);
    pthread_mutex_unlock(&clients_lock);
}

static void cmd_list(int slot) {
    char names[MAX_CLIENTS * (NAME_MAX_LEN + 8)];
    char out[sizeof(names) + 64];
    int count = 0;
    names[0] = '\0';

    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && clients[i].name[0] &&
            strcmp(clients[i].room, clients[slot].room) == 0) {
            if (count++) strcat(names, ", ");
            strcat(names, clients[i].name);
            if (i == slot) strcat(names, " (you)");
        }
    }
    snprintf(out, sizeof(out), "Users in #%s (%d): %s", clients[slot].room, count, names);
    send_line(clients[slot].sock, out);
    pthread_mutex_unlock(&clients_lock);
}

static void cmd_rooms(int slot) {
    char rooms[MAX_CLIENTS][ROOM_MAX_LEN + 1];
    int  counts[MAX_CLIENTS];
    int  n = 0;
    char out[4096];

    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!(clients[i].active && clients[i].name[0])) continue;
        int j;
        for (j = 0; j < n; j++)
            if (strcmp(rooms[j], clients[i].room) == 0) break;
        if (j < n) counts[j]++;
        else { strcpy(rooms[n], clients[i].room); counts[n++] = 1; }
    }
    size_t off = (size_t)snprintf(out, sizeof(out), "Rooms (%d):", n);
    for (int j = 0; j < n && off < sizeof(out); j++) {
        off += (size_t)snprintf(out + off, sizeof(out) - off, "%s #%s (%d)%s",
                                j ? "," : "", rooms[j], counts[j],
                                strcmp(rooms[j], clients[slot].room) == 0 ? " <- you" : "");
    }
    send_line(clients[slot].sock, out);
    pthread_mutex_unlock(&clients_lock);
}

static void cmd_join(int slot, const char *name, const char *arg) {
    char room[ROOM_MAX_LEN + 1], old[ROOM_MAX_LEN + 1];
    char out[LINE_MAX_LEN + 64];

    if (!normalize_room(arg, room)) {
        send_to_slot(slot, "ERR Usage: /join <room>  (1-15 letters, digits, '_' or '-')");
        return;
    }

    // Leave announcement, room change and join announcement are ONE atomic step.
    pthread_mutex_lock(&clients_lock);
    if (strcmp(clients[slot].room, room) == 0) {
        snprintf(out, sizeof(out), "You are already in #%s.", room);
        send_line(clients[slot].sock, out);
        pthread_mutex_unlock(&clients_lock);
        return;
    }
    strcpy(old, clients[slot].room);

    snprintf(out, sizeof(out), "*** %s moved to #%s ***", name, room);
    send_room_locked(old, out, clients[slot].sock);

    strcpy(clients[slot].room, room);

    snprintf(out, sizeof(out), "*** %s joined #%s ***", name, room);
    send_room_locked(room, out, clients[slot].sock);

    snprintf(out, sizeof(out), "You joined #%s.", room);
    send_line(clients[slot].sock, out);

    server_log("%s moved from #%s to #%s", name, old, room);
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
        server_log("Private: %s -> %s", name, clients[target].name);   // content stays private
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
    else if (strcasecmp(line, "/rooms") == 0) cmd_rooms(slot);
    else if (strcasecmp(line, "/join") == 0) cmd_join(slot, name, rest);
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
    char room[ROOM_MAX_LEN + 1] = "";
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
        server_log("%s connected.", name);
        snprintf(msg, sizeof(msg), "*** %s joined the chat ***", name);
        send_room(DEFAULT_ROOM, msg, sock);
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
        relay_chat(slot, name, line);
    }

    // Cleanup: free the slot first so nobody writes to a closed socket.
    pthread_mutex_lock(&clients_lock);
    if (registered) strcpy(room, clients[slot].room);
    clients[slot].active = 0;
    clients[slot].name[0] = '\0';
    pthread_mutex_unlock(&clients_lock);
    close(sock);

    if (registered) {
        server_log("%s disconnected.", name);
        snprintf(msg, sizeof(msg), "*** %s left the chat ***", name);
        send_room(room, msg, -1);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    int port = (argc > 1) ? atoi(argv[1]) : DEFAULT_PORT;
    const char *log_path = (argc > 2) ? argv[2] : DEFAULT_LOG;
    signal(SIGPIPE, SIG_IGN);

    log_fp = fopen(log_path, "a");
    if (!log_fp) perror("warning: cannot open log file (continuing without logging)");

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
    if (listen(listenfd, 1024) < 0) { perror("listen"); return 1; }

    server_log("Chat Server started on port %d...", port);
    if (log_fp) server_log("Logging to %s", log_path);

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
            clients[slot].room[0] = '\0';
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