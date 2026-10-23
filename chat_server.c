// chat_server.c - single-threaded epoll chat server
// Build: make        Run: ./chat_server [port] [logfile] [--quiet]
//        defaults: port 8080, logfile chat.log; --quiet silences the console
//
// Same protocol and commands as the thread-per-client version, but ONE thread
// serves every client with a non-blocking epoll event loop:
//   * no mutexes: only one thread ever touches the data
//   * non-blocking sockets + per-client input/output buffers
//   * a slow reader can no longer stall the others (output is queued, and
//     clients that fall too far behind are disconnected)
//
// Commands: /help  /list  /rooms  /join <room>  /msg <user> <text>  /quit

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <stdarg.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include "common.h"

#define ROOM_MAX_LEN    15
#define DEFAULT_ROOM    "general"
#define DEFAULT_LOG     "chat.log"
#define MAX_EVENTS      256
#define LISTEN_BACKLOG  1024
#define MAX_FD_CAP      65536              // upper bound on fds / clients we plan for
#define FD_RESERVE      32                 // fds kept for stdio, epoll, listener, log file
#define MAX_OUTBUF      (256 * 1024)       // pending output allowed per client
#define MAX_OUT_LINE    (LINE_MAX_LEN + 63)

typedef struct {
    int    fd;
    int    idx;                            // position in all[]
    int    registered;
    int    dead;                           // scheduled for removal at the end of this loop pass
    int    want_out;                       // EPOLLOUT currently enabled?
    char   name[NAME_MAX_LEN + 1];
    char   room[ROOM_MAX_LEN + 1];
    char   inbuf[LINE_MAX_LEN * 2];        // bytes received, not yet a full line
    size_t inlen;
    char  *out;                            // bytes waiting to be sent
    size_t outlen, outcap, outoff;         // used / allocated / already sent
} Client;

static int      epfd = -1, listenfd = -1;
static Client **by_fd;                     // fd -> client
static int      fd_cap;
static Client **all;                       // dense list of live clients (for broadcasts)
static int      nall, max_clients;
static Client **dead_list;                 // clients to remove after the current batch
static int      ndead;
static volatile sig_atomic_t stop_flag;

// ------------------------------------------------------------------ logging

static FILE *log_fp;
static int   quiet, log_dirty;

static void server_log(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void server_log(const char *fmt, ...) {
    char msg[LINE_MAX_LEN + 128];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    if (!quiet) printf("%s\n", msg);
    if (log_fp) {
        char ts[32];
        time_t now = time(NULL);
        struct tm tmv;
        localtime_r(&now, &tmv);
        strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);
        fprintf(log_fp, "[%s] %s\n", ts, msg);
    }
    log_dirty = 1;
}

// Flushed once per event-loop pass instead of once per line (far fewer syscalls).
static void flush_logs(void) {
    if (!log_dirty) return;
    fflush(stdout);
    if (log_fp) fflush(log_fp);
    log_dirty = 0;
}

// ------------------------------------------------------------ output queue

static void mark_dead(Client *c) {
    if (c->dead) return;
    c->dead = 1;
    dead_list[ndead++] = c;
}

static void update_epoll(Client *c, int want_out) {
    if (c->want_out == want_out) return;
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events  = EPOLLIN | (want_out ? EPOLLOUT : 0);
    ev.data.fd = c->fd;
    epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd, &ev);
    c->want_out = want_out;
}

// Write as much pending output as the socket accepts. Returns -1 on a fatal error.
static int flush_out(Client *c) {
    while (c->outoff < c->outlen) {
        ssize_t n = send(c->fd, c->out + c->outoff, c->outlen - c->outoff, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {   // socket full: wait for EPOLLOUT
                update_epoll(c, 1);
                return 0;
            }
            return -1;
        }
        c->outoff += (size_t)n;
    }
    c->outoff = c->outlen = 0;
    if (c->outcap > 16 * 1024) {                // don't keep big buffers around for idle clients
        free(c->out);
        c->out = NULL;
        c->outcap = 0;
    }
    update_epoll(c, 0);
    return 0;
}

// Queue one line for a client and try to send it right away.
static void queue_line(Client *c, const char *text) {
    if (c->dead) return;
    size_t len = strlen(text);
    if (len > MAX_OUT_LINE) len = MAX_OUT_LINE;
    size_t pending = c->outlen - c->outoff;

    if (pending + len + 1 > MAX_OUTBUF) {       // backpressure: this reader is too slow
        server_log("Dropping slow client %s (output buffer full)",
                   c->registered ? c->name : "(unregistered)");
        mark_dead(c);
        return;
    }
    if (c->outlen + len + 1 > c->outcap) {
        if (c->outoff > 0) {                    // reclaim the part already sent
            memmove(c->out, c->out + c->outoff, pending);
            c->outlen = pending;
            c->outoff = 0;
        }
        if (c->outlen + len + 1 > c->outcap) {
            size_t ncap = c->outcap ? c->outcap : 512;
            while (ncap < c->outlen + len + 1) ncap *= 2;
            char *p = realloc(c->out, ncap);
            if (!p) { mark_dead(c); return; }
            c->out = p;
            c->outcap = ncap;
        }
    }
    memcpy(c->out + c->outlen, text, len);
    c->out[c->outlen + len] = '\n';
    c->outlen += len + 1;

    if (!c->want_out && flush_out(c) < 0) mark_dead(c);   // else EPOLLOUT will flush it
}

// ------------------------------------------------------------ lookup/send

static Client *find_by_name(const char *name) {
    for (int i = 0; i < nall; i++) {
        Client *c = all[i];
        if (c->registered && !c->dead && strcasecmp(c->name, name) == 0) return c;
    }
    return NULL;
}

static void send_room(const char *room, const char *msg, const Client *except) {
    for (int i = 0; i < nall; i++) {
        Client *c = all[i];
        if (c != except && c->registered && !c->dead && strcmp(c->room, room) == 0)
            queue_line(c, msg);
    }
}

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

// ---------------------------------------------------------------- commands

static void cmd_help(Client *me) {
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
    for (size_t i = 0; i < sizeof(lines) / sizeof(lines[0]); i++)
        queue_line(me, lines[i]);
}

static void cmd_list(Client *me) {
    char names[1024];
    char out[LINE_MAX_LEN + 64];
    size_t off = 0;
    int count = 0, truncated = 0;
    names[0] = '\0';

    for (int i = 0; i < nall; i++) {
        Client *c = all[i];
        if (!c->registered || c->dead || strcmp(c->room, me->room) != 0) continue;
        count++;
        if (truncated) continue;
        const char *suffix = (c == me) ? " (you)" : "";
        size_t need = (off ? 2 : 0) + strlen(c->name) + strlen(suffix);
        if (off + need + 6 <= sizeof(names)) {          // always keep room for ", ..."
            off += (size_t)snprintf(names + off, sizeof(names) - off, "%s%s%s",
                                    off ? ", " : "", c->name, suffix);
        } else {
            strcpy(names + off, ", ...");
            truncated = 1;
        }
    }
    snprintf(out, sizeof(out), "Users in #%s (%d): %s", me->room, count, names);
    queue_line(me, out);
}

static void cmd_rooms(Client *me) {
    size_t cap = (size_t)(nall > 0 ? nall : 1);
    char (*rooms)[ROOM_MAX_LEN + 1] = malloc(cap * (ROOM_MAX_LEN + 1));
    int *counts = malloc(cap * sizeof(int));
    if (!rooms || !counts) {
        free(rooms); free(counts);
        queue_line(me, "ERR Out of memory.");
        return;
    }

    int n = 0;
    for (int i = 0; i < nall; i++) {
        Client *c = all[i];
        if (!c->registered || c->dead) continue;
        int j;
        for (j = 0; j < n; j++)
            if (strcmp(rooms[j], c->room) == 0) break;
        if (j < n) counts[j]++;
        else { strcpy(rooms[n], c->room); counts[n++] = 1; }
    }

    char out[LINE_MAX_LEN + 64];
    size_t off = (size_t)snprintf(out, sizeof(out), "Rooms (%d):", n);
    for (int j = 0; j < n; j++) {
        if (off + 40 >= sizeof(out)) { strcat(out, " ..."); break; }
        off += (size_t)snprintf(out + off, sizeof(out) - off, "%s #%s (%d)%s",
                                j ? "," : "", rooms[j], counts[j],
                                strcmp(rooms[j], me->room) == 0 ? " <- you" : "");
    }
    queue_line(me, out);
    free(rooms);
    free(counts);
}

static void cmd_join(Client *me, const char *arg) {
    char room[ROOM_MAX_LEN + 1], old[ROOM_MAX_LEN + 1];
    char out[LINE_MAX_LEN + 64];

    if (!normalize_room(arg, room)) {
        queue_line(me, "ERR Usage: /join <room>  (1-15 letters, digits, '_' or '-')");
        return;
    }
    if (strcmp(me->room, room) == 0) {
        snprintf(out, sizeof(out), "You are already in #%s.", room);
        queue_line(me, out);
        return;
    }
    strcpy(old, me->room);

    snprintf(out, sizeof(out), "*** %s moved to #%s ***", me->name, room);
    send_room(old, out, me);

    strcpy(me->room, room);

    snprintf(out, sizeof(out), "*** %s joined #%s ***", me->name, room);
    send_room(room, out, me);

    snprintf(out, sizeof(out), "You joined #%s.", room);
    queue_line(me, out);

    server_log("%s moved from #%s to #%s", me->name, old, room);
}

// `rest` is everything after "/msg ", i.e. "<user> <text>".
static void cmd_msg(Client *me, char *rest) {
    char out[LINE_MAX_LEN + 64];
    char *text = strchr(rest, ' ');
    if (rest[0] == '\0' || text == NULL) {
        queue_line(me, "ERR Usage: /msg <user> <message>");
        return;
    }
    *text++ = '\0';
    while (*text == ' ') text++;
    if (*text == '\0') {
        queue_line(me, "ERR Usage: /msg <user> <message>");
        return;
    }
    if (strcasecmp(rest, me->name) == 0) {
        queue_line(me, "ERR You can't message yourself.");
        return;
    }

    Client *target = find_by_name(rest);
    if (!target) {
        snprintf(out, sizeof(out), "ERR No such user: %.*s", NAME_MAX_LEN, rest);
        queue_line(me, out);
        return;
    }
    snprintf(out, sizeof(out), "[private] %s: %s", me->name, text);
    queue_line(target, out);
    snprintf(out, sizeof(out), "[private to %s] %s", target->name, text);
    queue_line(me, out);
    server_log("Private: %s -> %s", me->name, target->name);     // content stays private
}

// Returns 1 if the client asked to leave, else 0.
static int handle_command(Client *me, char *line) {
    char *rest = strchr(line, ' ');
    if (rest) {
        *rest++ = '\0';
        while (*rest == ' ') rest++;
    } else {
        rest = line + strlen(line);             // empty string
    }

    if (strcasecmp(line, "/quit") == 0) return 1;
    if (strcasecmp(line, "/help") == 0) cmd_help(me);
    else if (strcasecmp(line, "/list") == 0) cmd_list(me);
    else if (strcasecmp(line, "/rooms") == 0) cmd_rooms(me);
    else if (strcasecmp(line, "/join") == 0) cmd_join(me, rest);
    else if (strcasecmp(line, "/msg") == 0) cmd_msg(me, rest);
    else queue_line(me, "ERR Unknown command. Type /help for a list.");
    return 0;
}

// ------------------------------------------------------- one line from a client

static void process_line(Client *c, char *line) {
    char msg[LINE_MAX_LEN + 64];

    if (!c->registered) {                       // the first valid line is the username
        if (!valid_name(line)) {
            queue_line(c, "ERR Name must be 1-15 letters, digits or underscores.");
        } else if (find_by_name(line)) {
            queue_line(c, "ERR That name is already taken.");
        } else {
            strcpy(c->name, line);
            strcpy(c->room, DEFAULT_ROOM);
            c->registered = 1;
            queue_line(c, "OK");
            server_log("%s connected.", c->name);
            snprintf(msg, sizeof(msg), "*** %s joined the chat ***", c->name);
            send_room(DEFAULT_ROOM, msg, c);
        }
        return;
    }

    if (line[0] == '\0') return;
    if (strcmp(line, "exit") == 0) { mark_dead(c); return; }
    if (line[0] == '/') {
        if (handle_command(c, line)) mark_dead(c);
        return;
    }

    snprintf(msg, sizeof(msg), "%s: %s", c->name, line);
    send_room(c->room, msg, c);
    server_log("Broadcast [#%s] %s", c->room, msg);
}

// ------------------------------------------------------------- socket events

static void handle_readable(Client *c) {
    ssize_t n = recv(c->fd, c->inbuf + c->inlen, sizeof(c->inbuf) - c->inlen, 0);
    if (n == 0) { mark_dead(c); return; }                           // client closed
    if (n < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) mark_dead(c);
        return;
    }
    c->inlen += (size_t)n;

    // Hand out every complete line. TCP may deliver half a line or several lines.
    size_t start = 0;
    while (!c->dead) {
        size_t avail = c->inlen - start;
        char *nl = memchr(c->inbuf + start, '\n', avail);
        size_t line_len, consumed;
        if (nl) {
            line_len = (size_t)(nl - (c->inbuf + start));
            consumed = line_len + 1;
        } else if (avail == sizeof(c->inbuf)) {                     // too long: force it out
            line_len = consumed = avail;
        } else {
            break;                                                  // wait for more data
        }

        char line[LINE_MAX_LEN];
        size_t copy = line_len < sizeof(line) - 1 ? line_len : sizeof(line) - 1;
        memcpy(line, c->inbuf + start, copy);
        line[copy] = '\0';
        if (copy > 0 && line[copy - 1] == '\r') line[copy - 1] = '\0';
        start += consumed;

        process_line(c, line);
    }
    memmove(c->inbuf, c->inbuf + start, c->inlen - start);
    c->inlen -= start;
}

static void accept_clients(void) {
    for (;;) {
        int fd = accept4(listenfd, NULL, NULL, SOCK_NONBLOCK);
        if (fd < 0) {
            if (errno == EINTR) continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK) perror("accept");
            return;                                                 // no more pending connections
        }
        if (nall >= max_clients || fd >= fd_cap) {
            static const char full[] = "ERR Server is full.\n";
            send(fd, full, sizeof(full) - 1, MSG_NOSIGNAL);
            close(fd);
            continue;
        }
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)); // don't delay small chat lines

        Client *c = calloc(1, sizeof(*c));
        if (!c) { close(fd); continue; }
        c->fd  = fd;
        c->idx = nall;

        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events  = EPOLLIN;
        ev.data.fd = fd;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            perror("epoll_ctl add");
            free(c);
            close(fd);
            continue;
        }
        by_fd[fd]  = c;
        all[nall++] = c;
    }
}

// Remove everything scheduled for removal. Announcing a departure can itself
// overflow another slow client's buffer, so keep going until the list is empty.
static void reap_dead(void) {
    char msg[LINE_MAX_LEN + 64];
    while (ndead > 0) {
        Client *c = dead_list[--ndead];

        int i = c->idx;                         // swap-remove from the dense list
        all[i] = all[--nall];
        all[i]->idx = i;
        by_fd[c->fd] = NULL;
        epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);
        close(c->fd);

        if (c->registered) {
            server_log("%s disconnected.", c->name);
            snprintf(msg, sizeof(msg), "*** %s left the chat ***", c->name);
            send_room(c->room, msg, NULL);
        }
        free(c->out);
        free(c);
    }
}

static void shutdown_all(void) {
    for (int i = 0; i < nall; i++)
        if (all[i]->registered) queue_line(all[i], "*** Server is shutting down ***");
    for (int i = 0; i < nall; i++) {
        close(all[i]->fd);
        free(all[i]->out);
        free(all[i]);
    }
    nall = 0;
}

static void on_signal(int sig) { (void)sig; stop_flag = 1; }

int main(int argc, char *argv[]) {
    int port = DEFAULT_PORT;
    const char *log_path = DEFAULT_LOG;
    int npos = 0;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--quiet") == 0 || strcmp(argv[i], "-q") == 0) quiet = 1;
        else if (npos == 0) { port = atoi(argv[i]); npos++; }
        else if (npos == 1) { log_path = argv[i]; npos++; }
    }

    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;                  // no SA_RESTART: epoll_wait returns on Ctrl+C
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    // Every client needs a file descriptor: raise the soft limit as far as allowed.
    struct rlimit rl;
    getrlimit(RLIMIT_NOFILE, &rl);
    rlim_t want = MAX_FD_CAP;
    if (rl.rlim_max != RLIM_INFINITY && rl.rlim_max < want) want = rl.rlim_max;
    if (rl.rlim_cur < want) {
        rl.rlim_cur = want;
        setrlimit(RLIMIT_NOFILE, &rl);
        getrlimit(RLIMIT_NOFILE, &rl);
    }
    fd_cap      = rl.rlim_cur > MAX_FD_CAP ? MAX_FD_CAP : (int)rl.rlim_cur;
    max_clients = fd_cap - FD_RESERVE;
    by_fd     = calloc((size_t)fd_cap, sizeof(Client *));
    all       = calloc((size_t)fd_cap, sizeof(Client *));
    dead_list = calloc((size_t)fd_cap, sizeof(Client *));
    if (!by_fd || !all || !dead_list) { perror("calloc"); return 1; }

    log_fp = fopen(log_path, "a");
    if (!log_fp) perror("warning: cannot open log file (continuing without logging)");

    listenfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (listenfd < 0) { perror("socket"); return 1; }
    int opt = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);
    if (bind(listenfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    if (listen(listenfd, LISTEN_BACKLOG) < 0) { perror("listen"); return 1; }

    epfd = epoll_create1(0);
    if (epfd < 0) { perror("epoll_create1"); return 1; }
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events  = EPOLLIN;
    ev.data.fd = listenfd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev);

    server_log("Chat Server started on port %d (epoll, up to %d clients)...", port, max_clients);
    if (log_fp) server_log("Logging to %s", log_path);
    flush_logs();

    struct epoll_event events[MAX_EVENTS];
    while (!stop_flag) {
        // 1s timeout so a signal that lands just before epoll_wait is still noticed
        int n = epoll_wait(epfd, events, MAX_EVENTS, 1000);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            if (fd == listenfd) { accept_clients(); continue; }

            Client *c = by_fd[fd];
            if (!c || c->dead) continue;
            uint32_t e = events[i].events;

            if (e & EPOLLIN) handle_readable(c);                    // also notices EOF
            else if (e & (EPOLLERR | EPOLLHUP)) { mark_dead(c); continue; }

            if (!c->dead && (e & EPOLLOUT) && flush_out(c) < 0) mark_dead(c);
        }
        reap_dead();
        flush_logs();
    }

    server_log("Shutting down...");
    shutdown_all();
    close(listenfd);
    close(epfd);
    server_log("Server stopped.");
    flush_logs();
    if (log_fp) fclose(log_fp);
    free(by_fd);
    free(all);
    free(dead_list);
    return 0;
}