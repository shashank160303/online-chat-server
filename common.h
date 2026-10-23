// common.h - shared protocol helpers for chat server and client
//
// PROTOCOL (v1): every message is ONE line of text ending in '\n'.
//   client -> server : first line is the username, then one chat message per line
//   server -> client : "OK" or "ERR <reason>" after the username is sent,
//                      then one chat/announcement line per message
#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>

#define DEFAULT_PORT 8080
#define LINE_MAX_LEN 1024
#define NAME_MAX_LEN 15

// Buffers incoming bytes so we can hand back exactly one full line at a time.
// TCP is a byte stream: one recv() may contain half a message or several messages.
typedef struct {
    int    sock;
    char   buf[LINE_MAX_LEN * 2];
    size_t len;
} LineReader;

static inline void reader_init(LineReader *r, int sock) {
    r->sock = sock;
    r->len  = 0;
}

// Returns 1 = got a line in `out`, 0 = connection closed, -1 = error.
static inline int read_line(LineReader *r, char *out, size_t outsz) {
    for (;;) {
        size_t line_len, consumed;
        char *nl = memchr(r->buf, '\n', r->len);

        if (nl) {                                   // a full line is buffered
            line_len = (size_t)(nl - r->buf);
            consumed = line_len + 1;
        } else if (r->len == sizeof(r->buf)) {      // line too long: force it out
            line_len = consumed = r->len;
        } else {                                    // need more data
            ssize_t n = recv(r->sock, r->buf + r->len, sizeof(r->buf) - r->len, 0);
            if (n == 0) return 0;
            if (n < 0) {
                if (errno == EINTR) continue;
                return -1;
            }
            r->len += (size_t)n;
            continue;
        }

        size_t copy = line_len < outsz - 1 ? line_len : outsz - 1;
        memcpy(out, r->buf, copy);
        out[copy] = '\0';
        if (copy > 0 && out[copy - 1] == '\r') out[copy - 1] = '\0';

        memmove(r->buf, r->buf + consumed, r->len - consumed);
        r->len -= consumed;
        return 1;
    }
}

// Sends `text` plus a trailing '\n'. Returns 0 on success, -1 on error.
static inline int send_line(int sock, const char *text) {
    char out[LINE_MAX_LEN + 64];
    int n = snprintf(out, sizeof(out), "%s\n", text);
    if (n < 0) return -1;
    if ((size_t)n >= sizeof(out)) {                 // truncated: keep the '\n'
        n = (int)sizeof(out) - 1;
        out[n - 1] = '\n';
    }
    size_t sent = 0;
    while (sent < (size_t)n) {
        ssize_t k = send(sock, out + sent, (size_t)n - sent, MSG_NOSIGNAL);
        if (k < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += (size_t)k;
    }
    return 0;
}

#endif