/*
 * server_2440.c  -  NetMessenger server (IE3010 Network Programming)
 *
 * Registration number : IT23782440
 * Listening port      : 8440        (6000 + 2440)
 * Node ID tag         : NID:7824    (digits 3-6 of 23782440)
 * Log file            : netmsg_IT23782440.log
 * Storage path        : ./storage/IT23782440/<sender>/<filename>
 *
 * Concurrency model   : one POSIX thread per connected client
 *                       (thread-per-connection). Shared state (users, rooms)
 *                       is protected by one mutex (state_lock).
 *
 * Build : make -f Makefile_2440
 * Run   : ./server_2440
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ===================== PHASE 1: personalised values ===================== */
#define REG_NO        "IT23782440"
#define SERVER_PORT   8440                  /* 6000 + 2440                 */
#define NID_TAG       "NID:7824"            /* middle 4 digits of 23782440 */
#define LOG_FILE      "netmsg_" REG_NO ".log"
#define STORAGE_ROOT  "./storage/" REG_NO

/* ===================== limits ===================== */
#define MAX_CLIENTS   64
#define MAX_ROOMS     32
#define MAX_NAME      32                    /* username / room name length */
#define MAX_FNAME     128                   /* file name length            */
#define LINE_MAX_LEN  4096                  /* longest accepted text line  */
#define MAX_FILE_SIZE (10L * 1024 * 1024)   /* 10 MB                       */
#define RATE_LIMIT    10                    /* extension: msgs per window  */
#define RATE_WINDOW   1                     /* window length in seconds    */

typedef struct {
    int    in_use;                 /* slot occupied?                       */
    int    fd;                     /* connected socket                     */
    int    registered;             /* REGISTER done?                       */
    char   name[MAX_NAME];
    char   ip[INET_ADDRSTRLEN];
    int    port;
    char   inbuf[LINE_MAX_LEN];    /* bytes received but not yet used      */
    size_t inlen;
    int    discarding;             /* skipping the rest of a too-long line */
    pthread_mutex_t send_lock;     /* one writer at a time on this socket  */
    time_t rate_start;             /* rate-limit window start              */
    int    rate_count;
} client_t;

typedef struct {
    int  in_use;
    char name[MAX_NAME];
    int  member[MAX_CLIENTS];      /* member[i] == 1 -> clients[i] in room */
} room_t;

static client_t clients[MAX_CLIENTS];
static room_t   rooms[MAX_ROOMS];
static pthread_mutex_t state_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_lock   = PTHREAD_MUTEX_INITIALIZER;
static FILE *log_fp = NULL;

/* ===================== PHASE 1: logging ===================== */
static void log_event(const char *fmt, ...)
{
    char ts[32];
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", &tmv);

    va_list ap;
    pthread_mutex_lock(&log_lock);
    fprintf(log_fp, "[%s] ", ts);
    va_start(ap, fmt); vfprintf(log_fp, fmt, ap); va_end(ap);
    fputc('\n', log_fp);
    fflush(log_fp);
    printf("[%s] ", ts);                      /* same line on the terminal */
    va_start(ap, fmt); vprintf(fmt, ap); va_end(ap);
    putchar('\n');
    fflush(stdout);
    pthread_mutex_unlock(&log_lock);
}

/* ===================== PHASE 2: sending helpers ===================== */
/* send() may write fewer bytes than asked, so loop until all are sent. */
static int send_all(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p   += n;
        len -= (size_t)n;
    }
    return 0;
}

static int send_raw(client_t *c, const char *data, size_t len)
{
    pthread_mutex_lock(&c->send_lock);
    int r = send_all(c->fd, data, len);
    pthread_mutex_unlock(&c->send_lock);
    return r;
}

/* Response to the client that sent the command: ALWAYS ends " NID:7824\n" */
static void reply(client_t *c, const char *fmt, ...)
{
    char line[LINE_MAX_LEN + 64];
    size_t cap = sizeof line - 16;            /* room for " NID:7824\n"     */
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(line, cap, fmt, ap);
    va_end(ap);
    if (n < 0) return;
    if ((size_t)n >= cap) n = (int)cap - 1;
    snprintf(line + n, sizeof line - (size_t)n, " %s\n", NID_TAG);
    send_raw(c, line, strlen(line));
}

/* Line forwarded to OTHER clients (MSG ...): NO NID tag. */
static void forward(client_t *c, const char *fmt, ...)
{
    char line[LINE_MAX_LEN + 128];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(line, sizeof line - 1, fmt, ap);
    va_end(ap);
    if (n < 0) return;
    if ((size_t)n >= sizeof line - 1) n = (int)sizeof line - 2;
    line[n++] = '\n';
    send_raw(c, line, (size_t)n);
}

/* ===================== PHASE 2: framing (receiving) ===================== */
/*
 * Read ONE line (without '\n') into out.
 * Handles: a partial line in one recv(), several lines in one recv().
 * Returns 1 = line ready, 0 = peer closed, -1 = error, -2 = line too long.
 */
static int read_line(client_t *c, char *out, size_t outsz)
{
    for (;;) {
        char *nl = memchr(c->inbuf, '\n', c->inlen);
        if (nl) {
            size_t len  = (size_t)(nl - c->inbuf);
            size_t used = len + 1;
            int drop = c->discarding;
            if (!drop) {
                if (len >= outsz) len = outsz - 1;
                memcpy(out, c->inbuf, len);
                out[len] = '\0';
                if (len > 0 && out[len - 1] == '\r') out[len - 1] = '\0';
            }
            memmove(c->inbuf, c->inbuf + used, c->inlen - used);
            c->inlen -= used;
            if (drop) { c->discarding = 0; continue; }
            return 1;
        }
        if (c->inlen == sizeof c->inbuf) {    /* full buffer, no newline   */
            c->inlen = 0;
            if (!c->discarding) { c->discarding = 1; return -2; }
            continue;
        }
        ssize_t n = recv(c->fd, c->inbuf + c->inlen,
                         sizeof c->inbuf - c->inlen, 0);
        if (n == 0) return 0;
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        c->inlen += (size_t)n;
    }
}

/*
 * Read EXACTLY n raw bytes (SENDFILE body). Bytes already sitting in inbuf
 * are used first, then recv() is called as many times as needed.
 * dst == NULL means "read and throw away" (used for rejected files).
 * Returns 1 = ok, 0 = peer closed, -1 = error.
 */
static int read_exact(client_t *c, char *dst, size_t n)
{
    size_t got = 0;
    if (c->inlen > 0) {
        size_t take = c->inlen < n ? c->inlen : n;
        if (dst) memcpy(dst, c->inbuf, take);
        memmove(c->inbuf, c->inbuf + take, c->inlen - take);
        c->inlen -= take;
        got = take;
    }
    char tmp[8192];
    while (got < n) {
        size_t want = n - got;
        char *p = dst ? dst + got : tmp;
        if (!dst && want > sizeof tmp) want = sizeof tmp;
        ssize_t r = recv(c->fd, p, want, 0);
        if (r == 0) return 0;
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        got += (size_t)r;
    }
    return 1;
}

/* ===================== small helpers ===================== */
/* Cut the next space-separated word off *p. Returns NULL if none left. */
static char *next_word(char **p)
{
    char *s = *p;
    while (*s == ' ') s++;
    if (*s == '\0') { *p = s; return NULL; }
    char *start = s;
    while (*s && *s != ' ') s++;
    if (*s) *s++ = '\0';
    *p = s;
    return start;
}

static char *skip_spaces(char *s) { while (*s == ' ') s++; return s; }

/* usernames and room names: 1-31 chars of A-Z a-z 0-9 _ - */
static int valid_name(const char *s)
{
    size_t n = strlen(s);
    if (n == 0 || n >= MAX_NAME) return 0;
    for (; *s; s++)
        if (!(isalnum((unsigned char)*s) || *s == '_' || *s == '-')) return 0;
    return 1;
}

/* file names: no '/', cannot start with '.', 1-127 chars of A-Z a-z 0-9 . _ - */
static int valid_filename(const char *s)
{
    size_t n = strlen(s);
    if (n == 0 || n >= MAX_FNAME || s[0] == '.') return 0;
    for (; *s; s++)
        if (!(isalnum((unsigned char)*s) || *s == '.' || *s == '_' || *s == '-'))
            return 0;
    return 1;
}

/* like "mkdir -p" */
static int mkdir_p(const char *path)
{
    char tmp[512];
    snprintf(tmp, sizeof tmp, "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0755) < 0 && errno != EEXIST) return -1;
            *p = '/';
        }
    }
    if (mkdir(tmp, 0755) < 0 && errno != EEXIST) return -1;
    return 0;
}

/* The two finders below must be called with state_lock held. */
static int find_client(const char *name)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i].in_use && clients[i].registered &&
            strcmp(clients[i].name, name) == 0)
            return i;
    return -1;
}

static int find_room(const char *name)
{
    for (int i = 0; i < MAX_ROOMS; i++)
        if (rooms[i].in_use && strcmp(rooms[i].name, name) == 0) return i;
    return -1;
}

static int room_is_empty(int r)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (rooms[r].member[i]) return 0;
    return 1;
}

/* Extension: flood protection - at most RATE_LIMIT messages per second. */
static int rate_limited(client_t *c)
{
    time_t now = time(NULL);
    if (now - c->rate_start >= RATE_WINDOW) {
        c->rate_start = now;
        c->rate_count = 0;
    }
    return ++c->rate_count > RATE_LIMIT;
}

/* header + bytes sent under ONE lock so no other line can get in between */
static void deliver_file(client_t *to, const char *sender, const char *fname,
                         const char *data, long size)
{
    char hdr[256];
    int n = snprintf(hdr, sizeof hdr, "MSG FILE %s %s %ld\n", sender, fname, size);
    pthread_mutex_lock(&to->send_lock);
    if (send_all(to->fd, hdr, (size_t)n) == 0 && size > 0)
        send_all(to->fd, data, (size_t)size);
    pthread_mutex_unlock(&to->send_lock);
}

/* ===================== PHASE 3+: command handlers ===================== */
static int handle_sendfile(client_t *c, int idx, char *args);

/* Returns 0 = keep going, 1 = client sent QUIT, -1 = connection lost */
static int handle_command(client_t *c, int idx, char *line)
{
    char *p = line;
    char *cmd = next_word(&p);
    if (!cmd) return 0;                              /* ignore empty line   */

    /* ---- QUIT (allowed before REGISTER too) ---- */
    if (strcmp(cmd, "QUIT") == 0) {
        reply(c, "OK BYE");
        return 1;
    }

    /* ---- REGISTER <username> ---- */
    if (strcmp(cmd, "REGISTER") == 0) {
        char *name = next_word(&p);
        if (c->registered)  { reply(c, "ERR 009 ALREADY_REGISTERED"); return 0; }
        if (!name)          { reply(c, "ERR 007 MISSING_ARGUMENT");   return 0; }
        if (*skip_spaces(p) || !valid_name(name)) {
            reply(c, "ERR 008 INVALID_USERNAME");
            return 0;
        }
        pthread_mutex_lock(&state_lock);
        if (find_client(name) >= 0) {
            pthread_mutex_unlock(&state_lock);
            reply(c, "ERR 001 USERNAME_TAKEN");
            log_event("REGISTER_FAIL %s:%d name=%s (taken)", c->ip, c->port, name);
            return 0;
        }
        snprintf(c->name, sizeof c->name, "%s", name);
        c->registered = 1;
        for (int i = 0; i < MAX_CLIENTS; i++)        /* presence notice     */
            if (i != idx && clients[i].in_use && clients[i].registered)
                forward(&clients[i], "MSG SYS JOINED %s", c->name);
        pthread_mutex_unlock(&state_lock);
        reply(c, "OK REGISTERED %s", c->name);
        log_event("REGISTER %s:%d as %s", c->ip, c->port, c->name);
        return 0;
    }

    /* every other command needs REGISTER first */
    if (!c->registered) { reply(c, "ERR 005 NOT_REGISTERED"); return 0; }

    /* ---- LIST ---- */
    if (strcmp(cmd, "LIST") == 0) {
        char list[MAX_CLIENTS * MAX_NAME + 1] = "";
        pthread_mutex_lock(&state_lock);
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (clients[i].in_use && clients[i].registered) {
                if (list[0]) strcat(list, ",");
                strcat(list, clients[i].name);
            }
        pthread_mutex_unlock(&state_lock);
        reply(c, "OK USERS %s", list);
        return 0;
    }

    /* ---- BCAST <message> ---- */
    if (strcmp(cmd, "BCAST") == 0) {
        char *msg = skip_spaces(p);
        if (!*msg)           { reply(c, "ERR 007 MISSING_ARGUMENT"); return 0; }
        if (rate_limited(c)) { reply(c, "ERR 013 RATE_LIMITED");     return 0; }
        pthread_mutex_lock(&state_lock);
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (i != idx && clients[i].in_use && clients[i].registered)
                forward(&clients[i], "MSG BCAST %s %s", c->name, msg);
        pthread_mutex_unlock(&state_lock);
        reply(c, "OK SENT");
        log_event("BCAST from=%s msg=\"%s\"", c->name, msg);
        return 0;
    }

    /* ---- PMSG <username> <message> ---- */
    if (strcmp(cmd, "PMSG") == 0) {
        char *to  = next_word(&p);
        char *msg = skip_spaces(p);
        if (!to || !*msg)    { reply(c, "ERR 007 MISSING_ARGUMENT"); return 0; }
        if (rate_limited(c)) { reply(c, "ERR 013 RATE_LIMITED");     return 0; }
        pthread_mutex_lock(&state_lock);
        int t = find_client(to);
        if (t >= 0) forward(&clients[t], "MSG PRIV %s %s", c->name, msg);
        pthread_mutex_unlock(&state_lock);
        if (t < 0) { reply(c, "ERR 002 USER_NOT_FOUND"); return 0; }
        reply(c, "OK SENT");
        log_event("PMSG from=%s to=%s msg=\"%s\"", c->name, to, msg);
        return 0;
    }

    /* ---- JOIN <room> (creates the room if needed) ---- */
    if (strcmp(cmd, "JOIN") == 0) {
        char *room = next_word(&p);
        if (!room)             { reply(c, "ERR 007 MISSING_ARGUMENT");  return 0; }
        if (!valid_name(room)) { reply(c, "ERR 015 INVALID_ROOM_NAME"); return 0; }
        pthread_mutex_lock(&state_lock);
        int r = find_room(room);
        int created = 0;
        if (r < 0) {
            for (int i = 0; i < MAX_ROOMS; i++)
                if (!rooms[i].in_use) { r = i; break; }
            if (r >= 0) {
                memset(&rooms[r], 0, sizeof rooms[r]);
                rooms[r].in_use = 1;
                snprintf(rooms[r].name, sizeof rooms[r].name, "%s", room);
                created = 1;
            }
        }
        if (r >= 0) rooms[r].member[idx] = 1;
        pthread_mutex_unlock(&state_lock);
        if (r < 0) { reply(c, "ERR 017 ROOM_LIMIT_REACHED"); return 0; }
        reply(c, "OK JOINED %s", room);
        log_event("JOIN user=%s room=%s%s", c->name, room, created ? " (created)" : "");
        return 0;
    }

    /* ---- LEAVE <room> ---- */
    if (strcmp(cmd, "LEAVE") == 0) {
        char *room = next_word(&p);
        if (!room) { reply(c, "ERR 007 MISSING_ARGUMENT"); return 0; }
        int err = 0;
        pthread_mutex_lock(&state_lock);
        int r = find_room(room);
        if (r < 0)                       err = 3;
        else if (!rooms[r].member[idx])  err = 12;
        else {
            rooms[r].member[idx] = 0;
            if (room_is_empty(r)) rooms[r].in_use = 0;   /* delete empty room */
        }
        pthread_mutex_unlock(&state_lock);
        if (err == 3)  { reply(c, "ERR 003 ROOM_NOT_FOUND"); return 0; }
        if (err == 12) { reply(c, "ERR 012 NOT_IN_ROOM");    return 0; }
        reply(c, "OK LEFT %s", room);
        log_event("LEAVE user=%s room=%s", c->name, room);
        return 0;
    }

    /* ---- ROOMS ---- */
    if (strcmp(cmd, "ROOMS") == 0) {
        char list[MAX_ROOMS * MAX_NAME + 1] = "";
        pthread_mutex_lock(&state_lock);
        for (int i = 0; i < MAX_ROOMS; i++)
            if (rooms[i].in_use) {
                if (list[0]) strcat(list, ",");
                strcat(list, rooms[i].name);
            }
        pthread_mutex_unlock(&state_lock);
        reply(c, "OK ROOMS %s", list[0] ? list : "(none)");
        return 0;
    }

    /* ---- RMSG <room> <message> ---- */
    if (strcmp(cmd, "RMSG") == 0) {
        char *room = next_word(&p);
        char *msg  = skip_spaces(p);
        if (!room || !*msg)  { reply(c, "ERR 007 MISSING_ARGUMENT"); return 0; }
        if (rate_limited(c)) { reply(c, "ERR 013 RATE_LIMITED");     return 0; }
        int err = 0;
        pthread_mutex_lock(&state_lock);
        int r = find_room(room);
        if (r < 0)                       err = 3;
        else if (!rooms[r].member[idx])  err = 12;
        else
            for (int i = 0; i < MAX_CLIENTS; i++)
                if (i != idx && rooms[r].member[i] && clients[i].in_use)
                    forward(&clients[i], "MSG ROOM %s %s %s", room, c->name, msg);
        pthread_mutex_unlock(&state_lock);
        if (err == 3)  { reply(c, "ERR 003 ROOM_NOT_FOUND"); return 0; }
        if (err == 12) { reply(c, "ERR 012 NOT_IN_ROOM");    return 0; }
        reply(c, "OK SENT");
        log_event("RMSG from=%s room=%s msg=\"%s\"", c->name, room, msg);
        return 0;
    }

    /* ---- SENDFILE <target> <filename> <filesize> + raw bytes ---- */
    if (strcmp(cmd, "SENDFILE") == 0)
        return handle_sendfile(c, idx, p);

    reply(c, "ERR 006 UNKNOWN_COMMAND");
    log_event("BAD_COMMAND from=%s cmd=\"%s\"", c->name, cmd);
    return 0;
}

/* ===================== PHASE 5: file transfer ===================== */
static int handle_sendfile(client_t *c, int idx, char *p)
{
    char *target = next_word(&p);
    char *fname  = next_word(&p);
    char *szstr  = next_word(&p);
    if (!target || !fname || !szstr) {               /* size unknown:       */
        reply(c, "ERR 007 MISSING_ARGUMENT");        /* nothing to drain    */
        return 0;
    }
    char *end;
    errno = 0;
    long long size = strtoll(szstr, &end, 10);
    if (errno || *end || size < 0 || szstr[0] == '-' || szstr[0] == '+') {
        reply(c, "ERR 011 INVALID_FILESIZE");
        return 0;
    }

    /* The client sends the bytes no matter what, so on any error we must
       still read (drain) exactly <filesize> bytes to stay in sync. */
    if (size > MAX_FILE_SIZE) {
        int r = read_exact(c, NULL, (size_t)size);
        if (r <= 0) return -1;
        reply(c, "ERR 004 FILE_TOO_LARGE");
        log_event("SENDFILE_REJECT from=%s file=%s size=%lld (too large)",
                  c->name, fname, size);
        return 0;
    }

    char *data = malloc(size > 0 ? (size_t)size : 1);
    if (!data) {
        if (read_exact(c, NULL, (size_t)size) <= 0) return -1;
        reply(c, "ERR 018 SERVER_ERROR");
        return 0;
    }
    int r = read_exact(c, data, (size_t)size);
    if (r <= 0) { free(data); return -1; }

    if (!valid_filename(fname)) {
        free(data); reply(c, "ERR 010 INVALID_FILENAME"); return 0;
    }
    if (rate_limited(c)) {
        free(data); reply(c, "ERR 013 RATE_LIMITED"); return 0;
    }

    /* user names are checked first, then room names */
    int err = 0, delivered = 0;
    pthread_mutex_lock(&state_lock);
    int t = find_client(target);
    if (t >= 0) {
        deliver_file(&clients[t], c->name, fname, data, (long)size);
        delivered = 1;
    } else {
        int rm = find_room(target);
        if (rm < 0)                       err = 2;
        else if (!rooms[rm].member[idx])  err = 12;
        else
            for (int i = 0; i < MAX_CLIENTS; i++)
                if (i != idx && rooms[rm].member[i] && clients[i].in_use) {
                    deliver_file(&clients[i], c->name, fname, data, (long)size);
                    delivered++;
                }
    }
    pthread_mutex_unlock(&state_lock);

    if (err == 2)  { free(data); reply(c, "ERR 002 USER_NOT_FOUND"); return 0; }
    if (err == 12) { free(data); reply(c, "ERR 012 NOT_IN_ROOM");    return 0; }

    /* keep a copy: ./storage/IT23782440/<sender>/<filename> */
    char dir[256], path[512];
    snprintf(dir, sizeof dir, "%s/%s", STORAGE_ROOT, c->name);
    snprintf(path, sizeof path, "%s/%s", dir, fname);
    FILE *fp = NULL;
    if (mkdir_p(dir) == 0) fp = fopen(path, "wb");
    if (!fp || (size > 0 && fwrite(data, 1, (size_t)size, fp) != (size_t)size)) {
        if (fp) fclose(fp);
        free(data);
        reply(c, "ERR 018 SERVER_ERROR");
        log_event("SENDFILE_STORE_FAIL from=%s path=%s", c->name, path);
        return 0;
    }
    fclose(fp);
    free(data);
    reply(c, "OK FILE_RECEIVED %s", fname);
    log_event("SENDFILE from=%s to=%s file=%s size=%lld recipients=%d stored=%s",
              c->name, target, fname, size, delivered, path);
    return 0;
}

/* ===================== PHASE 6: disconnect + cleanup ===================== */
static void remove_client(int idx, const char *reason)
{
    client_t *c = &clients[idx];
    char name[MAX_NAME];
    int was_registered;

    pthread_mutex_lock(&state_lock);
    was_registered = c->registered;
    snprintf(name, sizeof name, "%s", c->name);
    for (int r = 0; r < MAX_ROOMS; r++)              /* leave every room    */
        if (rooms[r].in_use && rooms[r].member[idx]) {
            rooms[r].member[idx] = 0;
            if (room_is_empty(r)) rooms[r].in_use = 0;
        }
    if (was_registered)                              /* presence notice     */
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (i != idx && clients[i].in_use && clients[i].registered)
                forward(&clients[i], "MSG SYS LEFT %s", name);
    close(c->fd);
    c->in_use = 0;
    c->registered = 0;
    c->name[0] = '\0';
    pthread_mutex_unlock(&state_lock);

    log_event("DISCONNECT %s:%d user=%s reason=%s", c->ip, c->port,
              was_registered ? name : "(unregistered)", reason);
}

/* ===================== PHASE 2: one thread per client ===================== */
static void *client_thread(void *arg)
{
    int idx = (int)(intptr_t)arg;
    client_t *c = &clients[idx];
    char line[LINE_MAX_LEN + 1];
    const char *reason = "QUIT";

    for (;;) {
        int r = read_line(c, line, sizeof line);
        if (r == -2) { reply(c, "ERR 014 LINE_TOO_LONG"); continue; }
        if (r == 0)  { reason = "connection closed by peer"; break; }
        if (r < 0)   { reason = "recv error (connection reset)"; break; }
        int h = handle_command(c, idx, line);
        if (h == 1) break;                                   /* QUIT        */
        if (h < 0) { reason = "connection lost during file transfer"; break; }
    }
    remove_client(idx, reason);
    return NULL;
}

/* ===================== PHASE 1: main / accept loop ===================== */
int main(void)
{
    signal(SIGPIPE, SIG_IGN);   /* writing to a dead socket must not kill us */

    log_fp = fopen(LOG_FILE, "a");
    if (!log_fp) { perror("fopen log"); return 1; }
    if (mkdir_p(STORAGE_ROOT) < 0) { perror("mkdir storage"); return 1; }
    for (int i = 0; i < MAX_CLIENTS; i++)
        pthread_mutex_init(&clients[i].send_lock, NULL);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return 1; }
    int yes = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(SERVER_PORT);
    if (bind(lfd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("bind"); return 1;
    }
    if (listen(lfd, 16) < 0) { perror("listen"); return 1; }

    log_event("SERVER_START reg=%s port=%d tag=%s log=%s storage=%s",
              REG_NO, SERVER_PORT, NID_TAG, LOG_FILE, STORAGE_ROOT);

    for (;;) {
        struct sockaddr_in cli;
        socklen_t len = sizeof cli;
        int cfd = accept(lfd, (struct sockaddr *)&cli, &len);
        if (cfd < 0) {
            if (errno != EINTR) perror("accept");
            continue;
        }
        /* a client that never reads must not block the server forever */
        struct timeval tv = { 5, 0 };
        setsockopt(cfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
        setsockopt(cfd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof yes);

        pthread_mutex_lock(&state_lock);
        int idx = -1;
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (!clients[i].in_use) { idx = i; break; }
        if (idx < 0) {
            pthread_mutex_unlock(&state_lock);
            const char *full = "ERR 016 SERVER_FULL " NID_TAG "\n";
            send_all(cfd, full, strlen(full));
            close(cfd);
            continue;
        }
        client_t *c = &clients[idx];
        c->in_use = 1;
        c->fd = cfd;
        c->registered = 0;
        c->name[0] = '\0';
        c->inlen = 0;
        c->discarding = 0;
        c->rate_start = 0;
        c->rate_count = 0;
        inet_ntop(AF_INET, &cli.sin_addr, c->ip, sizeof c->ip);
        c->port = ntohs(cli.sin_port);
        pthread_mutex_unlock(&state_lock);

        log_event("CONNECT %s:%d slot=%d", c->ip, c->port, idx);

        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread, (void *)(intptr_t)idx) != 0) {
            perror("pthread_create");
            remove_client(idx, "thread create failed");
            continue;
        }
        pthread_detach(tid);
    }
}
