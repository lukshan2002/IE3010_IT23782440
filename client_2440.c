/*
 * client_2440.c  -  NetMessenger client (IE3010 Network Programming)
 *
 * Registration number : IT23782440
 * Default server      : 127.0.0.1 : 8440
 *
 * Two threads:
 *   main thread     -> reads what you type and sends it to the server
 *   receiver thread -> prints everything the server sends, saves files
 *
 * Usage : ./client_2440 [server_ip] [port]
 * Type protocol commands directly, e.g.  REGISTER amal   BCAST hello
 * File  : SENDFILE <user|room> <path-to-local-file>
 *         (the client turns this into SENDFILE <target> <filename> <size>
 *          and then sends the raw bytes, as the protocol requires)
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <libgen.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define DEFAULT_HOST  "127.0.0.1"
#define DEFAULT_PORT  8440            /* 6000 + 2440 */
#define LINE_MAX_LEN  4096

static int    sockfd;
static char   inbuf[LINE_MAX_LEN];
static size_t inlen = 0;
static char   myname[64] = "";

static int send_all(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        p += n; len -= (size_t)n;
    }
    return 0;
}

/* same framing logic as the server: 1 = line, 0 = closed, -1 = error */
static int read_line(char *out, size_t outsz)
{
    for (;;) {
        char *nl = memchr(inbuf, '\n', inlen);
        if (nl) {
            size_t len = (size_t)(nl - inbuf), used = len + 1;
            if (len >= outsz) len = outsz - 1;
            memcpy(out, inbuf, len);
            out[len] = '\0';
            memmove(inbuf, inbuf + used, inlen - used);
            inlen -= used;
            return 1;
        }
        if (inlen == sizeof inbuf) inlen = 0;          /* overlong: drop   */
        ssize_t n = recv(sockfd, inbuf + inlen, sizeof inbuf - inlen, 0);
        if (n == 0) return 0;
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        inlen += (size_t)n;
    }
}

/* read exactly n bytes and write them to fp (fp may be NULL = discard) */
static int read_exact_to_file(FILE *fp, long n)
{
    long got = 0;
    if (inlen > 0) {
        size_t take = inlen < (size_t)n ? inlen : (size_t)n;
        if (fp) fwrite(inbuf, 1, take, fp);
        memmove(inbuf, inbuf + take, inlen - take);
        inlen -= take;
        got = (long)take;
    }
    char tmp[8192];
    while (got < n) {
        size_t want = (size_t)(n - got) < sizeof tmp ? (size_t)(n - got) : sizeof tmp;
        ssize_t r = recv(sockfd, tmp, want, 0);
        if (r == 0) return 0;
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (fp) fwrite(tmp, 1, (size_t)r, fp);
        got += r;
    }
    return 1;
}

static void *receiver(void *arg)
{
    (void)arg;
    char line[LINE_MAX_LEN + 1];
    for (;;) {
        int r = read_line(line, sizeof line);
        if (r <= 0) {
            printf("*** disconnected from server ***\n");
            fflush(stdout);
            exit(0);
        }
        printf("%s\n", line);
        fflush(stdout);

        /* remember our own name: "OK REGISTERED <name> NID:...." */
        if (strncmp(line, "OK REGISTERED ", 14) == 0)
            sscanf(line + 14, "%63s", myname);

        /* incoming file: "MSG FILE <sender> <filename> <size>" + raw bytes */
        if (strncmp(line, "MSG FILE ", 9) == 0) {
            char sender[64], fname[256];
            long size;
            if (sscanf(line + 9, "%63s %255s %ld", sender, fname, &size) != 3 || size < 0)
                continue;
            char dir[256], path[600];
            snprintf(dir, sizeof dir, "received_%s", myname[0] ? myname : "client");
            mkdir(dir, 0755);
            snprintf(path, sizeof path, "%s/%s", dir, basename(fname));
            FILE *fp = fopen(path, "wb");
            if (read_exact_to_file(fp, size) <= 0) {
                printf("*** disconnected during file transfer ***\n");
                exit(0);
            }
            if (fp) {
                fclose(fp);
                printf("[saved %ld bytes from %s to %s]\n", size, sender, path);
            } else {
                printf("[could not save %s]\n", path);
            }
            fflush(stdout);
        }
    }
    return NULL;
}

/* "SENDFILE <target> <localpath>"  ->  header + raw bytes */
static void send_file_cmd(char *args)
{
    char target[64], localpath[1024];
    if (sscanf(args, "%63s %1023s", target, localpath) != 2) {
        printf("usage: SENDFILE <user|room> <path-to-file>\n");
        return;
    }
    FILE *fp = fopen(localpath, "rb");
    if (!fp) { perror("open file"); return; }
    struct stat st;
    if (fstat(fileno(fp), &st) < 0 || !S_ISREG(st.st_mode)) {
        printf("not a regular file\n");
        fclose(fp);
        return;
    }
    char pathcopy[1024];
    snprintf(pathcopy, sizeof pathcopy, "%s", localpath);
    char *fname = basename(pathcopy);

    char hdr[512];
    int n = snprintf(hdr, sizeof hdr, "SENDFILE %s %s %lld\n",
                     target, fname, (long long)st.st_size);
    if (send_all(sockfd, hdr, (size_t)n) < 0) { fclose(fp); return; }

    char buf[8192];
    size_t r;
    while ((r = fread(buf, 1, sizeof buf, fp)) > 0)
        if (send_all(sockfd, buf, r) < 0) break;
    fclose(fp);
}

int main(int argc, char *argv[])
{
    const char *host = argc > 1 ? argv[1] : DEFAULT_HOST;
    int port = argc > 2 ? atoi(argv[2]) : DEFAULT_PORT;

    signal(SIGPIPE, SIG_IGN);
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); return 1; }

    struct sockaddr_in srv;
    memset(&srv, 0, sizeof srv);
    srv.sin_family = AF_INET;
    srv.sin_port   = htons((unsigned short)port);
    if (inet_pton(AF_INET, host, &srv.sin_addr) != 1) {
        fprintf(stderr, "bad IP address: %s\n", host);
        return 1;
    }
    if (connect(sockfd, (struct sockaddr *)&srv, sizeof srv) < 0) {
        perror("connect");
        return 1;
    }
    printf("Connected to %s:%d  (first command must be: REGISTER <name>)\n", host, port);

    pthread_t tid;
    pthread_create(&tid, NULL, receiver, NULL);

    char line[LINE_MAX_LEN + 2];   /* +2: room for the added '\n' */
    int quit_sent = 0;
    while (fgets(line, sizeof line, stdin)) {
        line[strcspn(line, "\r\n")] = '\0';
        if (line[0] == '\0') continue;
        if (strncmp(line, "SENDFILE ", 9) == 0) {
            send_file_cmd(line + 9);
            continue;
        }
        strcat(line, "\n");
        if (send_all(sockfd, line, strlen(line)) < 0) break;
        if (strcmp(line, "QUIT\n") == 0) { quit_sent = 1; break; }
    }
    /* stdin closed (Ctrl+D) without QUIT -> send QUIT politely */
    if (!quit_sent) send_all(sockfd, "QUIT\n", 5);
    pthread_join(tid, NULL);   /* receiver exits when server closes */
    return 0;
}
