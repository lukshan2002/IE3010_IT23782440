# NetMessenger - IE3010 Network Programming (IT23782440)

Multi-client chat and file-sharing server and client in C (BSD sockets, pthreads).

## Personalised values
| Item | Value |
|---|---|
| Registration number | IT23782440 |
| Port | 6000 + 2440 = **8440** |
| NID tag | digits 3-6 of 23782440 = **NID:7824** |
| Source files | server_2440.c, client_2440.c, Makefile_2440 |
| Log file | netmsg_IT23782440.log |
| Storage path | ./storage/IT23782440/<sender>/<filename> |
| Submission archive | IE3010_IT23782440.zip |

## Build
    make -f Makefile_2440          # needs gcc + make on Linux
    make -f Makefile_2440 clean

## Run
    ./server_2440                  # listens on 0.0.0.0:8440
    ./client_2440 [server_ip] [port]   # default 127.0.0.1 8440

## Client usage
Type protocol commands: REGISTER <name>, LIST, BCAST <msg>, PMSG <user> <msg>,
JOIN <room>, LEAVE <room>, ROOMS, RMSG <room> <msg>, QUIT.
Files: SENDFILE <user|room> <local-path>  (client sends SENDFILE <target> <filename> <size> + bytes).
Received files are saved in received_<yourname>/.

## Design
- Concurrency: one POSIX thread per client; shared users/rooms protected by a mutex.
- Extension: rate limiting (10 messages per second per client, ERR 013 RATE_LIMITED).
- See docs/design_diary.md and docs/prompt_log.md.
