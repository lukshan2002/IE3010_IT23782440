Design Diary - NetMessenger (IT23782440)

2026-10-01 - Project setup and planning

* I started by reading the NetMessenger requirements and breaking the project into smaller implementation phases.
* I set up the GitHub repository, project directory, README, `.gitignore` and design diary. I also confirmed the personalised registration number, NID `7824` and server port `8440`.
* I decided to implement the server incrementally so that each major feature could be compiled and tested before moving to the next phase.

2026-10-02 - Server skeleton and TCP connection handling

* I implemented the initial server socket, binding and listening on port `8440`, followed by accepting client connections.
* I used a thread-per-client approach with pthreads because multiple clients need to communicate with the server at the same time.
* I tested the basic connection handling and checked that the server could remain running while clients connected and disconnected.

2026-10-03 - Protocol framing and registration

* I implemented command processing and registration, including unique username checking and the required NID in server responses.
* A problem I encountered was that TCP does not preserve command boundaries. A command such as `REGISTER framer` could arrive as `REGISTER fr` first and `amer\n` later.
* I fixed this by keeping received data in a per-client buffer and processing only complete newline-terminated commands. This also allowed several commands received together to be processed separately.

2026-10-04 - Messaging and rooms

* I implemented `LIST`, `BCAST` and `PMSG`, then added room operations such as `J
