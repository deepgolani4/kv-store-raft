# KV Store with Coordinator Routing

A distributed key-value store prototype in C++.

## Components
- `coordinator`: accepts client requests, tracks registered nodes, routes key operations.
- `raft_node`: storage node process (currently single-node semantics per key-group in this runnable version).
- `client`: CLI to run `get`, `put`, `update`, `delete`.

## Build (macOS/Linux)
```bash
make
```

Or explicitly:
```bash
c++ -std=c++17 -pthread coordinator.cpp -o coordinator
c++ -std=c++17 -pthread raft_node.cpp -o raft_node
c++ -std=c++17 -pthread client.cpp -o client
```

## Run
Terminal 1:
```bash
./coordinator 127.0.0.1 9000
```

Terminal 2:
```bash
./raft_node 127.0.0.1 9101
```

Terminal 3:
```bash
./client 127.0.0.1 9200
```

Client commands:
- `put:<key>:<value>`
- `get:<key>`
- `update:<key>:<value>`
- `delete:<key>`
- `exit`

## Status
This is still a prototype codebase. The current runnable path is coordinator + storage node + client.
