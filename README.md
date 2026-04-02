# KV Store with Raft

A simple distributed key-value store prototype using Raft-style coordination.

## What It Includes

- `coordinator.cpp`: accepts client requests and routes them
- `raft_node.cpp`: node-side key/value + replication logic
- `client.cpp`: simple CLI for `get`, `put`, `update`, `delete`

## Build

Compile with your preferred C++ compiler (g++/clang++).

## Run (Quick Start)

1. Start the coordinator.
2. Start one or more raft nodes.
3. Run the client and issue commands.

## Notes

- This is a learning/prototype project, not production-ready.
- Network addresses and ports must match across components.
