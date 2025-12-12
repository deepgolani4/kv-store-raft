CXX ?= c++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -pthread

BINARIES = coordinator raft_node client

all: $(BINARIES)

coordinator: coordinator.cpp
	$(CXX) $(CXXFLAGS) coordinator.cpp -o coordinator

raft_node: raft_node.cpp
	$(CXX) $(CXXFLAGS) raft_node.cpp -o raft_node

client: client.cpp
	$(CXX) $(CXXFLAGS) client.cpp -o client

clean:
	rm -f $(BINARIES)

.PHONY: all clean
