#pragma once

#include <map>
#include <string>
#include <vector>

using namespace std;

inline map<string, vector<string>> raft_groups;
inline map<string, string> group_leader;
inline bool migration_active = false;

struct ClientThreadArgs {
  int sockfd;
  string peer_addr;
  string self_addr;
};

struct HeartbeatListenerArgs {
  string cs_ip;
};

struct RequestStats {
  long total_requests = 0;
  long cache_hits = 0;
  long redirects = 0;
};

inline RequestStats coordinator_stats;
