#pragma once
#include "rapidjson/document.h"
#include <bits/stdc++.h>

map<string, vector<string>> raft_groups;
map<string, string> group_leader;
bool migration_active = false;

struct ClientThreadArgs {
    int    sockfd;
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

RequestStats coordinator_stats;

struct RequestStats {
    long total_requests = 0;
    long cache_hits = 0;
    long redirects = 0;
};

RequestStats coordinator_stats;
