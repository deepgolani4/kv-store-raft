#pragma once

#include <fstream>
#include <set>
#include <string>

#include "common_functions.h"
#include "coordinator_state.h"
#include "lru.h"
#include "proto.h"
#include "rapidjson/document.h"
#include "ring.h"

using namespace rapidjson;
using namespace std;

inline void write_cs_config(const string &ip, const string &port) {
  ofstream f("cs_config.txt");
  f << ip << "\n" << port;
}

inline void ring_insert(const string &addr) {
  RingTree rt;
  ring_root = rt.insert(ring_root, ring_hash(addr), addr);
}

inline RingNode *responsible_node(const string &key) {
  if (!ring_root) return nullptr;
  RingTree rt;
  RingNode *pre = nullptr, *succ = nullptr;
  rt.find_neighbors(ring_root, pre, succ, ring_hash(key));
  return succ ? succ : rt.min_node(ring_root);
}

inline void register_raft_node(const string &group_id, const string &addr) {
  raft_groups[group_id].push_back(addr);
  ring_insert(group_id);
  if (!group_leader.count(group_id)) group_leader[group_id] = addr;
}

inline string send_to_group(const string &group_id, const string &msg) {
  for (int attempts = 0; attempts < 3; attempts++) {
    string target = group_leader.count(group_id)
                        ? group_leader[group_id]
                        : (raft_groups.count(group_id) && !raft_groups[group_id].empty() ? raft_groups[group_id][0] : "");
    if (target.empty()) return msg_ack("ack", "no_nodes_available");

    auto [ip, port] = split_addr(target);
    int fd = make_tcp_client();
    tcp_connect(fd, ip, port);
    net_send(fd, msg);
    string resp = net_recv(fd);
    close(fd);

    Document doc;
    string s = resp;
    if (!doc.ParseInsitu((char *)s.data()).HasParseError() && doc.HasMember("req_type") &&
        doc["req_type"].IsString() && string(doc["req_type"].GetString()) == "redirect") {
      if (doc.HasMember("leader") && doc["leader"].IsString()) {
        string new_leader = doc["leader"].GetString();
        if (!new_leader.empty()) group_leader[group_id] = new_leader;
      }
      coordinator_stats.redirects++;
      continue;
    }
    return resp;
  }
  return msg_ack("ack", "commit_failed");
}

inline string key_to_group(const string &key) {
  RingNode *node = responsible_node(key);
  return node ? node->addr : "";
}

inline void handle_get(int client_fd, const string &key) {
  coordinator_stats.total_requests++;
  if (cache.contains(key)) {
    coordinator_stats.cache_hits++;
    net_send(client_fd, msg_ack("data", cache.get(key)));
    return;
  }

  string group_id = key_to_group(key);
  if (group_id.empty()) {
    net_send(client_fd, msg_ack("ack", "no_nodes_available"));
    return;
  }

  string resp = send_to_group(group_id, msg_kv_op("get", key));
  Document doc;
  string s = resp;
  if (!doc.ParseInsitu((char *)s.data()).HasParseError() && doc.HasMember("req_type") &&
      doc["req_type"].IsString() && string(doc["req_type"].GetString()) == "data" &&
      doc.HasMember("message") && doc["message"].IsString()) {
    cache.put(key, doc["message"].GetString());
  }
  net_send(client_fd, resp);
}

inline void handle_put(int client_fd, const string &key, const string &value) {
  coordinator_stats.total_requests++;
  string group_id = key_to_group(key);
  if (group_id.empty()) {
    net_send(client_fd, msg_ack("ack", "no_nodes_available"));
    return;
  }
  net_send(client_fd, send_to_group(group_id, msg_kv_op("put", key, value)));
}

inline void handle_delete(int client_fd, const string &key) {
  coordinator_stats.total_requests++;
  string group_id = key_to_group(key);
  if (group_id.empty()) {
    net_send(client_fd, msg_ack("ack", "no_nodes_available"));
    return;
  }
  cache.remove(key);
  net_send(client_fd, send_to_group(group_id, msg_kv_op("delete", key)));
}

inline void handle_update(int client_fd, const string &key, const string &value) {
  coordinator_stats.total_requests++;
  string group_id = key_to_group(key);
  if (group_id.empty()) {
    net_send(client_fd, msg_ack("ack", "no_nodes_available"));
    return;
  }

  string resp = send_to_group(group_id, msg_kv_op("update", key, value));
  Document doc;
  string s = resp;
  if (!doc.ParseInsitu((char *)s.data()).HasParseError() && doc.HasMember("message") &&
      doc["message"].IsString() && string(doc["message"].GetString()) == "update_success") {
    cache.update(key, value);
  }
  net_send(client_fd, resp);
}

inline void serve_client(int client_fd) {
  while (true) {
    string msg = net_recv(client_fd);
    if (msg.empty()) return;

    Document doc;
    string s = msg;
    if (doc.ParseInsitu((char *)s.data()).HasParseError()) {
      net_send(client_fd, msg_ack("ack", "parse_error"));
      continue;
    }
    if (!doc.HasMember("req_type") || !doc["req_type"].IsString()) {
      net_send(client_fd, msg_ack("ack", "invalid_request"));
      continue;
    }

    string op = doc["req_type"].GetString();
    if (op == "get") {
      if (!doc.HasMember("key") || !doc["key"].IsString()) {
        net_send(client_fd, msg_ack("ack", "missing_key"));
      } else {
        handle_get(client_fd, doc["key"].GetString());
      }
    } else if (op == "put" || op == "update") {
      if (!doc.HasMember("key") || !doc["key"].IsString() || !doc.HasMember("value") ||
          !doc["value"].IsString()) {
        net_send(client_fd, msg_ack("ack", "missing_key_or_value"));
      } else if (op == "put") {
        handle_put(client_fd, doc["key"].GetString(), doc["value"].GetString());
      } else {
        handle_update(client_fd, doc["key"].GetString(), doc["value"].GetString());
      }
    } else if (op == "delete") {
      if (!doc.HasMember("key") || !doc["key"].IsString()) {
        net_send(client_fd, msg_ack("ack", "missing_key"));
      } else {
        handle_delete(client_fd, doc["key"].GetString());
      }
    } else {
      net_send(client_fd, msg_ack("ack", "unsupported_operation"));
    }
  }
}
