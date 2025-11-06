#pragma once
#include <fstream>
#include <bits/stdc++.h>
#include "common_functions.h"
#include "ring.h"
#include "proto.h"
#include "coordinator_state.h"
#include "lru.h"
#include "rapidjson/document.h"
using namespace std;
using namespace rapidjson;

void write_cs_config(const string &ip, const string &port) {
    ofstream f("cs_config.txt");
    f << ip << "\n" << port;
}

static void ring_insert(const string &addr) {
    RingTree rt;
    ring_root = rt.insert(ring_root, ring_hash(addr), addr);
}

static RingNode* responsible_node(const string &key) {
    if (!ring_root) return nullptr;
    RingTree rt;
    RingNode *pre = nullptr, *succ = nullptr;
    rt.find_neighbors(ring_root, pre, succ, ring_hash(key));
    return succ ? succ : rt.min_node(ring_root);
}

void register_raft_node(const string &group_id, const string &addr) {
    raft_groups[group_id].push_back(addr);
    ring_insert(group_id);
    if (!group_leader.count(group_id))
        group_leader[group_id] = addr;
}

static string send_to_group(const string &group_id, const string &msg) {
    for (int attempts = 0; attempts < 3; attempts++) {
        string target = group_leader.count(group_id)
                      ? group_leader[group_id]
                      : (raft_groups.count(group_id) && !raft_groups[group_id].empty()
                         ? raft_groups[group_id][0] : "");
        if (target.empty()) return msg_ack("ack", "no_nodes_available");

        auto [ip, port] = split_addr(target);
        int fd = make_tcp_client();
        tcp_connect(fd, ip, port);
        net_send(fd, msg);
        string resp = net_recv(fd);
        close(fd);

        Document doc;
        string s = resp;
        if (!doc.ParseInsitu((char*)s.c_str()).HasParseError()
            && doc.HasMember("req_type")
            && string(doc["req_type"].GetString()) == "redirect") {
            string new_leader = doc["leader"].GetString();
            if (!new_leader.empty()) group_leader[group_id] = new_leader;
            continue;
        }
        return resp;
    }
    return msg_ack("ack", "commit_failed");
}

static string key_to_group(const string &key) {
    RingNode *node = responsible_node(key);
    return node ? node->addr : "";  
}


void handle_get(int client_fd, const string &key) {
    if (cache.contains(key)) {
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
    if (!doc.ParseInsitu((char*)s.c_str()).HasParseError()
        && doc.HasMember("req_type")
        && string(doc["req_type"].GetString()) == "data") {
        cache.put(key, doc["message"].GetString());
    }
    net_send(client_fd, resp);
}

void handle_put(int client_fd, const string &key, const string &value) {
    string group_id = key_to_group(key);
    if (group_id.empty()) { net_send(client_fd, msg_ack("ack", "no_nodes_available")); return; }

    string resp = send_to_group(group_id, msg_kv_op("put", key, value));
    net_send(client_fd, resp);
}

void handle_delete(int client_fd, const string &key) {
    string group_id = key_to_group(key);
    if (group_id.empty()) { net_send(client_fd, msg_ack("ack", "no_nodes_available")); return; }

    cache.remove(key);
    string resp = send_to_group(group_id, msg_kv_op("delete", key));
    net_send(client_fd, resp);
}

void handle_update(int client_fd, const string &key, const string &value) {
    string group_id = key_to_group(key);
    if (group_id.empty()) { net_send(client_fd, msg_ack("ack", "no_nodes_available")); return; }

    string resp = send_to_group(group_id, msg_kv_op("update", key, value));
    Document doc;
    string s = resp;
    if (!doc.ParseInsitu((char*)s.c_str()).HasParseError()
        && doc.HasMember("message")
        && string(doc["message"].GetString()) == "update_success") {
        cache.update(key, value);
    }
    net_send(client_fd, resp);
}

void serve_client(int client_fd) {
    Document doc;
    while (true) {
        string msg = net_recv(client_fd);
        if (msg.empty()) return;

        string s = msg;
        if (doc.ParseInsitu((char*)s.c_str()).HasParseError()) {
            net_send(client_fd, msg_ack("ack", "parse_error"));
            continue;
        }

        string op = doc["req_type"].GetString();
        if      (op == "get")    handle_get   (client_fd, doc["key"].GetString());
        else if (op == "put")    handle_put   (client_fd, doc["key"].GetString(), doc["value"].GetString());
        else if (op == "delete") handle_delete(client_fd, doc["key"].GetString());
        else if (op == "update") handle_update(client_fd, doc["key"].GetString(), doc["value"].GetString());
    }
}

static bool should_cache_get_response(const string &op, const string &payload) {
  if (op != "get") return false;
  if (payload.empty()) return false;
  return payload != "key_error";
}

static bool should_cache_get_response(const string &op, const string &payload) {
  if (op != "get") return false;
  if (payload.empty()) return false;
  return payload != "key_error";
}

static bool is_supported_client_request(const string &op) {
    static const set<string> allowed = {"get", "put", "update", "delete"};
    return allowed.count(op) > 0;
}

static bool should_invalidate_cache(const string &op) {
    return op == "update" || op == "delete";
}

static bool is_success_ack(const Document &doc) {
    if (!doc.HasMember("message") || !doc["message"].IsString()) return false;
    string msg = doc["message"].GetString();
    return msg.find("success") != string::npos;
}

static bool is_supported_client_request(const string &op) {
    static const set<string> allowed = {"get", "put", "update", "delete"};
    return allowed.count(op) > 0;
}

static bool should_invalidate_cache(const string &op) {
    return op == "update" || op == "delete";
}

static bool should_cache_get_response(const string &op, const string &payload) {
    if (op != "get") return false;
    return !payload.empty() && payload != "key_error";
}

static string fallback_leader_for_group(const string &group_id) {
    if (!raft_groups.count(group_id) || raft_groups[group_id].empty()) return "";
    return raft_groups[group_id][0];
}
