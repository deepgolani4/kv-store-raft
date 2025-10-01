#include <bits/stdc++.h>
#include "common_functions.h"
#include "proto.h"
#include "raft.h"
#include "rapidjson/document.h"
using namespace rapidjson;
using namespace std;

#define CS_CONFIG "cs_config.txt"

static RaftNode *g_node = nullptr;


string dispatch(const string &raw) {
    Document doc;
    string s = raw;
    if (doc.ParseInsitu((char*)s.c_str()).HasParseError())
        return msg_ack("ack", "parse_error");

    if (!doc.HasMember("rpc") && !doc.HasMember("role"))
        return msg_ack("ack", "parse_error");

    if (doc.HasMember("rpc")) {
        string rpc = doc["rpc"].GetString();
        if (rpc == "append_entries")  return g_node->on_append_entries(raw);
        if (rpc == "request_vote")    return g_node->on_request_vote(raw);
        if (rpc == "install_snapshot")return g_node->on_install_snapshot(raw);
    }

    if (doc.HasMember("role")) {
        string role = doc["role"].GetString();
        string key  = doc.HasMember("key")   ? doc["key"].GetString()   : "";
        string val  = doc.HasMember("value") ? doc["value"].GetString() : "";

        if (role == "get") {
            string result = g_node->state_machine.get(key);
            if (result.empty()) return msg_ack("ack", "key_error");
            return msg_ack("data", result);
        }

        if (g_node->role != Role::LEADER)
            return msg_redirect(g_node->current_leader);

        if (role == "put")    return "ASYNC";   // handled in serve_connection
        if (role == "update") return "ASYNC";
        if (role == "delete") return "ASYNC";
    }

    return msg_ack("ack", "unknown_role");
}

void serve_connection(int client_fd) {
    string raw = net_recv(client_fd);
    Document doc;
    string s = raw;
    if (doc.ParseInsitu((char*)s.c_str()).HasParseError()) {
        net_send(client_fd, msg_ack("ack", "parse_error"));
        close(client_fd);
        return;
    }

    string rpc  = doc.HasMember("rpc")  ? doc["rpc"].GetString()  : "";
    string role = doc.HasMember("role") ? doc["role"].GetString() : "";
    string key  = doc.HasMember("key")  ? doc["key"].GetString()  : "";
    string val  = doc.HasMember("value")? doc["value"].GetString(): "";

    if (!rpc.empty()) {
        lock_guard<mutex> lk(g_node->mu);
        net_send(client_fd, dispatch(raw));
        close(client_fd);
        return;
    }

    if (role == "get") {
        string result = g_node->state_machine.get(key);
        net_send(client_fd, result.empty() ? msg_ack("ack", "key_error")
                                           : msg_ack("data", result));
        close(client_fd);
        return;
    }

    // redirect if not leader
    {
        lock_guard<mutex> lk(g_node->mu);
        if (g_node->role != Role::LEADER) {
            net_send(client_fd, msg_redirect(g_node->current_leader));
            close(client_fd);
            return;
        }
    }

    OpType op = (role == "put")    ? OpType::PUT
              : (role == "update") ? OpType::UPDATE
              : (role == "delete") ? OpType::DELETE
                                   : OpType::NOOP;

    lock_guard<mutex> lk(g_node->mu);
    int idx = g_node->propose(op, key, val, client_fd);
    if (idx < 0) {
        net_send(client_fd, msg_redirect(g_node->current_leader));
        close(client_fd);
    }
}

void* accept_loop(void *arg) {
    auto *addr_pair = (pair<string,string>*)arg;
    int server_fd = make_tcp_server(addr_pair->first, addr_pair->second);
    listen(server_fd, 32);

    sockaddr_in peer{};
    socklen_t   peer_len = sizeof(peer);
    while (true) {
        int conn = accept(server_fd, (sockaddr*)&peer, &peer_len);
        if (conn < 0) continue;
        pthread_t t;
        int *fdp = new int(conn);
        pthread_create(&t, nullptr, [](void *p) -> void* {
            int fd = *(int*)p; delete (int*)p;
            serve_connection(fd);
            return nullptr;
        }, fdp);
        pthread_detach(t);
    }
    return nullptr;
}

void register_with_coordinator(const string &self_addr, const string &group_id) {
    string cs_ip, cs_port;
    ifstream f(CS_CONFIG);
    getline(f, cs_ip);
    getline(f, cs_port);

    int fd = make_tcp_client();
    tcp_connect(fd, cs_ip, cs_port);
    net_recv(fd);  
    net_send(fd, msg_node_join(group_id, self_addr));
    net_recv(fd); 
    close(fd);
}


int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: raft_node <ip:port> <peer1> [peer2 ...]\n");
        return 1;
    }

    string self = argv[1];
    vector<string> peers;
    for (int i = 2; i < argc; i++) peers.push_back(argv[i]);

    g_node = new RaftNode(self, peers);

    auto [ip, port] = split_addr(self);
    auto *addr_pair = new pair<string,string>(ip, port);

    pthread_t accept_t, elect_t, hbeat_t;
    pthread_create(&accept_t, nullptr, accept_loop,      addr_pair);
    pthread_create(&elect_t,  nullptr, election_timer,   g_node);
    pthread_create(&hbeat_t,  nullptr, leader_heartbeat, g_node);

    string group_id = to_string(ring_hash(self));
    register_with_coordinator(self, group_id);

    pthread_join(accept_t, nullptr);
    pthread_join(elect_t,  nullptr);
    pthread_join(hbeat_t,  nullptr);
}

static bool is_mutation_role(const string &role) {
    return role == "put" || role == "update" || role == "delete";
}

static bool is_rpc_name(const string &rpc) {
    return rpc == "append_entries" || rpc == "request_vote" || rpc == "install_snapshot";
}

static bool should_redirect_request(RaftNode *node) {
    return node && node->role != Role::LEADER;
}

static OpType role_to_op(const string &role) {
    if (role == "put") return OpType::PUT;
    if (role == "update") return OpType::UPDATE;
    if (role == "delete") return OpType::DELETE;
    return OpType::NOOP;
}

static bool is_mutation_role(const string &role) {
    return role == "put" || role == "update" || role == "delete";
}

static bool is_rpc_name(const string &rpc) {
    return rpc == "append_entries" || rpc == "request_vote" || rpc == "install_snapshot";
}
