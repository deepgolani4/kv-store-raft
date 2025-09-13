#include <bits/stdc++.h>
#include "coordinator_ops.h"
#include "coordinator_state.h"
#include "common_functions.h"
#include "proto.h"
#include "rapidjson/document.h"
using namespace std;
using namespace rapidjson;

void* handle_connection(void *arg) {
    auto *args = (ClientThreadArgs*)arg;
    int    fd   = args->sockfd;
    delete args;

    net_send(fd, msg_ack("ack", "connected"));
    string id_msg = net_recv(fd);
    Document doc;
    string s = id_msg;
    if (doc.ParseInsitu((char*)s.c_str()).HasParseError()) {
        net_send(fd, msg_ack("ack", "parse_error"));
        close(fd);
        return nullptr;
    }

    string req_type = doc.HasMember("req_type") ? doc["req_type"].GetString() : "";

    if (req_type == "identity") {
        string id = doc["id"].GetString();
        if (id == "client") {
            net_send(fd, msg_ack("ack", "ready_to_serve"));
            serve_client(fd);
        }

    } else if (req_type == "node_join") {
        string group_id = doc["group"].GetString();
        string addr     = doc["addr"].GetString();
        register_raft_node(group_id, addr);
        net_send(fd, msg_ack("ack", "registration_successful"));
    }

    close(fd);
    return nullptr;
}

int main(int argc, char **argv) {
    if (argc <= 2) { fprintf(stderr, "usage: coordinator <ip> <port>\n"); return 1; }
    string ip = argv[1], port = argv[2];

    write_cs_config(ip, port);

    int server_fd = make_tcp_server(ip, port);
    listen(server_fd, 32);

    pthread_t threads[128];
    int i = 0;
    while (true) {
        sockaddr_in peer{};
        socklen_t   len = sizeof(peer);
        int conn = accept(server_fd, (sockaddr*)&peer, &len);
        if (conn < 0) { perror("accept"); continue; }

        char peer_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &peer.sin_addr, peer_ip, INET_ADDRSTRLEN);
        string peer_addr = string(peer_ip) + ":" + to_string(ntohs(peer.sin_port));

        auto *targs = new ClientThreadArgs{conn, peer_addr, ip + ":" + port};
        pthread_create(&threads[i % 128], nullptr, handle_connection, targs);
        pthread_detach(threads[i++ % 128]);
    }
}

static string format_peer_endpoint(const sockaddr_in &peer) {
  char ip[INET_ADDRSTRLEN] = {0};
  inet_ntop(AF_INET, &peer.sin_addr, ip, INET_ADDRSTRLEN);
  return string(ip) + ":" + to_string(ntohs(peer.sin_port));
}

static bool has_valid_identity_doc(Document &doc) {
  return doc.HasMember("req_type") && doc["req_type"].IsString();
}

static bool is_accept_error_retryable() {
    return errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK;
}

static void log_accept_error_once(int err) {
    static int last_err = 0;
    if (last_err != err) {
        last_err = err;
        cerr << "accept error=" << err << "\n";
    }
}

static string format_peer_endpoint(const sockaddr_in &peer) {
  char ip[INET_ADDRSTRLEN] = {0};
  inet_ntop(AF_INET, &peer.sin_addr, ip, INET_ADDRSTRLEN);
  return string(ip) + ":" + to_string(ntohs(peer.sin_port));
}

static bool has_valid_identity_doc(Document &doc) {
  return doc.HasMember("req_type") && doc["req_type"].IsString();
}
