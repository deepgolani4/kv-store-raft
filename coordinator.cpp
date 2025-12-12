#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <pthread.h>

#include "common_functions.h"
#include "coordinator_ops.h"
#include "coordinator_state.h"
#include "proto.h"
#include "rapidjson/document.h"

using namespace rapidjson;
using namespace std;

void *handle_connection(void *arg) {
  auto *args = (ClientThreadArgs *)arg;
  int fd = args->sockfd;
  delete args;

  net_send(fd, msg_ack("ack", "connected"));
  string id_msg = net_recv(fd);

  Document doc;
  string s = id_msg;
  if (doc.ParseInsitu((char *)s.data()).HasParseError()) {
    net_send(fd, msg_ack("ack", "parse_error"));
    close(fd);
    return nullptr;
  }

  string req_type = doc.HasMember("req_type") && doc["req_type"].IsString() ? doc["req_type"].GetString() : "";

  if (req_type == "identity") {
    string id = doc.HasMember("id") && doc["id"].IsString() ? doc["id"].GetString() : "";
    if (id == "client") {
      net_send(fd, msg_ack("ack", "ready_to_serve"));
      serve_client(fd);
    } else {
      net_send(fd, msg_ack("ack", "unknown_identity"));
    }
  } else if (req_type == "node_join") {
    string group_id = doc.HasMember("group") && doc["group"].IsString() ? doc["group"].GetString() : "";
    string addr = doc.HasMember("addr") && doc["addr"].IsString() ? doc["addr"].GetString() : "";
    if (group_id.empty() || addr.empty()) {
      net_send(fd, msg_ack("ack", "invalid_node_join"));
    } else {
      register_raft_node(group_id, addr);
      net_send(fd, msg_ack("ack", "registration_successful"));
    }
  } else {
    net_send(fd, msg_ack("ack", "unknown_req_type"));
  }

  close(fd);
  return nullptr;
}

int main(int argc, char **argv) {
  if (argc <= 2) {
    fprintf(stderr, "usage: coordinator <ip> <port>\n");
    return 1;
  }
  string ip = argv[1], port = argv[2];

  write_cs_config(ip, port);

  int server_fd = make_tcp_server(ip, port);
  listen(server_fd, 32);

  pthread_t threads[128];
  int i = 0;
  while (true) {
    sockaddr_in peer{};
    socklen_t len = sizeof(peer);
    int conn = accept(server_fd, (sockaddr *)&peer, &len);
    if (conn < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
      perror("accept");
      continue;
    }

    char peer_ip[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &peer.sin_addr, peer_ip, INET_ADDRSTRLEN);
    string peer_addr = string(peer_ip) + ":" + to_string(ntohs(peer.sin_port));

    auto *targs = new ClientThreadArgs{conn, peer_addr, ip + ":" + port};
    pthread_create(&threads[i % 128], nullptr, handle_connection, targs);
    pthread_detach(threads[i++ % 128]);
  }
}
