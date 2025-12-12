#include <fstream>
#include <mutex>
#include <pthread.h>
#include <cstdio>
#include <string>
#include <unordered_map>

#include "common_functions.h"
#include "proto.h"
#include "rapidjson/document.h"

using namespace rapidjson;
using namespace std;

#define CS_CONFIG "cs_config.txt"

static unordered_map<string, string> kv_store;
static mutex kv_mu;

static string op_get(const string &key) {
  lock_guard<mutex> lk(kv_mu);
  auto it = kv_store.find(key);
  if (it == kv_store.end()) return "key_error";
  return it->second;
}

static void op_put(const string &key, const string &value) {
  lock_guard<mutex> lk(kv_mu);
  kv_store[key] = value;
}

static string op_update(const string &key, const string &value) {
  lock_guard<mutex> lk(kv_mu);
  auto it = kv_store.find(key);
  if (it == kv_store.end()) return "key_error";
  it->second = value;
  return "update_success";
}

static string op_delete(const string &key) {
  lock_guard<mutex> lk(kv_mu);
  auto it = kv_store.find(key);
  if (it == kv_store.end()) return "key_error";
  kv_store.erase(it);
  return "delete_success";
}

static void handle_client_request(int client_fd) {
  string raw = net_recv(client_fd);
  if (raw.empty()) {
    close(client_fd);
    return;
  }

  Document doc;
  string s = raw;
  if (doc.ParseInsitu((char *)s.data()).HasParseError()) {
    net_send(client_fd, msg_ack("ack", "parse_error"));
    close(client_fd);
    return;
  }

  if (!doc.HasMember("role") || !doc["role"].IsString()) {
    net_send(client_fd, msg_ack("ack", "invalid_request"));
    close(client_fd);
    return;
  }

  string role = doc["role"].GetString();
  string key = doc.HasMember("key") && doc["key"].IsString() ? doc["key"].GetString() : "";
  string value = doc.HasMember("value") && doc["value"].IsString() ? doc["value"].GetString() : "";

  if (role == "get") {
    if (key.empty()) {
      net_send(client_fd, msg_ack("ack", "missing_key"));
    } else {
      string result = op_get(key);
      net_send(client_fd, result == "key_error" ? msg_ack("ack", result) : msg_ack("data", result));
    }
  } else if (role == "put") {
    if (key.empty()) {
      net_send(client_fd, msg_ack("ack", "missing_key"));
    } else {
      op_put(key, value);
      net_send(client_fd, msg_ack("ack", "put_success"));
    }
  } else if (role == "update") {
    if (key.empty()) {
      net_send(client_fd, msg_ack("ack", "missing_key"));
    } else {
      net_send(client_fd, msg_ack("ack", op_update(key, value)));
    }
  } else if (role == "delete") {
    if (key.empty()) {
      net_send(client_fd, msg_ack("ack", "missing_key"));
    } else {
      net_send(client_fd, msg_ack("ack", op_delete(key)));
    }
  } else {
    net_send(client_fd, msg_ack("ack", "unsupported_role"));
  }

  close(client_fd);
}

static void *serve_requests(void *ptr) {
  auto *td = (pair<string, string> *)ptr;
  string ip = td->first;
  string port = td->second;

  int server_fd = make_tcp_server(ip, port);
  listen(server_fd, 32);

  while (true) {
    sockaddr_in peer{};
    socklen_t peer_len = sizeof(peer);
    int client_fd = accept(server_fd, (sockaddr *)&peer, &peer_len);
    if (client_fd < 0) continue;
    handle_client_request(client_fd);
  }
  return nullptr;
}

static void register_with_coordinator(const string &self_addr) {
  string cs_ip, cs_port;
  ifstream f(CS_CONFIG);
  getline(f, cs_ip);
  getline(f, cs_port);
  if (cs_ip.empty() || cs_port.empty()) {
    fprintf(stderr, "cs_config.txt missing. start coordinator first.\n");
    exit(1);
  }

  string group_id = to_string(ring_hash(self_addr));

  int fd = make_tcp_client();
  tcp_connect(fd, cs_ip, cs_port);
  net_recv(fd);
  net_send(fd, msg_node_join(group_id, self_addr));
  net_recv(fd);
  close(fd);
}

int main(int argc, char **argv) {
  if (argc <= 2) {
    fprintf(stderr, "usage: raft_node <ip> <port>\n");
    return 1;
  }

  string ip = argv[1];
  string port = argv[2];
  string self_addr = ip + ":" + port;

  register_with_coordinator(self_addr);

  auto *td = new pair<string, string>(ip, port);
  pthread_t srv;
  pthread_create(&srv, nullptr, serve_requests, td);
  pthread_join(srv, nullptr);
  return 0;
}
