#include "common_functions.h"
#include "proto.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include <bits/stdc++.h>
#include <semaphore.h>
using namespace rapidjson;
using namespace std;

#define CS_CONFIG "cs_config.txt"

map<string, string> own_table, prev_table;
int active_readers = 0, writer_slot = 1;
mutex rw_mutex;

Document rdoc, rdoc1, rdoc2;

map<string, string> &table_ref(const string &t) {
  return t == "prev" ? prev_table : own_table;
}

bool has_key(const string &t, const string &k) { return table_ref(t).count(k); }

string op_get(const string &table, const string &key) {
  if (!has_key(table, key))
    return "key_error";
  return table_ref(table)[key];
}

void op_put(const string &table, const string &key, const string &value) {
  table_ref(table)[key] = value;
}

string op_update(const string &table, const string &key, const string &value) {
  if (!has_key(table, key))
    return "key_error";
  table_ref(table)[key] = value;
  return "update_success";
}

string op_delete(const string &table, const string &key) {
  if (!has_key(table, key))
    return "key_error";
  table_ref(table).erase(key);
  return "delete_success";
}

void promote_prev_to_own() {
  for (auto &kv : prev_table)
    own_table[kv.first] = kv.second;
  prev_table.clear();
}

void send_table(int fd, const string &table) {
  string out;
  for (auto &kv : table_ref(table)) {
    if (!out.empty())
      out += "|";
    out += kv.first + ":" + kv.second;
  }
  net_send(fd, out);
}

void recv_table(int fd, const string &table) {
  string raw = net_recv(fd);
  if (raw.empty())
    return;
  table_ref(table).clear();
  stringstream ss(raw);
  string tok;
  while (getline(ss, tok, '|')) {
    size_t p = tok.find(':');
    if (p == string::npos)
      continue;
    table_ref(table)[tok.substr(0, p)] = tok.substr(p + 1);
  }
}

int connect_to(const string &ip, const string &port, const string &role,
               const string &table, const string &pre = "",
               const string &self = "", const string &succ2 = "") {
  int fd = make_tcp_client();
  tcp_connect(fd, ip, port);
  if (pre.empty())
    net_send(fd, msg_table_update(role, table));
  else
    net_send(fd, msg_migration_directive(role, pre, self, succ2));
  return fd;
}

void rebalance_own(int lo, int hi) {
  prev_table.clear();
  vector<string> to_remove;
  for (auto &kv : own_table) {
    int h = ring_hash(kv.first);
    bool in_range = (lo < hi)
                        ? (lo <= h && h < hi)
                        : ((lo <= h && h < RING_SIZE) || (0 <= h && h < hi));
    if (in_range) {
      prev_table[kv.first] = kv.second;
      to_remove.push_back(kv.first);
    }
  }
  for (auto &k : to_remove)
    own_table.erase(k);
}

void handle_as_new_node(const string &ss_ip, const string &ss_port,
                        int coord_fd) {
  assert(rdoc2.HasMember("pre_ip") && rdoc2.HasMember("succ_ip") &&
         rdoc2.HasMember("succ_of_succ_ip"));
  auto [pre_ip, pre_port] = split_addr(rdoc2["pre_ip"].GetString());
  auto [succ_ip, succ_port] = split_addr(rdoc2["succ_ip"].GetString());
  auto [succ2_ip, succ2_port] =
      split_addr(rdoc2["succ_of_succ_ip"].GetString());

  string pre_addr = string(rdoc2["pre_ip"].GetString());
  string self_addr = ss_ip + ":" + ss_port;
  string succ2_addr = string(rdoc2["succ_of_succ_ip"].GetString());

  int fd = connect_to(succ_ip, succ_port, "new_ss_succ", "prev", pre_addr,
                      self_addr, succ2_addr);
  recv_table(fd, "own");
  close(fd);

  fd = connect_to(pre_ip, pre_port, "new_ss_pre", "own");
  recv_table(fd, "prev");
  close(fd);
}

void handle_as_leader(const string &ss_ip, const string &ss_port,
                      int coord_fd) {
  assert(rdoc.HasMember("pre_ip") && rdoc.HasMember("succ_of_succ_ip"));
  auto [pre_ip, pre_port] = split_addr(rdoc["pre_ip"].GetString());
  auto [succ2_ip, succ2_port] = split_addr(rdoc["succ_of_succ_ip"].GetString());

  promote_prev_to_own();

  int fd = connect_to(pre_ip, pre_port, "pre", "own");
  recv_table(fd, "prev");
  close(fd);

  fd = connect_to(succ2_ip, succ2_port, "succ_of_succ", "prev");
  net_recv(fd);
  send_table(fd, "own");
  close(fd);

  net_send(coord_fd, msg_ready("migration_completed"));
}

void handle_as_successor(const string &self_ip, const string &self_port) {
  assert(rdoc.HasMember("pre_ip") && rdoc.HasMember("succ_ip") &&
         rdoc.HasMember("succ_of_succ_ip"));
  string pre_addr = rdoc["pre_ip"].GetString();
  string new_ss_addr = rdoc["succ_ip"].GetString();
  auto [succ2_ip, succ2_port] = split_addr(rdoc["succ_of_succ_ip"].GetString());

  int lo = ring_hash(pre_addr), hi = ring_hash(new_ss_addr);
  rebalance_own(lo, hi);

  if (self_ip == succ2_ip && self_port == succ2_port)
    return;

  int fd = connect_to(succ2_ip, succ2_port, "new_ss_succ_of_succ", "prev");
  net_recv(fd);
  send_table(fd, "own");
  net_recv(fd);
  close(fd);
}

void *serve_requests(void *ptr) {
  auto *td = (pair<string, string> *)ptr;
  string ip = td->first, port = td->second;

  int server_fd = make_tcp_server(ip, port);
  listen(server_fd, 10);

  sockaddr_in peer{};
  socklen_t peer_len = sizeof(peer);
  int client_fd;

  while ((client_fd = accept(server_fd, (sockaddr *)&peer, &peer_len)) > 0) {
    string raw = net_recv(client_fd);
    if (rdoc.ParseInsitu((char *)raw.c_str()).HasParseError()) {
      net_send(client_fd, msg_ack("ack", "parse_error"));
      continue;
    }
    string role = rdoc["role"].GetString();

    if (role == "get") {
      int flag = 0;
      if (writer_slot == 0) {
        rw_mutex.lock();
        flag = 1;
      }
      active_readers++;
      string val = op_get(rdoc["table"].GetString(), rdoc["key"].GetString());
      net_send(client_fd,
               val == "key_error" ? msg_ack("ack", val) : msg_ack("data", val));
      active_readers--;
      if (flag)
        rw_mutex.unlock();

    } else if (role == "put") {
      rw_mutex.lock();
      while (active_readers)
        ;
      writer_slot--;
      op_put(rdoc["table"].GetString(), rdoc["key"].GetString(),
             rdoc["value"].GetString());
      writer_slot++;
      rw_mutex.unlock();
      net_send(client_fd, msg_ack("ack", "put_success"));

    } else if (role == "update") {
      rw_mutex.lock();
      while (active_readers)
        ;
      writer_slot--;
      string val = op_update(rdoc["table"].GetString(), rdoc["key"].GetString(),
                             rdoc["value"].GetString());
      net_send(client_fd, msg_ack("ack", val));
      writer_slot++;
      rw_mutex.unlock();

    } else if (role == "delete") {
      rw_mutex.lock();
      while (active_readers)
        ;
      writer_slot--;
      string val =
          op_delete(rdoc["table"].GetString(), rdoc["key"].GetString());
      net_send(client_fd, msg_ack("ack", val));
      writer_slot++;
      rw_mutex.unlock();

    } else if (role == "leader") {
      handle_as_leader(ip, port, client_fd);
    } else if (role == "pre") {
      send_table(client_fd, rdoc["table"].GetString());
    } else if (role == "succ_of_succ") {
      net_send(client_fd, msg_ready("ready_for_table"));
      recv_table(client_fd, rdoc["table"].GetString());
    } else if (role == "new_ss_pre") {
      send_table(client_fd, "own");
    } else if (role == "new_ss_succ") {
      handle_as_successor(ip, port);
      send_table(client_fd, "prev");
    } else if (role == "new_ss_succ_of_succ") {
      net_send(client_fd, msg_ready("ready_for_table"));
      recv_table(client_fd, "prev");
      net_send(client_fd, msg_ack("ack", "new_ss_succ_of_succ_done"));
    }
    close(client_fd);
  }
  return nullptr;
}

void *heartbeat_thread(void *ptr) {
  auto *td = (pair<string, string> *)ptr;
  string ss_ip = td->first, ss_port = td->second;

  string cs_ip, cs_port;
  ifstream f(CS_CONFIG);
  getline(f, cs_ip);
  getline(f, cs_port);

  int fd = make_tcp_server(ss_ip, ss_port);
  tcp_connect(fd, cs_ip, cs_port);

  net_recv(fd);
  net_send(fd, msg_identity("slave_server"));
  string resp = net_recv(fd);

  if (rdoc1.ParseInsitu((char *)resp.c_str()).HasParseError()) {
    net_send(fd, msg_ack("ack", "parse_error"));
    pthread_exit(nullptr);
  }

  string msg = rdoc1["message"].GetString();
  if (msg == "migration_new_server") {
    net_send(fd, msg_ack("ack", "ready_for_migration"));
    string directive = net_recv(fd);
    if (rdoc2.ParseInsitu((char *)directive.c_str()).HasParseError()) {
      net_send(fd, msg_ack("ack", "parse_error"));
      pthread_exit(nullptr);
    }
    string role = rdoc2["role"].GetString();
    if (role == "new_ss_leader") {
      handle_as_new_node(ss_ip, ss_port, fd);
      net_send(fd, msg_ack("ack", "migration_ss_done"));
    }
  }
  close(fd);

  while (true) {
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = inet_addr(cs_ip.c_str());
    sa.sin_port = htons(UDP_PORT);
    int ufd = socket(AF_INET, SOCK_DGRAM, 0);
    connect(ufd, (sockaddr *)&sa, sizeof(sa));
    net_send(ufd, msg_ack("heartbeat", ss_ip + ":" + ss_port));
    close(ufd);
    sleep(4);
  }
  return nullptr;
}

int main(int argc, char **argv) {
  if (argc <= 2) {
    fprintf(stderr, "usage: node <ip> <port>\n");
    return 1;
  }
  auto *td = new pair<string, string>(argv[1], argv[2]);
  pthread_t hb, srv;
  pthread_create(&hb, nullptr, heartbeat_thread, td);
  pthread_create(&srv, nullptr, serve_requests, td);
  pthread_join(hb, nullptr);
  pthread_join(srv, nullptr);
}

static bool is_known_node_role(const string &role) {
  static const set<string> roles = {
      "get", "put", "update", "delete", "leader", "pre",
      "succ_of_succ", "new_ss_pre", "new_ss_succ", "new_ss_succ_of_succ"};
  return roles.count(role) > 0;
}

static string doc_member_or_empty(const Document &doc, const char *key) {
  if (!doc.HasMember(key) || !doc[key].IsString()) return "";
  return doc[key].GetString();
}

static bool is_blank(const string &s) {
  for (char c : s) if (!isspace(static_cast<unsigned char>(c))) return false;
  return true;
}

static void erase_blank_keys(map<string, string> &table) {
  vector<string> garbage;
  for (const auto &kv : table) {
    if (kv.first.empty() || is_blank(kv.first)) garbage.push_back(kv.first);
  }
  for (const string &k : garbage) table.erase(k);
}

static vector<string> split_tokens(const string &raw, char delim) {
  vector<string> out;
  string cur;
  for (char c : raw) {
    if (c == delim) {
      out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  out.push_back(cur);
  return out;
}

static string join_entries(const vector<string> &parts, char delim) {
  string out;
  for (size_t i = 0; i < parts.size(); i++) {
    if (i) out.push_back(delim);
    out += parts[i];
  }
  return out;
}

static bool is_blank(const string &s) {
  for (char c : s) if (!isspace(static_cast<unsigned char>(c))) return false;
  return true;
}

static void erase_blank_keys(map<string, string> &table) {
  vector<string> garbage;
  for (const auto &kv : table) {
    if (kv.first.empty() || is_blank(kv.first)) garbage.push_back(kv.first);
  }
  for (const string &k : garbage) table.erase(k);
}

static bool is_known_node_role(const string &role) {
  static const set<string> roles = {
      "get", "put", "update", "delete", "leader", "pre",
      "succ_of_succ", "new_ss_pre", "new_ss_succ", "new_ss_succ_of_succ"};
  return roles.count(role) > 0;
}

static vector<string> split_tokens(const string &raw, char delim) {
  vector<string> out;
  string cur;
  for (char c : raw) {
    if (c == delim) {
      out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  out.push_back(cur);
  return out;
}
