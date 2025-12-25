#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common_functions.h"

using namespace std;

#define CS_CONFIG "cs_config.txt"

enum class NodeRole { FOLLOWER, CANDIDATE, LEADER };

struct Peer {
  string data_addr;
  string election_addr;
};

class SimpleRaftNode {
public:
  SimpleRaftNode(string ip, string data_port, vector<string> peer_data_addrs)
      : ip_(std::move(ip)), data_port_(std::move(data_port)) {
    self_data_addr_ = ip_ + ":" + data_port_;
    self_election_addr_ = ip_ + ":" + to_string(stoi(data_port_) + 1000);

    for (const string &p : peer_data_addrs) {
      if (p == self_data_addr_) continue;
      auto [peer_ip, peer_port] = split_addr(p);
      peers_.push_back(Peer{p, peer_ip + ":" + to_string(stoi(peer_port) + 1000)});
    }

    wal_path_ = "wal_" + data_port_ + ".log";
  }

  bool start() {
    bool announce_boot_leader = false;
    {
      lock_guard<mutex> lk(mu_);
      if (peers_.empty()) {
        role_ = NodeRole::LEADER;
        leader_data_addr_ = self_data_addr_;
        leader_election_addr_ = self_election_addr_;
        announce_boot_leader = true;
      }
    }

    crud_server_fd_ = make_tcp_server(ip_, data_port_);
    election_server_fd_ = make_tcp_server(ip_, to_string(stoi(data_port_) + 1000));
    listen(crud_server_fd_, 64);
    listen(election_server_fd_, 64);

    if (!register_with_coordinator()) return false;
    crud_thread_ = thread(&SimpleRaftNode::crud_accept_loop, this);
    election_thread_ = thread(&SimpleRaftNode::election_accept_loop, this);
    timer_thread_ = thread(&SimpleRaftNode::follower_watch_loop, this);
    heartbeat_thread_ = thread(&SimpleRaftNode::leader_heartbeat_loop, this);

    if (announce_boot_leader) announce_leader_to_coordinator();
    sync_from_leader_on_startup();
    return true;
  }

  void wait_forever() {
    crud_thread_.join();
    election_thread_.join();
    timer_thread_.join();
    heartbeat_thread_.join();
  }

private:
  string ip_;
  string data_port_;
  string self_data_addr_;
  string self_election_addr_;

  vector<Peer> peers_;
  unordered_map<string, string> kv_;

  NodeRole role_ = NodeRole::FOLLOWER;
  int current_term_ = 0;
  string leader_data_addr_;
  string leader_election_addr_;

  string wal_path_;

  int crud_server_fd_ = -1;
  int election_server_fd_ = -1;

  mutex mu_;
  atomic<bool> running_{true};
  thread crud_thread_;
  thread election_thread_;
  thread timer_thread_;
  thread heartbeat_thread_;

private:
  int majority_size() const {
    int n = static_cast<int>(peers_.size()) + 1;
    return n / 2 + 1;
  }

  void apply_local_locked(const string &op, const string &key, const string &value) {
    if (op == "delete") {
      kv_.erase(key);
      return;
    }
    kv_[key] = value;
  }

  bool append_wal(const string &op, const string &key, const string &value) {
    int fd = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) return false;

    string line = op + "|" + key + "|" + value + "\n";
    size_t sent = 0;
    while (sent < line.size()) {
      ssize_t n = ::write(fd, line.data() + sent, line.size() - sent);
      if (n < 0) {
        if (errno == EINTR) continue;
        close(fd);
        return false;
      }
      sent += static_cast<size_t>(n);
    }

    while (true) {
      if (::fsync(fd) == 0) break;
      if (errno != EINTR) {
        close(fd);
        return false;
      }
    }

    close(fd);
    return true;
  }

  bool send_rpc(const string &target_addr, const string &req, string &resp) {
    auto [ip, port] = split_addr(target_addr);
    int fd = make_tcp_client();
    if (!tcp_connect(fd, ip, port)) {
      close(fd);
      return false;
    }
    if (!send_line(fd, req)) {
      close(fd);
      return false;
    }
    resp = recv_line_or_empty(fd);
    close(fd);
    return !resp.empty();
  }

  void announce_leader_to_coordinator() {
    string cs_ip, cs_port;
    ifstream f(CS_CONFIG);
    getline(f, cs_ip);
    getline(f, cs_port);

    int fd = make_tcp_client();
    if (!tcp_connect(fd, cs_ip, cs_port)) {
      close(fd);
      return;
    }

    send_line(fd, join_fields({"leader_announce", self_data_addr_}, '|'));
    recv_line_or_empty(fd);
    close(fd);
  }

  string leader_addr_from_coordinator() {
    string cs_ip, cs_port;
    ifstream f(CS_CONFIG);
    getline(f, cs_ip);
    getline(f, cs_port);

    int fd = make_tcp_client();
    if (!tcp_connect(fd, cs_ip, cs_port)) {
      close(fd);
      return "";
    }

    send_line(fd, "leader_sync");
    string resp = recv_line_or_empty(fd);
    close(fd);

    if (resp.rfind("leader|", 0) != 0) return "";
    return resp.substr(7);
  }

  void sync_from_leader_on_startup() {
    if (peers_.empty()) return;

    string leader = leader_addr_from_coordinator();
    if (leader.empty() || leader == self_data_addr_) return;

    string resp;
    if (!send_rpc(leader, "sync_all", resp)) return;

    vector<string> parts = split_string(resp, '|');

    lock_guard<mutex> lk(mu_);
    kv_.clear();
    for (size_t i = 1; i + 1 < parts.size(); i += 2) {
      kv_[parts[i]] = parts[i + 1];
    }
  }

  string handle_get(const string &key) {
    lock_guard<mutex> lk(mu_);
    auto it = kv_.find(key);
    if (it == kv_.end()) return "err:key_error";
    return "value:" + it->second;
  }

  string handle_write(const string &op, const string &key, const string &value, const string &ok_msg) {
    int term = 0;
    vector<Peer> peers;
    {
      lock_guard<mutex> lk(mu_);
      term = current_term_;
      peers = peers_;
    }

    string req = join_fields({"append", to_string(term), self_election_addr_, self_data_addr_, "1", op, key, value},
                             '|');

    int acks = 1;
    for (const auto &peer : peers) {
      string resp;
      if (!send_rpc(peer.election_addr, req, resp)) continue;
      vector<string> parts = split_string(resp, '|');

      int remote_term = stoi(parts[1]);
      if (remote_term > term) {
        lock_guard<mutex> lk(mu_);
        current_term_ = remote_term;
        role_ = NodeRole::FOLLOWER;
        return "err:not_leader";
      }

      if (parts[2] == "1") acks++;
    }

    if (acks < majority_size()) return "err:commit_failed";

    if (!append_wal(op, key, value)) return "err:wal_failed";
    {
      lock_guard<mutex> lk(mu_);
      apply_local_locked(op, key, value);
    }
    return ok_msg;
  }

  string handle_sync_all() {
    vector<string> fields;
    fields.push_back("sync");

    lock_guard<mutex> lk(mu_);
    for (const auto &kv : kv_) {
      fields.push_back(kv.first);
      fields.push_back(kv.second);
    }
    return join_fields(fields, '|');
  }

  string dispatch_crud(const string &req) {
    vector<string> parts = split_string(req, ':');
    string op = parts[0];

    if (op == "get") return handle_get(parts[1]);
    if (op == "delete") return handle_write("delete", parts[1], "", "ok:delete_success");
    if (op == "sync_all") return handle_sync_all();

    string value = parts[2];
    for (size_t i = 3; i < parts.size(); i++) value += ":" + parts[i];
    string ok = (op == "put") ? "ok:put_success" : (op == "update") ? "ok:update_success" : "ok:done";
    return handle_write(op, parts[1], value, ok);
  }

  string rpc_who_leader() {
    lock_guard<mutex> lk(mu_);
    if (role_ == NodeRole::LEADER) return join_fields({"leader", self_data_addr_}, '|');
    return join_fields({"leader", leader_data_addr_}, '|');
  }

  string rpc_request_vote(const vector<string> &parts) {
    int term = stoi(parts[1]);

    lock_guard<mutex> lk(mu_);
    if (term < current_term_) return join_fields({"vote_reply", to_string(current_term_), "0"}, '|');

    if (term > current_term_) {
      current_term_ = term;
      role_ = NodeRole::FOLLOWER;
    }
    return join_fields({"vote_reply", to_string(current_term_), "1"}, '|');
  }

  string rpc_ping(const vector<string> &parts) {
    int term = stoi(parts[1]);
    string leader_election = parts[2];
    string leader_data = parts[3];

    lock_guard<mutex> lk(mu_);
    if (term < current_term_) return join_fields({"pong", to_string(current_term_), "0"}, '|');

    if (term > current_term_) {
      current_term_ = term;
    }
    role_ = NodeRole::FOLLOWER;
    if (leader_data_addr_ != leader_data || leader_election_addr_ != leader_election) {
      leader_data_addr_ = leader_data;
      leader_election_addr_ = leader_election;
    }
    return join_fields({"pong", to_string(current_term_), "1"}, '|');
  }

  string rpc_append(const vector<string> &parts) {
    int term = stoi(parts[1]);
    string leader_election = parts[2];
    string leader_data = parts[3];
    int has_entry = stoi(parts[4]);

    string op;
    string key;
    string value;
    if (has_entry == 1) {
      op = parts[5];
      key = parts[6];
      value = parts[7];
    }

    lock_guard<mutex> lk(mu_);
    if (term < current_term_) {
      return join_fields({"append_reply", to_string(current_term_), "0"}, '|');
    }

    if (term > current_term_) {
      current_term_ = term;
    }

    role_ = NodeRole::FOLLOWER;
    if (leader_data_addr_ != leader_data || leader_election_addr_ != leader_election) {
      leader_data_addr_ = leader_data;
      leader_election_addr_ = leader_election;
    }

    if (has_entry == 1) apply_local_locked(op, key, value);

    return join_fields({"append_reply", to_string(current_term_), "1"}, '|');
  }

  string dispatch_election(const string &req) {
    vector<string> parts = split_string(req, '|');
    string rpc = parts[0];

    if (rpc == "who_leader") return rpc_who_leader();
    if (rpc == "ping") return rpc_ping(parts);
    if (rpc == "request_vote") return rpc_request_vote(parts);
    if (rpc == "append") return rpc_append(parts);
    return "err:unknown_rpc";
  }

  void handle_crud_connection(int fd) {
    string req = recv_line_or_empty(fd);
    if (req.empty()) {
      close(fd);
      return;
    }
    send_line(fd, dispatch_crud(req));
    close(fd);
  }

  void handle_election_connection(int fd) {
    string req = recv_line_or_empty(fd);
    if (req.empty()) {
      close(fd);
      return;
    }
    send_line(fd, dispatch_election(req));
    close(fd);
  }

  void crud_accept_loop() {
    while (running_) {
      sockaddr_in peer{};
      socklen_t len = sizeof(peer);
      int conn = accept(crud_server_fd_, (sockaddr *)&peer, &len);
      if (conn < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
        continue;
      }
      thread(&SimpleRaftNode::handle_crud_connection, this, conn).detach();
    }
  }

  void election_accept_loop() {
    while (running_) {
      sockaddr_in peer{};
      socklen_t len = sizeof(peer);
      int conn = accept(election_server_fd_, (sockaddr *)&peer, &len);
      if (conn < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
        continue;
      }
      thread(&SimpleRaftNode::handle_election_connection, this, conn).detach();
    }
  }

  void start_election() {
    int term = 0;
    vector<Peer> peers;
    bool became_leader = false;

    {
      lock_guard<mutex> lk(mu_);
      role_ = NodeRole::CANDIDATE;
      current_term_++;
      term = current_term_;
      leader_data_addr_.clear();
      leader_election_addr_.clear();
      peers = peers_;
    }

    int votes = 1;
    string req = join_fields({"request_vote", to_string(term), self_election_addr_}, '|');

    for (const auto &peer : peers) {
      string resp;
      if (!send_rpc(peer.election_addr, req, resp)) continue;
      vector<string> parts = split_string(resp, '|');

      int remote_term = stoi(parts[1]);
      if (remote_term > term) {
        lock_guard<mutex> lk(mu_);
        current_term_ = remote_term;
        role_ = NodeRole::FOLLOWER;
        return;
      }

      if (parts[2] == "1") votes++;
    }

    if (votes >= majority_size()) {
      lock_guard<mutex> lk(mu_);
      role_ = NodeRole::LEADER;
      leader_data_addr_ = self_data_addr_;
      leader_election_addr_ = self_election_addr_;
      became_leader = true;
    }
    if (became_leader) announce_leader_to_coordinator();
  }

  void follower_watch_loop() {
    while (running_) {
      this_thread::sleep_for(chrono::milliseconds(250));

      NodeRole role;
      int term;
      string leader_election;
      bool no_peers = false;
      bool announce_boot_leader = false;
      {
        lock_guard<mutex> lk(mu_);
        if (peers_.empty()) {
          no_peers = true;
          if (role_ != NodeRole::LEADER) {
            role_ = NodeRole::LEADER;
            leader_data_addr_ = self_data_addr_;
            leader_election_addr_ = self_election_addr_;
            announce_boot_leader = true;
          }
        } else {
          role = role_;
          term = current_term_;
          leader_election = leader_election_addr_;
        }
      }
      if (announce_boot_leader) announce_leader_to_coordinator();
      if (no_peers || role == NodeRole::LEADER) continue;
      if (leader_election.empty()) {
        start_election();
        continue;
      }

      string resp;
      string req = join_fields({"ping", to_string(term), self_election_addr_, self_data_addr_}, '|');
      if (!send_rpc(leader_election, req, resp)) {
        start_election();
        continue;
      }

      vector<string> parts = split_string(resp, '|');
      int remote_term = stoi(parts[1]);
      if (remote_term > term) {
        lock_guard<mutex> lk(mu_);
        current_term_ = remote_term;
        role_ = NodeRole::FOLLOWER;
        continue;
      }
      if (parts[2] != "1") start_election();
    }
  }

  void leader_heartbeat_loop() {
    while (running_) {
      this_thread::sleep_for(chrono::milliseconds(180));

      int term = 0;
      vector<Peer> peers;
      {
        lock_guard<mutex> lk(mu_);
        if (role_ != NodeRole::LEADER) continue;
        term = current_term_;
        peers = peers_;
      }

      string req = join_fields({"append", to_string(term), self_election_addr_, self_data_addr_, "0", "", "", ""},
                               '|');

      for (const auto &peer : peers) {
        string resp;
        if (!send_rpc(peer.election_addr, req, resp)) continue;
        vector<string> parts = split_string(resp, '|');
        int remote_term = stoi(parts[1]);
        if (remote_term > term) {
          lock_guard<mutex> lk(mu_);
          current_term_ = remote_term;
          role_ = NodeRole::FOLLOWER;
        }
      }
    }
  }

  bool register_with_coordinator() {
    string cs_ip, cs_port;
    ifstream f(CS_CONFIG);
    getline(f, cs_ip);
    getline(f, cs_port);

    int fd = make_tcp_client();
    if (!tcp_connect(fd, cs_ip, cs_port)) {
      close(fd);
      return false;
    }

    string req = join_fields({"node_join", self_data_addr_, self_election_addr_}, '|');
    bool ok = send_line(fd, req);
    string resp = recv_line_or_empty(fd);
    close(fd);
    return ok && resp == "ok:node_registered";
  }
};

int main(int argc, char **argv) {
  if (argc <= 2) {
    fprintf(stderr, "usage: raft_node <ip> <data_port> [group_id_optional] [peer1 peer2 ...]\n");
    return 1;
  }

  string ip = argv[1];
  string data_port = argv[2];

  vector<string> peers;
  for (int i = 3; i < argc; i++) {
    peers.push_back(argv[i]);
  }

  SimpleRaftNode node(ip, data_port, peers);
  if (!node.start()) {
    fprintf(stderr, "failed to start raft node\n");
    return 1;
  }

  node.wait_forever();
  return 0;
}
