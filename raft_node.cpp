#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <fstream>
#include <fcntl.h>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
#include <vector>

#include <pthread.h>

#include "common_functions.h"
#include "proto.h"
#include "rapidjson/document.h"

using namespace rapidjson;
using namespace std;

#define CS_CONFIG "cs_config.txt"

enum class NodeRole { FOLLOWER, CANDIDATE, LEADER };

struct LogEntry {
  int term = 0;
  string op;
  string key;
  string value;
};

static unordered_map<string, string> kv_store;
static vector<LogEntry> raft_log;

static mutex mu;
static string self_addr;
static vector<string> peers;
static NodeRole role = NodeRole::FOLLOWER;
static int current_term = 0;
static string voted_for;
static string leader_addr;
static int commit_index = -1;
static int last_applied = -1;
static long last_heartbeat_ms = 0;
static string hardstate_path;
static string wal_path;
static void apply_entry(const LogEntry &e);
static void apply_committed();

static inline int majority_size() {
  int n = static_cast<int>(peers.size()) + 1;
  return n / 2 + 1;
}

static inline int last_log_index() { return static_cast<int>(raft_log.size()) - 1; }

static inline int last_log_term() {
  if (raft_log.empty()) return 0;
  return raft_log.back().term;
}

static string escape_field(const string &in) {
  string out;
  out.reserve(in.size());
  for (char c : in) {
    if (c == '\\') {
      out += "\\\\";
    } else if (c == '\t') {
      out += "\\t";
    } else if (c == '\n') {
      out += "\\n";
    } else {
      out.push_back(c);
    }
  }
  return out;
}

static string unescape_field(const string &in) {
  string out;
  out.reserve(in.size());
  for (size_t i = 0; i < in.size(); i++) {
    if (in[i] != '\\' || i + 1 >= in.size()) {
      out.push_back(in[i]);
      continue;
    }
    char n = in[++i];
    if (n == 't') out.push_back('\t');
    else if (n == 'n') out.push_back('\n');
    else out.push_back(n);
  }
  return out;
}

static bool write_all(int fd, const string &buf) {
  size_t sent = 0;
  while (sent < buf.size()) {
    ssize_t n = ::write(fd, buf.data() + sent, buf.size() - sent);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    sent += static_cast<size_t>(n);
  }
  return true;
}

static bool flush_fd(int fd) {
  while (true) {
    if (::fsync(fd) == 0) return true;
    if (errno != EINTR) return false;
  }
}

static bool persist_hardstate_locked() {
  string tmp = hardstate_path + ".tmp";
  string payload = "term\t" + to_string(current_term) + "\n" +
                   "voted_for\t" + escape_field(voted_for) + "\n" +
                   "commit_index\t" + to_string(commit_index) + "\n" +
                   "last_applied\t" + to_string(last_applied) + "\n";

  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return false;
  bool ok = write_all(fd, payload) && flush_fd(fd);
  ::close(fd);
  if (!ok) return false;
  if (::rename(tmp.c_str(), hardstate_path.c_str()) != 0) return false;
  return true;
}

static bool append_wal_entry_sync_locked(const LogEntry &e) {
  int fd = ::open(wal_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) return false;
  string line = to_string(e.term) + "\t" + escape_field(e.op) + "\t" + escape_field(e.key) +
                "\t" + escape_field(e.value) + "\n";
  bool ok = write_all(fd, line) && flush_fd(fd);
  ::close(fd);
  return ok;
}

static bool rewrite_wal_sync_locked() {
  string tmp = wal_path + ".tmp";
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return false;

  bool ok = true;
  for (const auto &e : raft_log) {
    string line = to_string(e.term) + "\t" + escape_field(e.op) + "\t" + escape_field(e.key) +
                  "\t" + escape_field(e.value) + "\n";
    if (!write_all(fd, line)) {
      ok = false;
      break;
    }
  }
  if (ok) ok = flush_fd(fd);
  ::close(fd);
  if (!ok) return false;
  if (::rename(tmp.c_str(), wal_path.c_str()) != 0) return false;
  return true;
}

static bool parse_int_field(const string &s, int &out) {
  try {
    size_t used = 0;
    int v = stoi(s, &used);
    if (used != s.size()) return false;
    out = v;
    return true;
  } catch (...) {
    return false;
  }
}

static void load_hardstate_if_present() {
  ifstream in(hardstate_path);
  if (!in.good()) return;

  string line;
  while (getline(in, line)) {
    size_t p = line.find('\t');
    if (p == string::npos) continue;
    string k = line.substr(0, p);
    string v = line.substr(p + 1);
    if (k == "term") {
      int t;
      if (parse_int_field(v, t)) current_term = t;
    } else if (k == "voted_for") {
      voted_for = unescape_field(v);
    } else if (k == "commit_index") {
      int c;
      if (parse_int_field(v, c)) commit_index = c;
    } else if (k == "last_applied") {
      int a;
      if (parse_int_field(v, a)) last_applied = a;
    }
  }
}

static void load_wal_if_present() {
  ifstream in(wal_path);
  if (!in.good()) return;

  string line;
  while (getline(in, line)) {
    vector<string> parts;
    size_t start = 0;
    while (true) {
      size_t p = line.find('\t', start);
      if (p == string::npos) {
        parts.push_back(line.substr(start));
        break;
      }
      parts.push_back(line.substr(start, p - start));
      start = p + 1;
    }
    if (parts.size() != 4) continue;
    int t;
    if (!parse_int_field(parts[0], t)) continue;
    LogEntry e;
    e.term = t;
    e.op = unescape_field(parts[1]);
    e.key = unescape_field(parts[2]);
    e.value = unescape_field(parts[3]);
    raft_log.push_back(e);
  }
}

static void recover_from_disk_locked() {
  load_hardstate_if_present();
  load_wal_if_present();

  if (raft_log.empty()) {
    commit_index = -1;
    last_applied = -1;
    return;
  }

  if (commit_index > last_log_index()) commit_index = last_log_index();
  if (commit_index < -1) commit_index = -1;
  if (last_applied > commit_index) last_applied = commit_index;
  if (last_applied < -1) last_applied = -1;

  // Rebuild the state machine from durable log up to durable commit index.
  kv_store.clear();
  last_applied = -1;
  apply_committed();
}

static string state_basename(const string &addr) {
  string out = addr;
  for (char &c : out) {
    if (c == ':' || c == '/' || c == '\\') c = '_';
  }
  return out;
}

static void apply_entry(const LogEntry &e) {
  if (e.op == "put" || e.op == "update") {
    kv_store[e.key] = e.value;
  } else if (e.op == "delete") {
    kv_store.erase(e.key);
  }
}

static void apply_committed() {
  while (last_applied < commit_index) {
    last_applied++;
    apply_entry(raft_log[last_applied]);
  }
}

static bool send_rpc(const string &target, const string &req, string &resp) {
  auto [ip, port] = split_addr(target);
  if (ip.empty() || port.empty()) return false;
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return false;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(ip.c_str());
  addr.sin_port = htons(stoi(port));
  if (::connect(fd, (sockaddr *)&addr, sizeof(addr)) < 0) {
    close(fd);
    return false;
  }
  net_send(fd, req);
  resp = net_recv(fd);
  close(fd);
  return !resp.empty();
}

static bool step_down_if_newer_term(const Document &doc) {
  if (!doc.HasMember("term") || !doc["term"].IsInt()) return false;
  int remote_term = doc["term"].GetInt();
  if (remote_term > current_term) {
    current_term = remote_term;
    role = NodeRole::FOLLOWER;
    voted_for.clear();
    if (!persist_hardstate_locked()) fprintf(stderr, "persist hardstate failed on stepdown\n");
    return true;
  }
  return false;
}

static bool replicate_to_majority(const LogEntry &entry) {
  int idx;
  int term;
  {
    lock_guard<mutex> lk(mu);
    if (role != NodeRole::LEADER) return false;
    raft_log.push_back(entry);
    if (!append_wal_entry_sync_locked(entry)) {
      raft_log.pop_back();
      return false;
    }
    idx = last_log_index();
    term = current_term;
  }

  int acks = 1;
  int prev_idx = idx - 1;
  int prev_term = 0;
  {
    lock_guard<mutex> lk(mu);
    if (prev_idx >= 0) prev_term = raft_log[prev_idx].term;
  }

  string entries_json = "[{\"term\":" + to_string(entry.term) +
                        ",\"op\":\"" + entry.op + "\",\"key\":\"" + entry.key +
                        "\",\"value\":\"" + entry.value + "\"}]";

  for (const string &peer : peers) {
    string req = msg_append_entries(term, self_addr, prev_idx, prev_term, entries_json, commit_index);
    string resp;
    if (!send_rpc(peer, req, resp)) continue;

    Document d;
    string rs = resp;
    if (d.ParseInsitu((char *)rs.data()).HasParseError()) continue;
    if (!d.HasMember("rpc") || !d["rpc"].IsString()) continue;
    if (string(d["rpc"].GetString()) != "append_reply") continue;

    lock_guard<mutex> lk(mu);
    step_down_if_newer_term(d);
    if (role != NodeRole::LEADER) break;
    if (d.HasMember("success") && d["success"].IsBool() && d["success"].GetBool()) acks++;
  }

  if (acks >= majority_size()) {
    lock_guard<mutex> lk(mu);
    if (role != NodeRole::LEADER) return false;
    commit_index = idx;
    apply_committed();
    if (!persist_hardstate_locked()) return false;
    return true;
  }

  lock_guard<mutex> lk(mu);
  if (role == NodeRole::LEADER && !raft_log.empty() && last_log_index() == idx) {
    raft_log.pop_back();
    rewrite_wal_sync_locked();
  }
  return false;
}

static string handle_request_vote(const Document &doc) {
  lock_guard<mutex> lk(mu);

  int term = doc.HasMember("term") && doc["term"].IsInt() ? doc["term"].GetInt() : 0;
  string candidate = doc.HasMember("candidate") && doc["candidate"].IsString() ? doc["candidate"].GetString() : "";
  int cand_last_idx = doc.HasMember("last_log_index") && doc["last_log_index"].IsInt() ? doc["last_log_index"].GetInt() : -1;
  int cand_last_term = doc.HasMember("last_log_term") && doc["last_log_term"].IsInt() ? doc["last_log_term"].GetInt() : 0;

  if (term < current_term) return msg_vote_reply(current_term, false);
  if (term > current_term) {
    current_term = term;
    role = NodeRole::FOLLOWER;
    voted_for.clear();
    if (!persist_hardstate_locked()) return msg_vote_reply(current_term, false);
  }

  bool up_to_date = (cand_last_term > last_log_term()) ||
                    (cand_last_term == last_log_term() && cand_last_idx >= last_log_index());

  bool grant = false;
  if ((voted_for.empty() || voted_for == candidate) && up_to_date) {
    voted_for = candidate;
    if (!persist_hardstate_locked()) {
      voted_for.clear();
      return msg_vote_reply(current_term, false);
    }
    grant = true;
    last_heartbeat_ms = now_epoch_ms();
  }

  return msg_vote_reply(current_term, grant);
}

static string handle_append_entries(const Document &doc) {
  lock_guard<mutex> lk(mu);

  int term = doc.HasMember("term") && doc["term"].IsInt() ? doc["term"].GetInt() : 0;
  string leader = doc.HasMember("leader") && doc["leader"].IsString() ? doc["leader"].GetString() : "";
  int prev_idx = doc.HasMember("prev_index") && doc["prev_index"].IsInt() ? doc["prev_index"].GetInt() : -1;
  int prev_term = doc.HasMember("prev_term") && doc["prev_term"].IsInt() ? doc["prev_term"].GetInt() : 0;
  int leader_commit = doc.HasMember("commit") && doc["commit"].IsInt() ? doc["commit"].GetInt() : -1;

  if (term < current_term) return msg_append_reply(current_term, false, last_log_index());

  if (term >= current_term) {
    current_term = term;
    role = NodeRole::FOLLOWER;
    voted_for.clear();
    leader_addr = leader;
    last_heartbeat_ms = now_epoch_ms();
    if (!persist_hardstate_locked()) return msg_append_reply(current_term, false, last_log_index());
  }

  if (prev_idx >= 0) {
    if (prev_idx > last_log_index()) return msg_append_reply(current_term, false, last_log_index());
    if (raft_log[prev_idx].term != prev_term) {
      raft_log.resize(prev_idx);
      if (commit_index >= static_cast<int>(raft_log.size())) commit_index = static_cast<int>(raft_log.size()) - 1;
      if (last_applied > commit_index) last_applied = commit_index;
      if (!rewrite_wal_sync_locked()) return msg_append_reply(current_term, false, last_log_index());
      if (!persist_hardstate_locked()) return msg_append_reply(current_term, false, last_log_index());
      return msg_append_reply(current_term, false, last_log_index());
    }
  }

  bool log_changed = false;
  if (doc.HasMember("entries") && doc["entries"].IsArray()) {
    if (last_log_index() > prev_idx) {
      raft_log.resize(prev_idx + 1);
      if (commit_index > last_log_index()) commit_index = last_log_index();
      if (last_applied > commit_index) last_applied = commit_index;
      log_changed = true;
    }
    const auto &arr = doc["entries"].GetArray();
    for (const auto &v : arr) {
      if (!v.IsObject()) continue;
      LogEntry e;
      e.term = v.HasMember("term") && v["term"].IsInt() ? v["term"].GetInt() : current_term;
      e.op = v.HasMember("op") && v["op"].IsString() ? v["op"].GetString() : "";
      e.key = v.HasMember("key") && v["key"].IsString() ? v["key"].GetString() : "";
      e.value = v.HasMember("value") && v["value"].IsString() ? v["value"].GetString() : "";
      raft_log.push_back(e);
      log_changed = true;
    }
  }
  if (log_changed && !rewrite_wal_sync_locked()) return msg_append_reply(current_term, false, last_log_index());

  if (leader_commit > commit_index) {
    commit_index = min(leader_commit, last_log_index());
    apply_committed();
    if (!persist_hardstate_locked()) return msg_append_reply(current_term, false, last_log_index());
  }

  return msg_append_reply(current_term, true, last_log_index());
}

static string handle_kv_role(const string &op, const string &key, const string &value) {
  if (op == "get") {
    lock_guard<mutex> lk(mu);
    auto it = kv_store.find(key);
    if (it == kv_store.end()) return msg_ack("ack", "key_error");
    return msg_ack("data", it->second);
  }

  {
    lock_guard<mutex> lk(mu);
    if (role != NodeRole::LEADER) return msg_redirect(leader_addr);
  }

  LogEntry e;
  {
    lock_guard<mutex> lk(mu);
    e.term = current_term;
  }
  e.op = op;
  e.key = key;
  e.value = value;

  bool ok = replicate_to_majority(e);
  if (!ok) return msg_ack("ack", "commit_failed");

  if (op == "put") return msg_ack("ack", "put_success");
  if (op == "update") return msg_ack("ack", "update_success");
  if (op == "delete") return msg_ack("ack", "delete_success");
  return msg_ack("ack", "unknown_role");
}

static void handle_connection(int client_fd) {
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

  if (doc.HasMember("rpc") && doc["rpc"].IsString()) {
    string rpc = doc["rpc"].GetString();
    if (rpc == "request_vote") {
      net_send(client_fd, handle_request_vote(doc));
    } else if (rpc == "append_entries") {
      net_send(client_fd, handle_append_entries(doc));
    } else {
      net_send(client_fd, msg_ack("ack", "unknown_rpc"));
    }
    close(client_fd);
    return;
  }

  if (!doc.HasMember("role") || !doc["role"].IsString()) {
    net_send(client_fd, msg_ack("ack", "invalid_request"));
    close(client_fd);
    return;
  }

  string op = doc["role"].GetString();
  string key = doc.HasMember("key") && doc["key"].IsString() ? doc["key"].GetString() : "";
  string value = doc.HasMember("value") && doc["value"].IsString() ? doc["value"].GetString() : "";

  net_send(client_fd, handle_kv_role(op, key, value));
  close(client_fd);
}

static void *serve_requests(void *ptr) {
  auto *td = (pair<string, string> *)ptr;
  string ip = td->first;
  string port = td->second;

  int server_fd = make_tcp_server(ip, port);
  listen(server_fd, 64);

  while (true) {
    sockaddr_in peer{};
    socklen_t peer_len = sizeof(peer);
    int client_fd = accept(server_fd, (sockaddr *)&peer, &peer_len);
    if (client_fd < 0) continue;
    handle_connection(client_fd);
  }
  return nullptr;
}

static void start_election() {
  int term;
  int my_last_idx;
  int my_last_term;
  {
    lock_guard<mutex> lk(mu);
    role = NodeRole::CANDIDATE;
    current_term++;
    term = current_term;
    voted_for = self_addr;
    my_last_idx = last_log_index();
    my_last_term = last_log_term();
    if (!persist_hardstate_locked()) {
      role = NodeRole::FOLLOWER;
      voted_for.clear();
      return;
    }
  }

  int votes = 1;
  string req = msg_request_vote(term, self_addr, my_last_idx, my_last_term);

  for (const string &peer : peers) {
    string resp;
    if (!send_rpc(peer, req, resp)) continue;

    Document d;
    string rs = resp;
    if (d.ParseInsitu((char *)rs.data()).HasParseError()) continue;
    if (!d.HasMember("rpc") || !d["rpc"].IsString()) continue;
    if (string(d["rpc"].GetString()) != "vote_reply") continue;

    lock_guard<mutex> lk(mu);
    if (step_down_if_newer_term(d)) return;
    if (role != NodeRole::CANDIDATE || current_term != term) return;
    if (d.HasMember("granted") && d["granted"].IsBool() && d["granted"].GetBool()) votes++;
  }

  lock_guard<mutex> lk(mu);
  if (role == NodeRole::CANDIDATE && current_term == term && votes >= majority_size()) {
    role = NodeRole::LEADER;
    leader_addr = self_addr;
    last_heartbeat_ms = now_epoch_ms();
  }
}

static void *election_thread(void *) {
  std::mt19937 rng(static_cast<unsigned>(now_epoch_ms()));
  std::uniform_int_distribution<int> timeout_dist(500, 900);

  while (true) {
    this_thread::sleep_for(chrono::milliseconds(50));

    bool should_elect = false;
    {
      lock_guard<mutex> lk(mu);
      if (peers.empty()) {
        role = NodeRole::LEADER;
        leader_addr = self_addr;
        continue;
      }
      if (role == NodeRole::LEADER) continue;
      int timeout_ms = timeout_dist(rng);
      if (now_epoch_ms() - last_heartbeat_ms > timeout_ms) should_elect = true;
    }

    if (should_elect) start_election();
  }
  return nullptr;
}

static void *heartbeat_thread(void *) {
  while (true) {
    this_thread::sleep_for(chrono::milliseconds(180));

    int term;
    int local_commit;
    int prev_idx;
    int prev_term;
    {
      lock_guard<mutex> lk(mu);
      if (role != NodeRole::LEADER) continue;
      term = current_term;
      local_commit = commit_index;
      prev_idx = last_log_index();
      prev_term = last_log_term();
    }

    for (const string &peer : peers) {
      string req = msg_append_entries(term, self_addr, prev_idx, prev_term, "[]", local_commit);
      string resp;
      if (!send_rpc(peer, req, resp)) continue;

      Document d;
      string rs = resp;
      if (d.ParseInsitu((char *)rs.data()).HasParseError()) continue;
      if (!d.HasMember("rpc") || !d["rpc"].IsString()) continue;
      if (string(d["rpc"].GetString()) != "append_reply") continue;

      lock_guard<mutex> lk(mu);
      step_down_if_newer_term(d);
    }
  }
  return nullptr;
}

static void register_with_coordinator(const string &group_id) {
  string cs_ip, cs_port;
  ifstream f(CS_CONFIG);
  getline(f, cs_ip);
  getline(f, cs_port);
  if (cs_ip.empty() || cs_port.empty()) {
    fprintf(stderr, "cs_config.txt missing. start coordinator first.\n");
    exit(1);
  }

  int fd = make_tcp_client();
  tcp_connect(fd, cs_ip, cs_port);
  net_recv(fd);
  net_send(fd, msg_node_join(group_id, self_addr));
  net_recv(fd);
  close(fd);
}

int main(int argc, char **argv) {
  if (argc <= 2) {
    fprintf(stderr, "usage: raft_node <ip> <port> [group_id] [peer1 peer2 ...]\n");
    return 1;
  }

  string ip = argv[1];
  string port = argv[2];
  self_addr = ip + ":" + port;
  string base = state_basename(self_addr);
  hardstate_path = "hardstate_" + base + ".txt";
  wal_path = "wal_" + base + ".log";

  string group_id = (argc >= 4) ? string(argv[3]) : to_string(ring_hash(self_addr));
  for (int i = 4; i < argc; i++) {
    string p = argv[i];
    if (p != self_addr) peers.push_back(p);
  }

  {
    lock_guard<mutex> lk(mu);
    recover_from_disk_locked();
    if (peers.empty()) {
      role = NodeRole::LEADER;
      leader_addr = self_addr;
    } else {
      role = NodeRole::FOLLOWER;
      leader_addr.clear();
    }
    last_heartbeat_ms = now_epoch_ms();
    if (!persist_hardstate_locked()) {
      fprintf(stderr, "failed to persist initial hardstate\n");
      return 1;
    }
  }

  register_with_coordinator(group_id);

  auto *td = new pair<string, string>(ip, port);
  pthread_t srv;
  pthread_t elect;
  pthread_t hbeat;
  pthread_create(&srv, nullptr, serve_requests, td);
  pthread_create(&elect, nullptr, election_thread, nullptr);
  pthread_create(&hbeat, nullptr, heartbeat_thread, nullptr);

  pthread_join(srv, nullptr);
  pthread_join(elect, nullptr);
  pthread_join(hbeat, nullptr);
  return 0;
}
