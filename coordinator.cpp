#include <arpa/inet.h>
#include <cerrno>
#include <condition_variable>
#include <cstdio>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common_functions.h"

namespace {

std::mutex g_mu;
std::condition_variable g_leader_cv;
std::string g_current_leader;

void write_cs_config(const std::string &ip, const std::string &port) {
  std::ofstream f("cs_config.txt");
  f << ip << "\n" << port << "\n";
}

void register_node(const std::string &, const std::string &) {
}

void set_current_leader(const std::string &leader_addr) {
  std::lock_guard<std::mutex> lk(g_mu);
  g_current_leader = leader_addr;
  g_leader_cv.notify_all();
}

void clear_current_leader_if_match(const std::string &leader_addr) {
  std::lock_guard<std::mutex> lk(g_mu);
  if (g_current_leader == leader_addr) g_current_leader.clear();
}

std::string get_current_leader_snapshot() {
  std::lock_guard<std::mutex> lk(g_mu);
  return g_current_leader;
}

std::string wait_for_leader() {
  std::unique_lock<std::mutex> lk(g_mu);
  while (g_current_leader.empty()) {
    g_leader_cv.wait(lk);
  }
  return g_current_leader;
}

std::string send_request_line(const std::string &target_addr, const std::string &line) {
  auto [ip, port] = split_addr(target_addr);

  int fd = make_tcp_client();
  if (!tcp_connect(fd, ip, port)) {
    close(fd);
    return "err:no_nodes_available";
  }
  if (!send_line(fd, line)) {
    close(fd);
    return "err:no_nodes_available";
  }

  std::string resp = recv_line_or_empty(fd);
  close(fd);
  if (resp.empty()) return "err:no_nodes_available";
  return resp;
}

bool is_retryable_response(const std::string &resp) {
  return resp == "err:not_leader" || resp == "err:no_nodes_available" || resp == "err:commit_failed";
}

std::string forward_to_leader(const std::string &cmd_line) {
  std::string leader_addr = wait_for_leader();
  std::string resp = send_request_line(leader_addr, cmd_line);
  if (is_retryable_response(resp)) {
    clear_current_leader_if_match(leader_addr);
    leader_addr = wait_for_leader();
    resp = send_request_line(leader_addr, cmd_line);
  }
  return resp;
}

void serve_client_stream(int fd, std::string first_line) {
  std::string line = first_line;
  while (!line.empty()) {
    send_line(fd, forward_to_leader(line));
    if (!recv_line(fd, line)) break;
  }
}

void handle_connection(int fd) {
  std::string first_line = recv_line_or_empty(fd);
  if (first_line.empty()) {
    close(fd);
    return;
  }

  std::vector<std::string> parts = split_string(first_line, '|');
  if (parts[0] == "node_join") {
    register_node(parts[1], parts[2]);
    send_line(fd, "ok:node_registered");
    close(fd);
    return;
  }

  if (parts[0] == "leader_announce") {
    set_current_leader(parts[1]);
    send_line(fd, "ok:leader_updated");
    close(fd);
    return;
  }

  if (first_line == "leader_sync") {
    send_line(fd, join_fields({"leader", get_current_leader_snapshot()}, '|'));
    close(fd);
    return;
  }

  serve_client_stream(fd, first_line);
  close(fd);
}

}  // namespace

int main(int argc, char **argv) {
  if (argc <= 2) {
    fprintf(stderr, "usage: coordinator <ip> <port>\n");
    return 1;
  }
  std::string ip = argv[1], port = argv[2];

  write_cs_config(ip, port);

  int server_fd = make_tcp_server(ip, port);
  listen(server_fd, 32);

  while (true) {
    sockaddr_in peer{};
    socklen_t len = sizeof(peer);
    int conn = accept(server_fd, (sockaddr *)&peer, &len);
    if (conn < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
      perror("accept");
      continue;
    }
    std::thread(handle_connection, conn).detach();
  }
}
