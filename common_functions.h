#pragma once

#include <arpa/inet.h>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

inline int make_tcp_server(const std::string &ip, const std::string &port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    perror("socket");
    exit(1);
  }
  int opt = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(ip.c_str());
  addr.sin_port = htons(static_cast<uint16_t>(std::stoi(port)));
  if (::bind(fd, (sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("bind");
    exit(1);
  }
  return fd;
}

inline int make_tcp_client() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    perror("socket");
    exit(1);
  }
  int opt = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif
  return fd;
}

inline bool tcp_connect(int fd, const std::string &ip, const std::string &port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(ip.c_str());
  addr.sin_port = htons(static_cast<uint16_t>(std::stoi(port)));
  while (true) {
    if (::connect(fd, (sockaddr *)&addr, sizeof(addr)) == 0) return true;
    if (errno != EINTR) return false;
  }
}

inline bool send_all(int fd, const std::string &data) {
  size_t sent = 0;
  while (sent < data.size()) {
    ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, 0);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    sent += static_cast<size_t>(n);
  }
  return true;
}

inline bool send_line(int fd, const std::string &line) {
  return send_all(fd, line + "\n");
}

inline bool recv_line(int fd, std::string &out) {
  out.clear();
  char ch = 0;
  while (true) {
    ssize_t n = ::recv(fd, &ch, 1, 0);
    if (n == 0) return !out.empty();
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (ch == '\n') return true;
    if (ch != '\r') out.push_back(ch);
  }
}

inline std::string recv_line_or_empty(int fd) {
  std::string out;
  if (!recv_line(fd, out)) return "";
  return out;
}

inline std::pair<std::string, std::string> split_addr(const std::string &addr) {
  size_t pos = addr.find(':');
  if (pos == std::string::npos) return {addr, ""};
  return {addr.substr(0, pos), addr.substr(pos + 1)};
}

inline std::vector<std::string> split_string(const std::string &s, char delim) {
  std::vector<std::string> out;
  size_t start = 0;
  while (true) {
    size_t pos = s.find(delim, start);
    if (pos == std::string::npos) {
      out.push_back(s.substr(start));
      return out;
    }
    out.push_back(s.substr(start, pos - start));
    start = pos + 1;
  }
}

inline std::string join_fields(const std::vector<std::string> &fields, char delim) {
  if (fields.empty()) return "";
  std::string out = fields[0];
  for (size_t i = 1; i < fields.size(); i++) {
    out.push_back(delim);
    out += fields[i];
  }
  return out;
}

inline long now_epoch_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}
