#pragma once

#include <arpa/inet.h>
#include <chrono>
#include <cctype>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

using namespace std;

#define RING_SIZE 31
#define BUFF_SIZE 1024
#define UDP_PORT 3769
#define HASH_MUL 99999989

inline int ring_hash(const string &s) {
  int j = RING_SIZE;
  int h = 0;
  for (char c : s) h = (h + (static_cast<int>(c) * HASH_MUL) % j) % j;
  return (h + j) % j;
}

inline int make_tcp_server(const string &ip, const string &port) {
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
  addr.sin_port = htons(stoi(port));
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

inline void tcp_connect(int fd, const string &ip, const string &port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(ip.c_str());
  addr.sin_port = htons(stoi(port));
  if (::connect(fd, (sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("connect");
    exit(1);
  }
}

inline void net_send(int fd, const string &msg) {
  char buf[BUFF_SIZE];
  strncpy(buf, msg.c_str(), BUFF_SIZE - 1);
  buf[BUFF_SIZE - 1] = '\0';
  ::send(fd, buf, sizeof(buf), 0);
}

inline string net_recv(int fd) {
  char buf[BUFF_SIZE] = {0};
  ::recv(fd, buf, BUFF_SIZE, 0);
  return string(buf);
}

inline pair<string, string> split_addr(const string &addr) {
  size_t pos = addr.find(':');
  if (pos == string::npos) return {addr, ""};
  return {addr.substr(0, pos), addr.substr(pos + 1)};
}

inline bool is_numeric_port_string(const string &port) {
  if (port.empty()) return false;
  for (char c : port)
    if (!isdigit(static_cast<unsigned char>(c))) return false;
  int p = stoi(port);
  return p > 0 && p <= 65535;
}

inline string safe_recv_trimmed(int fd) {
  string raw = net_recv(fd);
  while (!raw.empty() && (raw.back() == '\n' || raw.back() == '\r' || raw.back() == ' ')) {
    raw.pop_back();
  }
  return raw;
}

inline bool addr_has_port(const string &addr) { return addr.find(':') != string::npos; }

inline string trim_copy(const string &s) {
  size_t i = 0, j = s.size();
  while (i < j && isspace(static_cast<unsigned char>(s[i]))) i++;
  while (j > i && isspace(static_cast<unsigned char>(s[j - 1]))) j--;
  return s.substr(i, j - i);
}

inline long now_epoch_ms() {
  using namespace chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}
