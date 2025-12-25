#include <fstream>
#include <iostream>
#include <string>

#include "common_functions.h"

using namespace std;

#define CS_CONFIG "cs_config.txt"

static bool starts_with(const string &s, const string &prefix) { return s.rfind(prefix, 0) == 0; }

static void print_response(const string &resp) {
  if (starts_with(resp, "value:")) {
    printf("%s\n", resp.substr(6).c_str());
    return;
  }
  if (starts_with(resp, "ok:")) {
    printf("%s\n", resp.substr(3).c_str());
    return;
  }
  if (starts_with(resp, "err:")) {
    printf("%s\n", resp.substr(4).c_str());
    return;
  }
  printf("%s\n", resp.c_str());
}

int main(int argc, char **) {
  if (argc <= 2) {
    fprintf(stderr, "usage: client <ip> <port>\n");
    return 1;
  }

  string cs_ip, cs_port;
  ifstream f(CS_CONFIG);
  getline(f, cs_ip);
  getline(f, cs_port);
  if (cs_ip.empty() || cs_port.empty()) {
    fprintf(stderr, "cs_config.txt missing. start coordinator first.\n");
    return 1;
  }

  int fd = make_tcp_client();
  if (!tcp_connect(fd, cs_ip, cs_port)) {
    fprintf(stderr, "failed to connect to coordinator\n");
    close(fd);
    return 1;
  }

  printf("commands: get:<key>  put:<key>:<val>  update:<key>:<val>  delete:<key>  exit\n");
  while (true) {
    printf(">> ");
    string cmd;
    if (!getline(cin, cmd)) break;
    if (cmd.empty()) continue;
    if (cmd == "exit") break;

    if (!send_line(fd, cmd)) {
      printf("connection_error\n");
      break;
    }
    string resp = recv_line_or_empty(fd);
    if (resp.empty()) {
      printf("connection_error\n");
      break;
    }
    print_response(resp);
  }

  close(fd);
  return 0;
}
