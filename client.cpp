#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "common_functions.h"
#include "proto.h"
#include "rapidjson/document.h"

using namespace rapidjson;
using namespace std;

#define CS_CONFIG "cs_config.txt"

static void print_response(const string &json) {
  Document doc;
  string s = json;
  if (doc.ParseInsitu((char *)s.data()).HasParseError()) {
    printf("invalid_response\n");
    return;
  }
  if (doc.HasMember("message") && doc["message"].IsString()) {
    printf("%s\n", doc["message"].GetString());
  } else {
    printf("invalid_response\n");
  }
}

static vector<string> split_colon_tokens(const string &cmd) {
  vector<string> out;
  string cur;
  for (char ch : cmd) {
    if (ch == ':') {
      out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(ch);
    }
  }
  out.push_back(cur);
  return out;
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
  tcp_connect(fd, cs_ip, cs_port);
  net_recv(fd);
  net_send(fd, msg_identity("client"));
  net_recv(fd);

  printf("commands: get:<key>  put:<key>:<val>  update:<key>:<val>  delete:<key>  exit\n");
  while (true) {
    printf(">> ");
    string cmd;
    cin >> cmd;
    if (cmd == "exit") break;

    vector<string> parts = split_colon_tokens(cmd);
    if (parts.empty()) continue;

    const string &op = parts[0];
    if ((op == "get" || op == "delete") && parts.size() >= 2) {
      net_send(fd, msg_get_delete(op, parts[1]));
      print_response(net_recv(fd));
    } else if ((op == "put" || op == "update") && parts.size() >= 3) {
      net_send(fd, msg_put_update(op, parts[1], parts[2]));
      print_response(net_recv(fd));
    } else {
      printf("invalid command\n");
    }
  }

  close(fd);
  return 0;
}
