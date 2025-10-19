#include "common_functions.h"
#include "proto.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include <bits/stdc++.h>
using namespace rapidjson;
using namespace std;

#define CS_CONFIG "cs_config.txt"

void print_response(const string &json) {
  Document doc;
  string s = json;
  if (doc.ParseInsitu((char *)s.c_str()).HasParseError())
    return;
  printf("%s\n", doc["message"].GetString());
}

void do_get_delete(int fd, const string &op, const string &key) {
  net_send(fd, msg_get_delete(op, key));
  print_response(net_recv(fd));
}

void do_put_update(int fd, const string &op, const string &key,
                   const string &val) {
  net_send(fd, msg_put_update(op, key, val));
  print_response(net_recv(fd));
}

int main(int argc, char **argv) {
  if (argc <= 2) {
    fprintf(stderr, "usage: client <ip> <port>\n");
    return 1;
  }

  string cs_ip, cs_port;
  ifstream f(CS_CONFIG);
  getline(f, cs_ip);
  getline(f, cs_port);

  int fd = make_tcp_server(argv[1], argv[2]);
  tcp_connect(fd, cs_ip, cs_port);
  net_recv(fd);
  net_send(fd, msg_identity("client"));
  net_recv(fd);

  printf("commands: get:<key>  put:<key>:<val>  update:<key>:<val>  "
         "delete:<key>  exit\n");
  while (true) {
    printf(">> ");
    string cmd;
    cin >> cmd;
    if (cmd == "exit")
      break;

    char buf[1024];
    strncpy(buf, cmd.c_str(), sizeof(buf) - 1);
    string op = strtok(buf, ":");

    if (op == "get" || op == "delete") {
      do_get_delete(fd, op, strtok(nullptr, ":"));
    } else if (op == "put" || op == "update") {
      string key = strtok(nullptr, ":");
      string val = strtok(nullptr, ":");
      do_put_update(fd, op, key, val);
    }
  }
}

static vector<string> split_colon_tokens(const string &cmd) {
  vector<string> out;
  string cur;
  for (char ch : cmd) {
    if (ch == ':') {
      out.push_back(cur);
      cur.clear();
      continue;
    }
    cur.push_back(ch);
  }
  out.push_back(cur);
  return out;
}

static bool is_supported_client_op(const string &op) {
  return op == "get" || op == "put" || op == "update" || op == "delete";
}

static vector<string> split_colon_tokens(const string &cmd) {
  vector<string> out;
  string cur;
  for (char ch : cmd) {
    if (ch == ':') {
      out.push_back(cur);
      cur.clear();
      continue;
    }
    cur.push_back(ch);
  }
  out.push_back(cur);
  return out;
}

static bool is_supported_client_op(const string &op) {
  return op == "get" || op == "put" || op == "update" || op == "delete";
}

static string trim_spaces(const string &s) {
  size_t i = 0, j = s.size();
  while (i < j && isspace(static_cast<unsigned char>(s[i]))) i++;
  while (j > i && isspace(static_cast<unsigned char>(s[j - 1]))) j--;
  return s.substr(i, j - i);
}

static bool has_min_parts(const vector<string> &parts, size_t expected) {
  return parts.size() >= expected;
}

static vector<string> split_colon_tokens(const string &cmd) {
  vector<string> out;
  string cur;
  for (char ch : cmd) {
    if (ch == ':') {
      out.push_back(cur);
      cur.clear();
      continue;
    }
    cur.push_back(ch);
  }
  out.push_back(cur);
  return out;
}

static bool is_supported_client_op(const string &op) {
  return op == "get" || op == "put" || op == "update" || op == "delete";
}

static bool valid_client_tokens(const vector<string> &parts) {
  if (parts.empty()) return false;
  if (parts[0] == "get" || parts[0] == "delete") return parts.size() >= 2;
  if (parts[0] == "put" || parts[0] == "update") return parts.size() >= 3;
  return false;
}

static string trim_spaces(const string &s) {
  size_t i = 0, j = s.size();
  while (i < j && isspace(static_cast<unsigned char>(s[i]))) i++;
  while (j > i && isspace(static_cast<unsigned char>(s[j - 1]))) j--;
  return s.substr(i, j - i);
}
