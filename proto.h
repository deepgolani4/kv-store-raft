#pragma once
#include <string>
using namespace std;


string msg_ack(const string &type, const string &body) {
    return "{\"req_type\":\"" + type + "\",\"message\":\"" + body + "\"}";
}

string msg_identity(const string &id) {
    return "{\"req_type\":\"identity\",\"id\":\"" + id + "\"}";
}

string msg_get_delete(const string &op, const string &key) {
    return "{\"req_type\":\"" + op + "\",\"key\":\"" + key + "\"}";
}

string msg_put_update(const string &op, const string &key, const string &value) {
    return "{\"req_type\":\"" + op + "\",\"key\":\"" + key + "\",\"value\":\"" + value + "\"}";
}

string msg_kv_op(const string &op, const string &key, const string &value = "") {
    return "{\"role\":\"" + op + "\",\"key\":\"" + key + "\",\"value\":\"" + value + "\"}";
}


string msg_node_join(const string &group_id, const string &addr) {
    return "{\"req_type\":\"node_join\",\"group\":\"" + group_id + "\",\"addr\":\"" + addr + "\"}";
}

string msg_request_vote(int term, const string &candidate,
                        int last_log_index, int last_log_term) {
    return "{\"rpc\":\"request_vote\",\"term\":" + to_string(term)
         + ",\"candidate\":\"" + candidate + "\""
         + ",\"last_log_index\":" + to_string(last_log_index)
         + ",\"last_log_term\":" + to_string(last_log_term) + "}";
}

string msg_vote_reply(int term, bool granted) {
    return "{\"rpc\":\"vote_reply\",\"term\":" + to_string(term)
         + ",\"granted\":" + (granted ? "true" : "false") + "}";
}

string msg_append_entries(int term, const string &leader,
                          int prev_index, int prev_term,
                          const string &entries_json, int leader_commit) {
    return "{\"rpc\":\"append_entries\",\"term\":" + to_string(term)
         + ",\"leader\":\"" + leader + "\""
         + ",\"prev_index\":" + to_string(prev_index)
         + ",\"prev_term\":" + to_string(prev_term)
         + ",\"entries\":" + entries_json
         + ",\"commit\":" + to_string(leader_commit) + "}";
}

string msg_append_reply(int term, bool success, int match_index) {
    return "{\"rpc\":\"append_reply\",\"term\":" + to_string(term)
         + ",\"success\":" + (success ? "true" : "false")
         + ",\"match\":" + to_string(match_index) + "}";
}

string msg_install_snapshot(int term, const string &leader,
                             int last_index, int last_term,
                             const string &data) {
    return "{\"rpc\":\"install_snapshot\",\"term\":" + to_string(term)
         + ",\"leader\":\"" + leader + "\""
         + ",\"last_index\":" + to_string(last_index)
         + ",\"last_term\":" + to_string(last_term)
         + ",\"data\":\"" + data + "\"}";
}

string msg_snapshot_reply(int term) {
    return "{\"rpc\":\"snapshot_reply\",\"term\":" + to_string(term) + "}";
}

string msg_redirect(const string &leader_addr) {
    return "{\"req_type\":\"redirect\",\"leader\":\"" + leader_addr + "\"}";
}


string msg_health_ping(const string &node) {
    return "{\"req_type\":\"health_ping\",\"node\":\"" + node + "\"}";
}

string msg_health_pong(const string &node, const string &status) {
    return "{\"req_type\":\"health_pong\",\"node\":\"" + node + "\",\"status\":\"" + status + "\"}";
}

string msg_client_error(const string &reason) {
    return "{\"req_type\":\"client_error\",\"message\":\"" + reason + "\"}";
}

string msg_health_ping(const string &node) {
    return "{\"req_type\":\"health_ping\",\"node\":\"" + node + "\"}";
}

string msg_health_pong(const string &node, const string &status) {
    return "{\"req_type\":\"health_pong\",\"node\":\"" + node + "\",\"status\":\"" + status + "\"}";
}

string msg_client_error(const string &reason) {
    return "{\"req_type\":\"client_error\",\"message\":\"" + reason + "\"}";
}
