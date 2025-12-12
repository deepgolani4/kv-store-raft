#pragma once

#include <cctype>
#include <string>
#include <unordered_map>

using namespace std;

struct LRUNode {
  string key, value;
  LRUNode *prev = nullptr, *next = nullptr;
  LRUNode(const string &k, const string &v) : key(k), value(v) {}
};

class LRUCache {
  int capacity, count = 0;
  LRUNode *head = nullptr, *tail = nullptr;
  unordered_map<string, LRUNode *> index;

  void evict(LRUNode *n) {
    count--;
    index.erase(n->key);
    if (head == tail) {
      head = tail = nullptr;
    } else if (n == tail) {
      tail = n->prev;
      tail->next = nullptr;
    } else if (n == head) {
      head = n->next;
      head->prev = nullptr;
    } else {
      n->prev->next = n->next;
      n->next->prev = n->prev;
    }
    delete n;
  }

  void push_front(const string &k, const string &v) {
    count++;
    LRUNode *n = new LRUNode(k, v);
    index[k] = n;
    if (!head) {
      head = tail = n;
      return;
    }
    n->next = head;
    head->prev = n;
    head = n;
  }

  void move_to_front(LRUNode *n) {
    if (n == head) return;
    if (n == tail) {
      tail = n->prev;
      tail->next = nullptr;
    } else {
      n->prev->next = n->next;
      n->next->prev = n->prev;
    }
    n->prev = nullptr;
    n->next = head;
    head->prev = n;
    head = n;
  }

public:
  explicit LRUCache(int cap = 0) : capacity(cap) {}

  void put(const string &k, const string &v) {
    if (!capacity || index.count(k)) return;
    if (count >= capacity) evict(tail);
    push_front(k, v);
  }

  void update(const string &k, const string &v) {
    if (!capacity) return;
    auto it = index.find(k);
    if (it == index.end()) return;
    it->second->value = v;
    move_to_front(it->second);
  }

  string get(const string &k) {
    if (!capacity) return "";
    auto it = index.find(k);
    if (it == index.end()) return "";
    move_to_front(it->second);
    return it->second->value;
  }

  void remove(const string &k) {
    auto it = index.find(k);
    if (it != index.end()) evict(it->second);
  }

  bool contains(const string &k) const { return index.count(k); }
};

inline LRUCache cache(4);

inline string normalize_cache_key(const string &key) {
  string out = key;
  for (char &c : out) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
  return out;
}

inline bool cacheable_value(const string &v) { return !v.empty() && v != "key_error"; }
