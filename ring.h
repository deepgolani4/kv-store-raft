#pragma once
#include <bits/stdc++.h>
using namespace std;
// basic AVL tree implementation
struct RingNode {
  int id;
  string addr;
  int lo = INT_MIN, hi = INT_MAX;
  RingNode *left = nullptr, *right = nullptr;
  RingNode() : id(0) {}
};

RingNode *ring_root = nullptr;

class RingTree {
public:
  RingNode *min_node(RingNode *n) {
    while (n->left)
      n = n->left;
    return n;
  }

  RingNode *max_node(RingNode *n) {
    while (n->right)
      n = n->right;
    return n;
  }

  void find_neighbors(RingNode *cur, RingNode *&pre, RingNode *&succ, int id) {
    if (!cur)
      return;
    if (cur->id == id) {
      if (cur->left) {
        RingNode *t = cur->left;
        while (t->right)
          t = t->right;
        pre = t;
      }
      if (cur->right) {
        RingNode *t = cur->right;
        while (t->left)
          t = t->left;
        succ = t;
      }
      return;
    }
    if (cur->id > id) {
      succ = cur;
      find_neighbors(cur->left, pre, succ, id);
    } else {
      pre = cur;
      find_neighbors(cur->right, pre, succ, id);
    }
  }

  RingNode *insert(RingNode *node, int id, const string &addr) {
    if (!node) {
      RingNode *n = new RingNode;
      n->id = id;
      n->addr = addr;
      n->lo = id;
      n->hi = id;
      return n;
    }
    if (id < node->id) {
      node->lo = min(node->lo, id);
      node->left = insert(node->left, id, addr);
    } else {
      node->hi = max(node->hi, id);
      node->right = insert(node->right, id, addr);
    }
    return node;
  }

  RingNode *remove(RingNode *node, int id) {
    if (!node)
      return nullptr;
    if (id < node->id)
      node->left = remove(node->left, id);
    else if (id > node->id)
      node->right = remove(node->right, id);
    else {
      if (!node->left) {
        RingNode *t = node->right;
        free(node);
        return t;
      }
      if (!node->right) {
        RingNode *t = node->left;
        free(node);
        return t;
      }
      RingNode *t = min_node(node->right);
      node->id = t->id;
      node->addr = t->addr;
      node->right = remove(node->right, t->id);
    }
    return node;
  }

  void preorder(RingNode *n) {
    if (!n)
      return;
    preorder(n->left);
    preorder(n->right);
  }
};

inline bool ring_id_in_half_open_range(int id, int lo, int hi) {
  if (lo < hi) return lo <= id && id < hi;
  return (lo <= id && id < RING_SIZE) || (0 <= id && id < hi);
}

inline string ring_node_label(const RingNode *n) {
  if (!n) return "<nil>";
  return n->addr + "#" + to_string(n->id);
}

inline void collect_ring_addresses(RingNode *node, vector<string> &out) {
  if (!node) return;
  collect_ring_addresses(node->left, out);
  out.push_back(node->addr);
  collect_ring_addresses(node->right, out);
}

inline bool ring_id_in_half_open_range(int id, int lo, int hi) {
  if (lo < hi) return lo <= id && id < hi;
  return (lo <= id && id < RING_SIZE) || (0 <= id && id < hi);
}

inline string ring_node_label(const RingNode *n) {
  if (!n) return "<nil>";
  return n->addr + "#" + to_string(n->id);
}

inline void collect_ring_addresses(RingNode *node, vector<string> &out) {
  if (!node) return;
  collect_ring_addresses(node->left, out);
  out.push_back(node->addr);
  collect_ring_addresses(node->right, out);
}
