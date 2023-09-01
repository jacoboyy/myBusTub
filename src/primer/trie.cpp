#include "primer/trie.h"
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  std::shared_ptr<const TrieNode> node = root_;
  auto it = key.begin();
  while (it < key.end() && node) {
    char c = *(it++);
    node = node->children_.find(c) != node->children_.end() ? node->children_.at(c) : nullptr;
  }
  if (!node || !(node->is_value_node_)) {
    return nullptr;
  }
  const auto *leaf_node = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  return leaf_node ? leaf_node->value_.get() : nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<const TrieNode> node = root_;
  std::vector<std::shared_ptr<const TrieNode>> cow_nodes;
  auto it_front = key.begin();
  while (it_front < key.end() && node) {
    cow_nodes.push_back(node);
    char c = *(it_front++);
    node = node->children_.find(c) == node->children_.end() ? nullptr : node->children_.at(c);
  }

  /*create new leaf node*/
  std::shared_ptr<T> shared_value = std::make_shared<T>(std::move(value));
  node = node ? std::make_shared<const TrieNodeWithValue<T>>(node->children_, shared_value)
              : std::make_shared<const TrieNodeWithValue<T>>(shared_value);
  for (auto it_back = key.end() - 1; it_back >= it_front; it_back--) {
    char c = *it_back;
    std::map<char, std::shared_ptr<const TrieNode>> children{{c, node}};
    node = std::make_shared<const TrieNode>(children);
  }

  /*copy-on-write create existing nodes*/
  if (!cow_nodes.empty()) {
    for (auto it = cow_nodes.end() - 1; it >= cow_nodes.begin(); it--) {
      std::unique_ptr<TrieNode> cloned_node = (*it)->Clone();
      size_t idx = std::distance(cow_nodes.begin(), it);
      cloned_node->children_[key[idx]] = node;
      node = std::shared_ptr<const TrieNode>(std::move(cloned_node));
    }
  }

  return Trie(node);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::shared_ptr<const TrieNode> node = root_;
  std::vector<std::shared_ptr<const TrieNode>> cow_nodes;
  auto it = key.begin();
  while (it < key.end() && node) {
    cow_nodes.push_back(node);
    char c = *(it++);
    node = node->children_.find(c) == node->children_.end() ? nullptr : node->children_.at(c);
  }

  // key doesn't exist: return the original trie
  if (!node || !node->is_value_node_) {
    return *this;
  }

  // key exists: update leaf and copy-on-write the existing nodes
  node = node->children_.empty() ? nullptr : std::make_shared<const TrieNode>(node->children_);
  if (!cow_nodes.empty()) {
    for (auto it = cow_nodes.end() - 1; it >= cow_nodes.begin(); it--) {
      std::unique_ptr<TrieNode> cloned_node = (*it)->Clone();
      size_t idx = std::distance(cow_nodes.begin(), it);
      if (!node) {
        cloned_node->children_.erase(key[idx]);
      } else {
        cloned_node->children_[key[idx]] = node;
      }
      node = (cloned_node->children_.empty() && !cloned_node->is_value_node_)
                 ? nullptr
                 : std::shared_ptr<const TrieNode>(std::move(cloned_node));
    }
  }

  return Trie(node);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
