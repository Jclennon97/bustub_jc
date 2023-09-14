#include "primer/trie.h"
#include <cstddef>
#include <memory>
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  std::shared_ptr<const TrieNode> cur_node = root_;
  std::size_t key_size = key.size();
  decltype(key_size) id = 0;
  while (id < key_size && cur_node) {
    auto ch = key.at(id++);
    cur_node = cur_node->children_.find(ch) == cur_node->children_.end() ? nullptr : cur_node->children_.at(ch);
  }
  if (cur_node == nullptr) {
    return nullptr;
  }
  auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(cur_node.get());
  return value_node == nullptr ? nullptr : value_node->value_.get();
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  std::shared_ptr<T> value_ptr = std::make_shared<T>(std::move(value));
  std::shared_ptr<const TrieNode> cur_node = root_;
  std::stack<std::unique_ptr<TrieNode>> st;
  std::size_t key_size = key.size();
  decltype(key_size) id = 0;
  while (id < key_size) {
    auto ch = key.at(id++);
    if (cur_node != nullptr) {
      st.push(cur_node->Clone());
      cur_node = cur_node->children_.find(ch) == cur_node->children_.end() ? nullptr : cur_node->children_.at(ch);
    } else {
      st.push(std::make_unique<TrieNode>());
    }
  }
  std::unique_ptr<TrieNode> node = std::make_unique<TrieNodeWithValue<T>>(value_ptr);
  if (cur_node.use_count() != 0) {
    node->children_ = cur_node->children_;
  }
  id = key_size - 1;
  while (!st.empty()) {
    char ch = key[id--];
    st.top()->children_.erase(ch);
    st.top()->children_[ch] = std::shared_ptr<const TrieNode>(std::move(node));
    node = std::move(st.top());
    st.pop();
  }
  auto trie = Trie(std::shared_ptr<const TrieNode>(std::move(node)));
  return trie;
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  std::shared_ptr<const TrieNode> cur_node = root_;
  std::stack<std::unique_ptr<TrieNode>> st;
  size_t key_size = key.size();
  decltype(key_size) id = 0;
  while (id < key_size && cur_node != nullptr) {
    char ch = key[id++];
    st.push(cur_node->Clone());
    cur_node = cur_node->children_.find(ch) == cur_node->children_.end() ? nullptr : cur_node->children_.at(ch);
  }
  if (cur_node == nullptr || !cur_node->is_value_node_) {
    return Trie(root_);
  }
  std::unique_ptr<TrieNode> node = std::make_unique<TrieNode>(cur_node->children_);
  id = key_size - 1;
  while (!st.empty()) {
    char ch = key[id--];
    if (node->children_.empty() && !node->is_value_node_) {
      st.top()->children_.erase(ch);
    } else {
      st.top()->children_.erase(ch);
      st.top()->children_.emplace(ch, std::shared_ptr<const TrieNode>(std::move(node)));
    }
    node = std::move(st.top());
    st.pop();
  }
  auto trie = Trie(std::shared_ptr<const TrieNode>(std::move(node)));
  return trie;
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
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
