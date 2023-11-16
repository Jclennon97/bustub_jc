//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAtKey(const KeyType &key, ValueType &v, KeyComparator comparator_) -> bool {
  // 叶子节点找到对应key的value
  int l = 0;
  int r = GetSize() - 1;
  while (l < r) {
    int mid = (l + r) >> 1;
    if (comparator_(array_[mid].first, key) > 0 || comparator_(array_[mid].first, key) == 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  int index = l;
  // 如果没找到则return false;
  if (index == GetMaxSize() || comparator_(array_[index].first, key) != 0) {
    return false;
  }
  v = array_[index].second;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator comparator_) -> bool {
  // 叶子节点找到恰好大于这个key的index，即upper_bound
  int l = 0;
  int r = GetSize();
  while (l < r) {
    int mid = (l + r) >> 1;
    if (comparator_(array_[mid].first, key) > 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  int index = l;
  // 如果插入相同的值则return false;
  if (comparator_(array_[index - 1].first, key) == 0) {
    return false;
  }
  auto new_pair = std::make_pair(key, value);
  for (int i = GetSize() - 1; i >= index; i--) {
    array_[i + 1] = array_[i];
  }
  array_[index] = new_pair;
  this->IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Split(B_PLUS_TREE_LEAF_PAGE_TYPE *new_leaf_page) -> KeyType {
  int min_size = GetMinSize();
  int max_size = GetMaxSize();
  for (int i = min_size; i < max_size; i++) {
    new_leaf_page->array_[i - min_size] = this->array_[i];
  }
  // 设置大小
  this->SetSize(min_size);
  new_leaf_page->SetSize(max_size - min_size);
  return new_leaf_page->array_[0].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KetIndex(const KeyType &key, KeyComparator comparator_) -> int {
  int l = 0;
  int r = GetSize() - 1;
  while (l < r) {
    int mid = (l + r) >> 1;
    if (comparator_(array_[mid].first, key) > 0 || comparator_(array_[mid].first, key) == 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ArrayIt(int index) -> const MappingType & { return this->array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveRecord(const KeyType &key, KeyComparator comparator_) -> bool {
  int l = 0;
  int r = GetSize() - 1;
  while (l < r) {
    int mid = (l + r) >> 1;
    if (comparator_(array_[mid].first, key) > 0 || comparator_(array_[mid].first, key) == 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  int index = l;
  if(comparator_(array_[index].first, key) != 0) {
    return false;
  }
  for(int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAll(B_PLUS_TREE_LEAF_PAGE_TYPE* recipient) {
  assert(recipient != nullptr);
  
  int start_index = recipient->GetSize();
  for(int i = 0; i < GetSize(); i++) {
    recipient->array_[start_index + i] = this->array_[i];
  }
  recipient->SetNextPageId(GetNextPageId());
  recipient->IncreaseSize(GetSize());
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFrontTo(B_PLUS_TREE_LEAF_PAGE_TYPE* page) -> KeyType {
  page->array_[page->GetSize()] = this->array_[0];
  for(int i = 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
  page->IncreaseSize(1);
  return array_[0].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveEndTo(B_PLUS_TREE_LEAF_PAGE_TYPE* page) -> KeyType {
  for(int i = page->GetSize() - 1; i >= 0; i--) {
    page->array_[i + 1] = page->array_[i];
  }
  page->array_[0] = array_[GetSize() - 1];
  IncreaseSize(-1);
  page->IncreaseSize(1);
  return page->array_[0].first;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
