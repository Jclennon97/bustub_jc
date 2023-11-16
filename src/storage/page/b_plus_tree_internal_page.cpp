//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "execution/executors/init_check_executor.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  // ???
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  auto key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetKeyIndex(const KeyType &key, KeyComparator comparator_) const -> int {
  // 中间节点大于key的前一个index，即upper_bound - 1
  int l = 1;
  int r = GetSize();
  while (l < r) {
    int mid = (l + r) >> 1;
    if (comparator_(array_[mid].first, key) > 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return l - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(B_PLUS_TREE_INTERNAL_PAGE_TYPE *new_page) -> KeyType {
  int min_size = GetMinSize();
  for (int i = min_size; i <= GetMaxSize(); i++) {
    new_page->array_[i - min_size] = this->array_[i];
  }
  new_page->SetSize(GetMaxSize() - min_size + 1);
  this->SetSize(min_size);
  return new_page->KeyAt(0);
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CreatNewRoot(const KeyType &key, page_id_t left_id, page_id_t right_id) {
  MappingType first_pair = std::make_pair(key, left_id);
  MappingType second_pair = std::make_pair(key, right_id);
  array_[0] = first_pair;
  array_[1] = second_pair;
  SetSize(2);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, page_id_t right_id, KeyComparator comparator_) {
  // 中间节点找到大于key的index
  int l = 1;
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
  for (int i = GetSize() - 1; i >= index; i--) {
    array_[i + 1] = array_[i];
  }
  array_[index] = std::make_pair(key, right_id);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  int i;
  for(i = 0; i < GetSize(); i++) {
    if(array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAll(B_PLUS_TREE_INTERNAL_PAGE_TYPE* recipient, int index, B_PLUS_TREE_INTERNAL_PAGE_TYPE* parent_page) {
  int start_index = recipient->GetSize();
  SetKeyAt(0, parent_page->KeyAt(index));
  for(int i = 0; i < GetSize(); i++) {
    recipient->array_[start_index + i] = array_[i];
  }
  recipient->IncreaseSize(GetSize());
  assert(recipient->GetSize() <= GetMaxSize());
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for(int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFrontTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE* page, const KeyType &parent_key) -> KeyType {
  page->array_[page->GetSize()] = array_[0];
  page->array_[page->GetSize()].first = parent_key;
  for(int i = 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
  page->IncreaseSize(1);
  return array_[0].first;
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveEndTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE* page, const KeyType &parent_key) -> KeyType {
  for(int i = page->GetSize() - 1; i >= 0; i--) {
    page->array_[i + 1] = page->array_[i];
  }
  page->array_[0] = array_[GetSize() - 1];
  page->array_[1].first = parent_key;
  IncreaseSize(-1);
  page->IncreaseSize(1);
  return page->array_[0].first;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
