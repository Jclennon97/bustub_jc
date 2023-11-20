//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator(page_id_t leaf_page_id, LeafPage *leafPage, int index, BufferPoolManager *bpm);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return itr.leaf_page_id_ == this->leaf_page_id_ && itr.index_ == this->index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(itr == *this); }

 private:
  // add your own private member variables here
  page_id_t leaf_page_id_ = INVALID_PAGE_ID;
  LeafPage *leaf_page_;
  int index_;
  BufferPoolManager *bpm_;
};

}  // namespace bustub
