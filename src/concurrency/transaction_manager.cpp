//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto meta_data = item.table_heap_->GetTupleMeta(item.rid_);
    if (item.wtype_ == WType::DELETE) {
      meta_data.is_deleted_ = false;
    } else if (item.wtype_ == WType::INSERT) {
      meta_data.is_deleted_ = true;
    }
    item.table_heap_->UpdateTupleMeta(meta_data, item.rid_);
    write_set->pop_back();
  }

  auto index_write_set = txn->GetIndexWriteSet();
  while (!index_write_set->empty()) {
    auto &item = index_write_set->back();
    auto *catalog = item.catalog_;
    // Metadata identifying the table that should be deleted from.
    TableInfo *table_info = catalog->GetTable(item.table_oid_);
    IndexInfo *index_info = catalog->GetIndex(item.index_oid_);
    auto new_key = item.tuple_.KeyFromTuple(table_info->schema_, *(index_info->index_->GetKeySchema()),
                                            index_info->index_->GetKeyAttrs());
    if (item.wtype_ == WType::DELETE) {
      index_info->index_->InsertEntry(new_key, item.rid_, txn);
    } else if (item.wtype_ == WType::INSERT) {
      index_info->index_->DeleteEntry(new_key, item.rid_, txn);
    } else if (item.wtype_ == WType::UPDATE) {
      // Delete the new key and insert the old key
      index_info->index_->DeleteEntry(new_key, item.rid_, txn);
      auto old_key = item.old_tuple_.KeyFromTuple(table_info->schema_, *(index_info->index_->GetKeySchema()),
                                                  index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(old_key, item.rid_, txn);
    }
    index_write_set->pop_back();
  }

  ReleaseLocks(txn);
  // lock_manager_->DeleteAbortRequest(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
