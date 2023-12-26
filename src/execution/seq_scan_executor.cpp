//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto txn = exec_ctx_->GetTransaction();
  if (exec_ctx_->IsDelete()) {
    try {
      exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info->oid_);
    } catch (TransactionAbortException &) {
      throw ExecutionException("lockTable failed!");
    }
  } else {
    if (!txn->IsTableIntentionExclusiveLocked(table_info->oid_) && !txn->IsTableExclusiveLocked(table_info->oid_)) {
      if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        try {
          exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, table_info->oid_);
        } catch (TransactionAbortException &) {
          throw ExecutionException("lockTable failed!");
        }
      }
    }
  }
  table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_->IsEnd()) {
    UnlockSRow();
    return false;
  }
  TableIterator *raw_pointer = table_iterator_.get();
  while (NextHelper(tuple, rid)) {
    ++(*raw_pointer);
    if (table_iterator_->IsEnd()) {
      UnlockSRow();
      return false;
    }
  }
  *tuple = table_iterator_->GetTuple().second;
  *rid = table_iterator_->GetRID();
  ++(*raw_pointer);
  LockRow(rid);
  return true;
}

auto SeqScanExecutor::NextHelper(Tuple *tuple, RID *rid) -> bool {
  // auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  // auto txn = exec_ctx_->GetTransaction();
  auto tuple_pair = table_iterator_->GetTuple();
  *rid = table_iterator_->GetRID();
  LockRow(rid);
  // if(tuple_pair.first.is_deleted_) {
  //   exec_ctx_->GetLockManager()->UnlockRow(txn, table_info->oid_, *rid , true);
  //   return true;
  // }
  return tuple_pair.first.is_deleted_;
}

}  // namespace bustub
