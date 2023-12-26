//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;
  auto NextHelper(Tuple *tuple, RID *rid) -> bool;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  std::unique_ptr<TableIterator> table_iterator_;

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  void LockRow(RID *rid) {
    auto txn = exec_ctx_->GetTransaction();
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    if (exec_ctx_->IsDelete()) {
      try {
        exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info->oid_, *rid);
      } catch (TransactionAbortException &) {
        throw ExecutionException("lockRow failed!");
      }
    } else {
      if (!txn->IsRowExclusiveLocked(table_info->oid_, *rid) &&
          txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        try {
          exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, table_info->oid_, *rid);
        } catch (TransactionAbortException &) {
          throw ExecutionException("lockRow failed!");
        }
      }
    }
  }
  void UnlockSRow() {
    auto txn = exec_ctx_->GetTransaction();
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    if (!exec_ctx_->IsDelete() && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      try {
        if (txn->GetSharedRowLockSet()->find(table_info->oid_) != txn->GetSharedRowLockSet()->end()) {
          const auto row_lock_set = txn->GetSharedRowLockSet()->at(table_info->oid_);
          for (const RID &row_rid : row_lock_set) {
            exec_ctx_->GetLockManager()->UnlockRow(txn, table_info->oid_, row_rid, true);
          }
        }
      } catch (TransactionAbortException &) {
        throw ExecutionException("unlockRow failed!");
      }
    }
  }
};
}  // namespace bustub
