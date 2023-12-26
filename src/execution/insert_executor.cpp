//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <memory>

#include "execution/executors/abstract_executor.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  if (!exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(plan_->TableOid())) {
    try {
      exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                             plan_->TableOid());
    } catch (TransactionAbortException &) {
      throw ExecutionException("lockTable failed!");
    }
  }
  count_ = 0;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  auto txn = exec_ctx_->GetTransaction();

  Tuple child_tuple{};
  RID child_rid{};

  if (count_ > 0) {
    return false;
  }

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto tmp_rid = table_info->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, child_tuple,
                                                   exec_ctx_->GetLockManager(), txn, table_info->oid_);
    TableWriteRecord write_record{table_info->oid_, tmp_rid.value(), table_info->table_.get()};
    write_record.wtype_ = WType::INSERT;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(write_record);
    if (tmp_rid.has_value()) {
      for (auto index : table_indexes) {
        index->index_->InsertEntry(
            child_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
            tmp_rid.value(), exec_ctx_->GetTransaction());
        IndexWriteRecord index_record{tmp_rid.value(), table_info->oid_,  WType::INSERT,
                                      child_tuple,     index->index_oid_, exec_ctx_->GetCatalog()};
        exec_ctx_->GetTransaction()->AppendIndexWriteRecord(index_record);
      }
      ++count_;
    }
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(Value{TypeId::INTEGER, count_++});
  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
