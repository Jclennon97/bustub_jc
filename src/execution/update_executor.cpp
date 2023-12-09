//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  count_ = 0;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (count_ > 0) {
    return false;
  }
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  Tuple child_tuple{};

  while (child_executor_->Next(&child_tuple, rid)) {
    auto tuple_meta_delete = table_info_->table_->GetTupleMeta(*rid);
    tuple_meta_delete.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta_delete, *rid);

    for (auto index : table_indexes) {
      auto index_key = child_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(index_key, *rid, exec_ctx_->GetTransaction());
    }

    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    child_tuple = Tuple{values, &child_executor_->GetOutputSchema()};

    auto tmp_rid = table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, child_tuple);

    if (tmp_rid.has_value()) {
      for (auto index : table_indexes) {
        index->index_->InsertEntry(
            child_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
            tmp_rid.value(), exec_ctx_->GetTransaction());
      }
    }
    ++count_;
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(Value{TypeId::INTEGER, count_++});
  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
