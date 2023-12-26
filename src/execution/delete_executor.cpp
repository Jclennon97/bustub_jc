//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/config.h"
#include "execution/executors/delete_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  count_ = 0;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  Tuple child_tuple{};
  RID child_rid{};

  if (count_ > 0) {
    return false;
  }

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto tuple_meta = table_info->table_->GetTupleMeta(child_rid);
    tuple_meta.is_deleted_ = true;
    table_info->table_->UpdateTupleMeta(tuple_meta, child_rid);
    TableWriteRecord write_record{table_info->oid_, child_rid, table_info->table_.get()};
    write_record.wtype_ = WType::DELETE;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(write_record);
    for (auto index : table_indexes) {
      index->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs()), child_rid,
          exec_ctx_->GetTransaction());
      IndexWriteRecord index_record{child_rid,   table_info->oid_,  WType::DELETE,
                                    child_tuple, index->index_oid_, exec_ctx_->GetCatalog()};
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(index_record);
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
