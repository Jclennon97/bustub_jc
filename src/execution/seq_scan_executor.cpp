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
  table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_->IsEnd()) {
    return false;
  }
  TableIterator *raw_pointer = table_iterator_.get();
  while (table_iterator_->GetTuple().first.is_deleted_) {
    ++(*raw_pointer);
    if (table_iterator_->IsEnd()) {
      return false;
    }
  }
  *tuple = table_iterator_->GetTuple().second;
  *rid = table_iterator_->GetRID();
  ++(*raw_pointer);
  return true;
}

}  // namespace bustub
