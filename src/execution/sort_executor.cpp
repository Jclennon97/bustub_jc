#include "execution/executors/sort_executor.h"
#include <algorithm>
#include "storage/table/tuple.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple{};
  RID rid{};
  while (child_executor_->Next(&child_tuple, &rid)) {
    sort_tuples_.push_back(child_tuple);
  }
  std::sort(sort_tuples_.begin(), sort_tuples_.end(), [&](const Tuple &a, const Tuple &b) {
    for (const auto &pair : plan_->GetOrderBy()) {
      auto order_type = pair.first;
      auto value_a = pair.second->Evaluate(&a, child_executor_->GetOutputSchema());
      auto value_b = pair.second->Evaluate(&b, child_executor_->GetOutputSchema());
      if (value_a.CompareNotEquals(value_b) == CmpBool::CmpTrue) {
        if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
          if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
            return true;
          }
          if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
            return false;
          }
        }
        if (order_type == OrderByType::DESC) {
          if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
            return true;
          }
          if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
            return false;
          }
        }
      }
    }
    return false;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!sort_tuples_.empty()) {
    *tuple = sort_tuples_.front();
    sort_tuples_.pop_front();
    return true;
  }
  return false;
}

}  // namespace bustub
