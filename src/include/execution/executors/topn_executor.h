//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct CompareRule {
  CompareRule(const TopNPlanNode *plan, const Schema &schema) : plan_(plan), schema_(schema) {}
  auto operator()(const Tuple &a, const Tuple &b) const -> bool {
    for (const auto &pair : plan_->GetOrderBy()) {
      auto order_type = pair.first;
      auto value_a = pair.second->Evaluate(&a, schema_);
      auto value_b = pair.second->Evaluate(&b, schema_);
      if (value_a.CompareNotEquals(value_b) == CmpBool::CmpTrue) {
        if (order_type == OrderByType::DESC) {
          if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
            return true;
          }
          if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
            return false;
          }
        }
        if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
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
  }
  const TopNPlanNode *plan_;
  const Schema &schema_;
};

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::deque<Tuple> top_entries_;
};
}  // namespace bustub
