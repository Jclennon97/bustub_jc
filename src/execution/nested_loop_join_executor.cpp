//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/executors/index_scan_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple{};
  RID rid{};
  inner_index_ = 0;
  while (right_executor_->Next(&tuple, &rid)) {
    inner_tuples_.push_back(tuple);
  }
  right_executor_->Init();
  while (left_executor_->Next(&tuple, &rid)) {
    right_executor_->Init();
    outer_tuples_.push_back(tuple);
    not_miss_.push_back(false);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->GetJoinType() == JoinType::INNER) {
    return InnerJoin(tuple, rid);
  }
  return LeftJoin(tuple, rid);
}

auto NestedLoopJoinExecutor::InnerJoin(Tuple *tuple, RID *rid) -> bool {
  if (outer_tuples_.empty() || inner_tuples_.empty()) {
    return false;
  }
  while (!outer_tuples_.empty()) {
    for (; inner_index_ < inner_tuples_.size(); inner_index_++) {
      auto value = plan_->Predicate()->EvaluateJoin(&outer_tuples_.front(), left_executor_->GetOutputSchema(),
                                                    &inner_tuples_[inner_index_], right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        MakeOutputTuple(tuple);
        inner_index_++;
        return true;
      }
    }
    outer_tuples_.pop_front();
    inner_index_ = 0;
  }
  return false;
}

auto NestedLoopJoinExecutor::LeftJoin(Tuple *tuple, RID *rid) -> bool {
  if (inner_tuples_.empty()) {
    while (!outer_tuples_.empty()) {
      outer_miss_tuples_.push_back(outer_tuples_.front());
      outer_tuples_.pop_front();
      MakeMissTuple(tuple);
      outer_miss_tuples_.pop_front();
      return true;
    }
    return false;
  }
  if ((outer_tuples_.empty() && outer_miss_tuples_.empty())) {
    return false;
  }
  while (!outer_tuples_.empty()) {
    for (; inner_index_ < inner_tuples_.size(); inner_index_++) {
      auto value = plan_->Predicate()->EvaluateJoin(&outer_tuples_.front(), left_executor_->GetOutputSchema(),
                                                    &inner_tuples_[inner_index_], right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        MakeOutputTuple(tuple);
        inner_index_++;
        not_miss_.front() = true;
        return true;
      }
    }
    if (!not_miss_.front()) {
      outer_miss_tuples_.push_back(outer_tuples_.front());
    }
    not_miss_.pop_front();
    outer_tuples_.pop_front();
    inner_index_ = 0;
  }
  while (!outer_miss_tuples_.empty()) {
    MakeMissTuple(tuple);
    outer_miss_tuples_.pop_front();
    return true;
  }
  return false;
}

void NestedLoopJoinExecutor::MakeOutputTuple(Tuple *tuple) {
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(outer_tuples_.front().GetValue(&left_executor_->GetOutputSchema(), i));
  }
  for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(inner_tuples_[inner_index_].GetValue(&right_executor_->GetOutputSchema(), i));
  }
  *tuple = Tuple{values, &GetOutputSchema()};
}

void NestedLoopJoinExecutor::MakeMissTuple(Tuple *tuple) {
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(outer_miss_tuples_.front().GetValue(&left_executor_->GetOutputSchema(), i));
  }
  auto inner_columns = right_executor_->GetOutputSchema().GetColumns();
  for (const auto &column : inner_columns) {
    switch (column.GetType()) {
      case TypeId::INVALID:
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INVALID));
        break;
      case TypeId::BOOLEAN:
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::BOOLEAN));
        break;
      case TypeId::TINYINT:
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::TINYINT));
        break;
      case TypeId::SMALLINT:
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::SMALLINT));
        break;
      case TypeId::INTEGER:
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
        break;
      case TypeId::BIGINT:
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::BIGINT));
        break;
      case TypeId::DECIMAL:
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::DECIMAL));
        break;
      case TypeId::VARCHAR:
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::VARCHAR));
        break;
      case TypeId::TIMESTAMP:
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::TIMESTAMP));
        break;
    }
  }
  *tuple = Tuple{values, &GetOutputSchema()};
}

}  // namespace bustub
