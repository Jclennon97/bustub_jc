//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <vector>
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  Tuple tuple{};
  RID rid{};
  while (right_child_->Next(&tuple, &rid)) {
    ht_.emplace(MakeRightHashJoinKey(&tuple), tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!tmp_.empty()) {
    *tuple = tmp_.front();
    tmp_.pop_front();
    return true;
  }
  Tuple left_tuple{};
  while (left_child_->Next(&left_tuple, rid)) {
    auto pair_it = ht_.equal_range(MakeLeftHashJoinKey(&left_tuple));
    if (pair_it.first == pair_it.second) {
      if (plan_->GetJoinType() == JoinType::INNER) {
        continue;
      }
      *tuple = MakeMissOutPutTuple(left_tuple);
      return true;
    }
    for (auto it = pair_it.first; it != pair_it.second; ++it) {
      *tuple = MakeOutPutTuple(left_tuple, it->second);
      tmp_.push_back(*tuple);
    }
    *tuple = tmp_.front();
    tmp_.pop_front();
    return true;
  }
  return false;
}

auto HashJoinExecutor::MakeOutPutTuple(const Tuple &left_tuple, const Tuple &right_tuple) -> Tuple {
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
  }
  for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
  }
  return Tuple{values, &GetOutputSchema()};
}

auto HashJoinExecutor::MakeMissOutPutTuple(const Tuple &left_tuple) -> Tuple {
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
  }
  auto inner_columns = right_child_->GetOutputSchema().GetColumns();
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
  return Tuple{values, &GetOutputSchema()};
}

}  // namespace bustub
