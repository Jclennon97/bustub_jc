//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  is_empty_ = true;
  empty_output_ = false;

  while (child_->Next(&child_tuple, &child_rid)) {
    aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
    is_empty_ = false;
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_empty_ && !empty_output_) {
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (uint32_t i = 0; i < plan_->GetAggregates().size(); i++) {
      if (plan_->GetAggregateTypes()[i] == AggregationType::CountStarAggregate) {
        values.push_back(ValueFactory::GetIntegerValue(0));
      } else {
        values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
    }
    *tuple = Tuple(values, &GetOutputSchema());

    empty_output_ = true;
    return true;
  }
  if (empty_output_ || aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  for (const auto &group_by : aht_iterator_.Key().group_bys_) {
    values.push_back(group_by);
  }
  for (const auto &aggr : aht_iterator_.Val().aggregates_) {
    values.push_back(aggr);
  }
  *tuple = Tuple(values, &GetOutputSchema());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
