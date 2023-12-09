#include <algorithm>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    std::vector<AbstractExpressionRef> left_expressions;
    std::vector<AbstractExpressionRef> right_expressions;

    // Check if expr is equal condition where one is for the left table, and one is for the right table.
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            // Ensure both exprs have tuple_id == 0
            auto left_expr_tuple_0 =
                std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
            auto right_expr_tuple_0 =
                std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());
            if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
              left_expressions.push_back(std::move(left_expr_tuple_0));
              right_expressions.push_back(std::move(right_expr_tuple_0));
            }
            if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
              left_expressions.push_back(std::move(right_expr_tuple_0));
              right_expressions.push_back(std::move(left_expr_tuple_0));
            }

            // Now it's in form of <column_expr> = <column_expr>. Let's check if one of them is from the left table, and
            // the other is from the right table.

            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                      nlj_plan.GetRightPlan(), left_expressions, right_expressions,
                                                      nlj_plan.GetJoinType());
          }
        }
      }
    }
    if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->logic_type_ == LogicType::And) {
        if (const auto *left_out_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[0].get());
            left_out_expr != nullptr) {
          if (const auto *right_out_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[1].get());
              right_out_expr != nullptr) {
            if (left_out_expr->comp_type_ == ComparisonType::Equal &&
                right_out_expr->comp_type_ == ComparisonType::Equal) {
              if (const auto *left_left_expr =
                      dynamic_cast<const ColumnValueExpression *>(left_out_expr->children_[0].get());
                  left_left_expr != nullptr) {
                if (const auto *left_right_expr =
                        dynamic_cast<const ColumnValueExpression *>(left_out_expr->children_[1].get());
                    left_right_expr != nullptr) {
                  auto left_expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, left_left_expr->GetColIdx(),
                                                                                   left_left_expr->GetReturnType());
                  auto right_expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, left_right_expr->GetColIdx(),
                                                                                    left_right_expr->GetReturnType());
                  if (left_left_expr->GetTupleIdx() == 0 && left_right_expr->GetTupleIdx() == 1) {
                    left_expressions.push_back(std::move(left_expr_tuple_0));
                    right_expressions.push_back(std::move(right_expr_tuple_0));
                  }
                  if (left_left_expr->GetTupleIdx() == 1 && left_right_expr->GetTupleIdx() == 0) {
                    left_expressions.push_back(std::move(right_expr_tuple_0));
                    right_expressions.push_back(std::move(left_expr_tuple_0));
                  }
                }
              }

              if (const auto *right_left_expr =
                      dynamic_cast<const ColumnValueExpression *>(right_out_expr->children_[0].get());
                  right_left_expr != nullptr) {
                if (const auto *right_right_expr =
                        dynamic_cast<const ColumnValueExpression *>(right_out_expr->children_[1].get());
                    right_right_expr != nullptr) {
                  auto left_expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, right_left_expr->GetColIdx(),
                                                                                   right_left_expr->GetReturnType());
                  auto right_expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, right_right_expr->GetColIdx(),
                                                                                    right_right_expr->GetReturnType());
                  if (right_left_expr->GetTupleIdx() == 0 && right_right_expr->GetTupleIdx() == 1) {
                    left_expressions.push_back(std::move(left_expr_tuple_0));
                    right_expressions.push_back(std::move(right_expr_tuple_0));
                  }
                  if (right_left_expr->GetTupleIdx() == 1 && right_right_expr->GetTupleIdx() == 0) {
                    left_expressions.push_back(std::move(right_expr_tuple_0));
                    right_expressions.push_back(std::move(left_expr_tuple_0));
                  }
                }
              }

              return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                        nlj_plan.GetRightPlan(), left_expressions, right_expressions,
                                                        nlj_plan.GetJoinType());
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
