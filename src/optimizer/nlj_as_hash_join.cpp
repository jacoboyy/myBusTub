#include <algorithm>
#include <memory>
#include <utility>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
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
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (plan->GetType() == PlanType::NestedLoopJoin) {
    auto &nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(*optimized_plan);
    std::vector<AbstractExpressionRef> left_exprs;
    std::vector<AbstractExpressionRef> right_exprs;
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get()) != nullptr &&
            dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get()) != nullptr) {
          for (const auto &child_expr : expr->children_) {
            if (dynamic_cast<ColumnValueExpression *>(child_expr.get())->GetTupleIdx() == 0) {
              left_exprs.emplace_back(child_expr);
            } else {
              right_exprs.emplace_back(child_expr);
            }
          }
          return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                    nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                    std::move(right_exprs), nlj_plan.GetJoinType());
        }
      }
    }
    if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->logic_type_ == LogicType::And) {
        std::vector<AbstractExpressionRef> left_exprs;
        std::vector<AbstractExpressionRef> right_exprs;
        // recusive update
        auto left_plan = OptimizeNLJAsHashJoin(std::make_shared<NestedLoopJoinPlanNode>(
            nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
            nlj_plan.predicate_->GetChildAt(0), nlj_plan.GetJoinType()));
        auto right_plan = OptimizeNLJAsHashJoin(std::make_shared<NestedLoopJoinPlanNode>(
            nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
            nlj_plan.predicate_->GetChildAt(1), nlj_plan.GetJoinType()));
        if (left_plan->GetType() == PlanType::HashJoin && right_plan->GetType() == PlanType::HashJoin) {
          auto left_hash_plan = dynamic_cast<const HashJoinPlanNode &>(*left_plan);
          auto right_hash_plan = dynamic_cast<const HashJoinPlanNode &>(*right_plan);
          for (auto &left_expr : left_hash_plan.LeftJoinKeyExpressions()) {
            left_exprs.emplace_back(left_expr);
          }
          for (auto &left_expr : right_hash_plan.LeftJoinKeyExpressions()) {
            left_exprs.emplace_back(left_expr);
          }
          for (auto &right_expr : left_hash_plan.RightJoinKeyExpressions()) {
            right_exprs.emplace_back(right_expr);
          }
          for (auto &right_expr : right_hash_plan.RightJoinKeyExpressions()) {
            right_exprs.emplace_back(right_expr);
          }
          return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                    nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                    std::move(right_exprs), nlj_plan.GetJoinType());
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
