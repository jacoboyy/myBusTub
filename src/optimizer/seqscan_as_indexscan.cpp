#include <memory>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan.filter_predicate_ != nullptr) {
      const auto *table_info = catalog_.GetTable(seq_scan.GetTableOid());
      const auto indices = catalog_.GetTableIndexes(table_info->name_);
      if (const auto *comparison_expr = dynamic_cast<const ComparisonExpression *>(seq_scan.filter_predicate_.get());
          comparison_expr != nullptr) {
        if (const auto *column_value_expr =
                dynamic_cast<const ColumnValueExpression *>(comparison_expr->GetChildAt(0).get());
            column_value_expr != nullptr) {
          if (auto *constant_value_expr = dynamic_cast<ConstantValueExpression *>(comparison_expr->GetChildAt(1).get());
              constant_value_expr != nullptr) {
            auto temp = *constant_value_expr;
            // check if there are any matching index on the column
            for (const auto *index : indices) {
              const auto &columns = index->key_schema_.GetColumns();
              if (columns.size() == 1 &&
                  columns[0].GetName() == table_info->schema_.GetColumn(column_value_expr->GetColIdx()).GetName()) {
                return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                           index->index_oid_, seq_scan.filter_predicate_,
                                                           constant_value_expr);
              }
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
