#include "execution/executors/sort_executor.h"
#include <utility>
#include "binder/bound_order_by.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  sorted_tuples_.clear();
  while (child_executor_->Next(&tuple, &rid)) {
    TupleRID tuple_rid = {tuple, rid};
    sorted_tuples_.emplace_back(tuple_rid);
  }

  // customised sort with lambda function
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), [this](const TupleRID &a, const TupleRID &b) {
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
      auto val_a = expr->Evaluate(&a.first, child_executor_->GetOutputSchema());
      auto val_b = expr->Evaluate(&b.first, child_executor_->GetOutputSchema());
      if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) {
        return order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC;
      }
      if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) {
        return order_by_type == OrderByType::DESC;
      }
    }
    return false;
  });
  sorted_iterator_ = sorted_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_iterator_ != sorted_tuples_.end()) {
    *tuple = sorted_iterator_->first;
    *rid = sorted_iterator_->second;
    ++sorted_iterator_;
    return true;
  }
  return false;
}

}  // namespace bustub