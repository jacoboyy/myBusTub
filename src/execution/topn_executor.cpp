#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), limit_(plan->GetN()) {}

void TopNExecutor::Init() {
  Tuple tuple;
  RID rid;
  // clear meta data
  idx_ = 0;
  sorted_tuples_.clear();
  child_executor_->Init();
  // customised sort with lambda function
  while (child_executor_->Next(&tuple, &rid)) {
    TupleRID tuple_rid = {tuple, rid};
    sorted_tuples_.emplace_back(tuple_rid);
  }
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

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_iterator_ != sorted_tuples_.end() && idx_ < limit_) {
    *tuple = sorted_iterator_->first;
    *rid = sorted_iterator_->second;
    ++sorted_iterator_;
    ++idx_;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return limit_; };

}  // namespace bustub
