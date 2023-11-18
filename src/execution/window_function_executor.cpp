#include "execution/executors/window_function_executor.h"
#include <algorithm>
#include <utility>
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  sorted_tuples_.clear();
  while (child_executor_->Next(&tuple, &rid)) {
    TupleRID tuple_rid = {tuple, rid};
    sorted_tuples_.emplace_back(tuple_rid);
  }

  // sort tuples based on order bys first
  std::stable_sort(sorted_tuples_.begin(), sorted_tuples_.end(), [this](const TupleRID &a, const TupleRID &b) {
    for (const auto &item : plan_->window_functions_) {
      auto window_function = item.second;
      for (auto [order_by_type, expr] : window_function.order_by_) {
        auto val_a = expr->Evaluate(&a.first, child_executor_->GetOutputSchema());
        auto val_b = expr->Evaluate(&b.first, child_executor_->GetOutputSchema());
        if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) {
          return order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC;
        }
        if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) {
          return order_by_type == OrderByType::DESC;
        }
      }
      break;
    }
    return false;
  });

  sorted_iterator_ = sorted_tuples_.begin();
  // insert into hash tables
  for (auto [idx, window_function] : plan_->window_functions_) {
    auto hash_table = SimpleWindowHashTable(window_function.type_, !window_function.order_by_.empty());
    window_hts_.emplace_back(hash_table);
  }

  while (sorted_iterator_ != sorted_tuples_.end()) {
    tuple = sorted_iterator_->first;
    for (auto [idx, window_function] : plan_->window_functions_) {
      auto real_idx = window_function.type_ != WindowFunctionType::Rank ? idx : idx - 1;
      window_hts_[real_idx].Insert(MakePartitionKey(&tuple, idx), MakeFunctionValue(&tuple, idx));
    }
    ++sorted_iterator_;
  }
  // create iterator for each window function
  for (auto [idx, window_function] : plan_->window_functions_) {
    auto real_idx = window_function.type_ != WindowFunctionType::Rank ? idx : idx - 1;
    auto iterator = std::make_pair(window_hts_[real_idx].Begin(), 0);
    window_ht_iterators_.emplace_back(iterator);
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_idx_ < sorted_tuples_.size()) {
    std::vector<Value> values;
    for (auto [idx, window_function] : plan_->window_functions_) {
      auto real_idx = window_function.type_ != WindowFunctionType::Rank ? idx : idx - 1;
      auto &ht_iterators = window_ht_iterators_[real_idx].first;
      auto &array_idx = window_ht_iterators_[real_idx].second;
      if (array_idx == ht_iterators.Val().size()) {
        ++ht_iterators;
        array_idx = 0;
      }
      if (window_function.type_ == WindowFunctionType::Rank) {
        values.emplace_back(window_hts_[real_idx].GetOriVal(array_idx));
      }
      values.emplace_back(ht_iterators.Val().at(array_idx));
      ++array_idx;
    }
    *tuple = {values, &plan_->OutputSchema()};
    cur_idx_++;
    return true;
  }
  return false;
}
}  // namespace bustub
