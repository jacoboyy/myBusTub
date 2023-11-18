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
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), [this](const TupleRID &a, const TupleRID &b) {
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
  // initialize the hash tables
  for (auto [idx, window_function] : plan_->window_functions_) {
    auto hash_table = SimpleWindowHashTable(window_function.type_, !window_function.order_by_.empty());
    index_map_.insert({idx, window_hts_.size()});
    window_hts_.emplace_back(hash_table);
  }
  // insert into hash tables
  while (sorted_iterator_ != sorted_tuples_.end()) {
    tuple = sorted_iterator_->first;
    for (auto [idx, window_function] : plan_->window_functions_) {
      window_hts_[index_map_.at(idx)].Insert(MakePartitionKey(&tuple, idx), MakeFunctionValue(&tuple, idx));
    }
    ++sorted_iterator_;
  }
  // create iterator
  for (size_t i = 0; i < window_hts_.size(); i++) {
    window_ht_iterators_.emplace_back(window_hts_[0].Begin(), 0);
  }
  for (auto [idx, window_function] : plan_->window_functions_) {
    window_ht_iterators_[index_map_.at(idx)] = {window_hts_[index_map_.at(idx)].Begin(), 0};
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_idx_ < sorted_tuples_.size()) {
    std::vector<Value> values;
    for (size_t i = 0; i < plan_->columns_.size(); i++) {
      auto val = Value();
      values.emplace_back(val);
    }
    for (auto [idx, window_function] : plan_->window_functions_) {
      auto table_idx = index_map_.at(idx);
      auto &ht_iterators = window_ht_iterators_[table_idx].first;
      auto &array_idx = window_ht_iterators_[table_idx].second;
      if (array_idx == ht_iterators.Val().size()) {
        ++ht_iterators;
        array_idx = 0;
      }
      if (window_function.type_ == WindowFunctionType::Rank) {
        values[idx - 1] = window_hts_[table_idx].GetOriVal(array_idx);
      }
      values[idx] = ht_iterators.Val().at(array_idx);
      ++array_idx;
    }
    *tuple = {values, &plan_->OutputSchema()};
    cur_idx_++;
    return true;
  }
  return false;
}
}  // namespace bustub
