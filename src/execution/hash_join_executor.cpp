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
#include <cstddef>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()),
      join_type_(plan->join_type_) {
  if (join_type_ != JoinType::LEFT && join_type_ != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  right_idx_ = -1;
  right_hashmap_.clear();
  // build hash table
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    auto key = MakeRightJoinKey(&tuple);
    right_hashmap_[key].emplace_back(tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  JoinKey cur_key;
  RID left_rid;
  if (right_idx_ != static_cast<size_t>(-1)) {
    cur_key = MakeLeftJoinKey(&left_tuple_);
  }
  if (right_idx_ == static_cast<size_t>(-1) || right_hashmap_.find(cur_key) == right_hashmap_.end() ||
      right_idx_ == right_hashmap_[cur_key].size()) {
    while (true) {
      if (left_executor_->Next(&left_tuple_, &left_rid)) {
        cur_key = MakeLeftJoinKey(&left_tuple_);
        if (right_hashmap_.find(cur_key) != right_hashmap_.end()) {
          // found a matching right tuple for the current left tuple
          right_idx_ = 0;
          break;
        }
        if (join_type_ == JoinType::LEFT) {
          // no matching tuple for current left
          std::vector<Value> values;
          // concatenate values of left tuples and right tuples
          for (uint32_t idx = 0; idx < left_schema_.GetColumnCount(); idx++) {
            values.emplace_back(left_tuple_.GetValue(&left_schema_, idx));
          }
          for (auto &col : right_schema_.GetColumns()) {
            values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
          }
          *tuple = {values, &GetOutputSchema()};
          return true;
        }
      } else {
        return false;
      }
    }
  }
  std::vector<Value> values;
  // concatenate values of left tuples and right tuples
  for (uint32_t idx = 0; idx < left_schema_.GetColumnCount(); idx++) {
    values.emplace_back(left_tuple_.GetValue(&left_schema_, idx));
  }
  auto right_tuple = right_hashmap_[cur_key].at(right_idx_);
  for (uint32_t idx = 0; idx < right_schema_.GetColumnCount(); idx++) {
    values.emplace_back(right_tuple.GetValue(&right_schema_, idx));
  }
  *tuple = {values, &GetOutputSchema()};
  ++right_idx_;
  return true;
}

}  // namespace bustub
