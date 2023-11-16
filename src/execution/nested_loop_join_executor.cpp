//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstddef>
#include <optional>
#include <utility>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()),
      join_type_(plan_->join_type_) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_tuple_ = Tuple();
  right_tuple_ = Tuple();
  started_ = false;
  finished_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!started_) {
    started_ = true;
    // left table is empty: exit immediately
    if (!left_executor_->Next(&left_tuple_, rid)) {
      finished_ = true;
      return false;
    }
  }

  while (!finished_) {
    // haven't finished matching the current left tuple
    while (right_executor_->Next(&right_tuple_, rid)) {
      auto matched =
          plan_->predicate_->EvaluateJoin(&left_tuple_, left_schema_, &right_tuple_, right_schema_).GetAs<bool>();
      if (matched) {
        std::vector<Value> values;
        // concatenate values of left tuples and right tuples
        for (uint32_t idx = 0; idx < left_schema_.GetColumnCount(); idx++) {
          values.emplace_back(left_tuple_.GetValue(&left_schema_, idx));
        }
        for (uint32_t idx = 0; idx < right_schema_.GetColumnCount(); idx++) {
          values.emplace_back(right_tuple_.GetValue(&right_schema_, idx));
        }

        *tuple = {values, &GetOutputSchema()};
        left_matched_ = true;
        return true;
      }
    }
    // finished enumerating all right tuples for the current left tuple
    // left_tuple is not matched in left join
    right_executor_->Init();  // wrap around the cursor on the right table
    if (!left_matched_ && join_type_ == JoinType::LEFT) {
      std::vector<Value> values;
      // concatenate values of left tuples and right tuples
      for (uint32_t idx = 0; idx < left_schema_.GetColumnCount(); idx++) {
        values.emplace_back(left_tuple_.GetValue(&left_schema_, idx));
      }
      for (auto &col : right_schema_.GetColumns()) {
        values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      *tuple = {values, &GetOutputSchema()};
      if (!left_executor_->Next(&left_tuple_, rid)) {
        finished_ = true;
      }
      return true;
    }
    left_matched_ = false;
    // get the next left tuple
    if (!left_executor_->Next(&left_tuple_, rid)) {
      finished_ = true;
      break;
    }
  }
  return false;
}

}  // namespace bustub
