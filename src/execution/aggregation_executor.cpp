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

#include "catalog/schema.h"
#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  auto tuple = Tuple();
  auto rid = RID();
  child_executor_->Init();
  aht_.Clear();  // clear hashtable in case aggregation appears as the right table in the nested loop join
  while (child_executor_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  // if table is empty and query doesn't have group bys
  if (aht_.Begin() == aht_.End() && plan_->group_bys_.empty()) {
    aht_.InsertEmpty();
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Schema schema(plan_->OutputSchema());
  if (aht_iterator_ != aht_.End()) {
    std::vector<Value> values(aht_iterator_.Key().group_bys_);
    for (const auto &val : aht_iterator_.Val().aggregates_) {
      values.emplace_back(val);
    }
    *tuple = {values, &schema};
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
