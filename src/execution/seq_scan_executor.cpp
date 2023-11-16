//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  iterator_ = std::make_unique<TableIterator>(table_heap_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto filter_expr = plan_->filter_predicate_;
  while (!iterator_->IsEnd()) {
    auto pair = iterator_->GetTuple();
    *rid = iterator_->GetRID();
    // increment the iterator
    iterator_->operator++();
    // check if the tuple is deleted
    if (!pair.first.is_deleted_) {
      *tuple = pair.second;
      // check if the plan node has any filter predicates
      if (filter_expr == nullptr) {
        return true;
      }
      auto value = filter_expr->Evaluate(tuple, GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
