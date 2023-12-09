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
#include <vector>
#include "catalog/schema.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  iterator_ = std::make_unique<TableIterator>(table_heap_->MakeIterator());
  txn_ = exec_ctx_->GetTransaction();
  txn_manager_ = exec_ctx_->GetTransactionManager();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iterator_->IsEnd()) {
    auto [cur_tuple_meta, cur_tuple] = iterator_->GetTuple();
    *rid = iterator_->GetRID();
    // increment the iterator
    iterator_->operator++();
    // check current tuple
    bool tuple_commited = (cur_tuple_meta.ts_ & TXN_START_ID) == 0;
    if (txn_->GetTransactionTempTs() == cur_tuple_meta.ts_ ||
        (tuple_commited && cur_tuple_meta.ts_ <= txn_->GetReadTs())) {
      if (!cur_tuple_meta.is_deleted_) {
        *tuple = cur_tuple;
        // check if the plan node has any filter predicates
        if (plan_->filter_predicate_ == nullptr) {
          return true;
        }
        auto value = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          return true;
        }
      }
    } else {
      auto undo_link = txn_manager_->GetUndoLink(*rid);
      while (undo_link && undo_link->IsValid()) {
        auto undo_log = txn_manager_->GetUndoLog(*undo_link);
        auto next_tuple = ReconstructTuple(&GetOutputSchema(), cur_tuple, cur_tuple_meta, {undo_log});
        if (undo_log.ts_ <= txn_->GetReadTs()) {
          if (next_tuple) {
            *tuple = *next_tuple;
            if (plan_->filter_predicate_ == nullptr) {
              return true;
            }
            auto value = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema());
            if (!value.IsNull() && value.GetAs<bool>()) {
              return true;
            }
          }
          // snapshot tuple is deleted or doesn't satisfy filter condition: skip
          break;
        }
        if (undo_log.ts_ <= txn_manager_->GetWatermark()) {
          // skip previous version if already at watermark boundary
          break;
        }
        undo_link = undo_log.prev_version_;
      }
    }
  }
  return false;
}

}  // namespace bustub
