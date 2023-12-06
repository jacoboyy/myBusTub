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
  auto filter_expr = plan_->filter_predicate_;
  while (!iterator_->IsEnd()) {
    auto cur_tuple_meta = iterator_->GetTuple().first;
    auto cur_tuple = iterator_->GetTuple().second;
    *rid = iterator_->GetRID();
    // increment the iterator
    iterator_->operator++();

    // check current tuple
    bool tuple_commited = (cur_tuple_meta.ts_ & TXN_START_ID) == 0;
    if (txn_->GetTransactionTempTs() == cur_tuple_meta.ts_ ||
        (tuple_commited && cur_tuple_meta.ts_ <= txn_->GetReadTs())) {
      if (!cur_tuple_meta.is_deleted_) {
        *tuple = cur_tuple;
        return true;
        // check if the plan node has any filter predicates [Not needed in Q4]
        // if (filter_expr == nullptr) {
        //   return true;
        // }
        // auto value = filter_expr->Evaluate(tuple, GetOutputSchema());
        // if (!value.IsNull() && value.GetAs<bool>()) {
        //   return true;
        // }
      }
    } else {
      auto undo_link_opt = txn_manager_->GetUndoLink(*rid);
      if (undo_link_opt.has_value()) {
        auto undo_link = undo_link_opt.value();
        std::vector<UndoLog> undo_logs;
        while (undo_link.IsValid()) {
          auto undo_log = txn_manager_->GetUndoLog(undo_link);
          undo_logs.emplace_back(undo_log);
          auto next_tuple_opt = ReconstructTuple(&GetOutputSchema(), cur_tuple, cur_tuple_meta, undo_logs);
          if (undo_log.ts_ <= txn_->GetReadTs()) {
            if (next_tuple_opt.has_value()) {
              *tuple = next_tuple_opt.value();
              return true;
            }
            break;
          }
          undo_link = undo_log.prev_version_;
        }
      }
    }
  }
  return false;
}

}  // namespace bustub
