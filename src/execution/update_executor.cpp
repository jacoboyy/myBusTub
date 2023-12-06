//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "execution/executor_context.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  table_name_ = table_info_->name_;
  table_heap_ = table_info_->table_.get();
  indices_ = catalog->GetTableIndexes(table_name_);
  child_executor_->Init();
  finished_ = false;
  txn_ = exec_ctx_->GetTransaction();
  txn_manager_ = exec_ctx_->GetTransactionManager();
  // store all tuples from child executor locally to avoid halloween problems
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuple_rid_pairs_.emplace_back(tuple, rid);
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  int cnt = 0;
  std::vector<RID> modified_rids;
  for (auto &tuple_rid_pair : tuple_rid_pairs_) {
    *tuple = tuple_rid_pair.first;
    *rid = tuple_rid_pair.second;
    auto old_tuple_meta = table_heap_->GetTupleMeta(*rid);
    if (IsWriteWriteConflict(old_tuple_meta.ts_, txn_)) {
      finished_ = true;
      txn_->SetTainted();
      throw ExecutionException("write-write conflict detected!");
    }

    // prepare meta data for update
    auto new_tuple_meta = TupleMeta{txn_->GetTransactionTempTs(), false};
    auto old_undo_link = txn_manager_->GetUndoLink(*rid);
    std::vector<bool> modified_fields;
    std::vector<Column> undo_log_columns;
    std::vector<Value> undo_log_values;

    // create new tuple
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    for (auto &e : plan_->target_expressions_) {
      new_values.emplace_back(e->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    auto new_tuple = Tuple(new_values, &child_executor_->GetOutputSchema());
    // skip if new tuple is identical to the previous one
    if (IsTupleContentEqual(new_tuple, *tuple)) {
      continue;
    }
    // check current tuple timestamp
    bool tuple_commited = (old_tuple_meta.ts_ & TXN_START_ID) == 0;
    if (tuple_commited) {
      for (size_t idx = 0; idx < plan_->target_expressions_.size(); ++idx) {
        auto new_value = new_tuple.GetValue(&child_executor_->GetOutputSchema(), idx);
        auto ori_value = tuple->GetValue(&child_executor_->GetOutputSchema(), idx);
        if (!new_value.CompareExactlyEquals(ori_value)) {
          undo_log_columns.emplace_back(child_executor_->GetOutputSchema().GetColumn(idx));
          undo_log_values.emplace_back(ori_value);
          modified_fields.emplace_back(true);
        } else {
          modified_fields.emplace_back(false);
        }
      }
      auto undo_log_schema = Schema(undo_log_columns);
      auto undo_log = UndoLog{false, modified_fields, Tuple(undo_log_values, &undo_log_schema), old_tuple_meta.ts_};
      if (old_undo_link && old_undo_link->IsValid()) {
        undo_log.prev_version_ = *old_undo_link;
      }
      // store undo log in current transaction
      auto new_undo_link = txn_->AppendUndoLog(undo_log);
      // update version link of tuple to point to the new undo log
      txn_manager_->UpdateUndoLink(*rid, new_undo_link);
    } else if (old_undo_link && old_undo_link->IsValid()) {
      // reconstruct tuple before the current transaction
      auto previous_undo_log = txn_manager_->GetUndoLog(*old_undo_link);
      auto previous_tuple =
          ReconstructTuple(&child_executor_->GetOutputSchema(), *tuple, old_tuple_meta, {previous_undo_log});
      if (previous_tuple) {
        // create new undo log
        for (size_t idx = 0; idx < plan_->target_expressions_.size(); ++idx) {
          auto new_value = new_tuple.GetValue(&child_executor_->GetOutputSchema(), idx);
          auto ori_value = previous_tuple->GetValue(&child_executor_->GetOutputSchema(), idx);
          // already modified field in the undo log will not be removed
          if (!new_value.CompareExactlyEquals(ori_value) || previous_undo_log.modified_fields_[idx]) {
            undo_log_columns.emplace_back(child_executor_->GetOutputSchema().GetColumn(idx));
            undo_log_values.emplace_back(ori_value);
            modified_fields.emplace_back(true);
          } else {
            modified_fields.emplace_back(false);
          }
        }
        auto undo_log_schema = Schema(undo_log_columns);
        auto undo_log = UndoLog{false, modified_fields, Tuple(undo_log_values, &undo_log_schema), previous_undo_log.ts_,
                                previous_undo_log.prev_version_};
        // modify existing undo log in current transaction
        txn_->ModifyUndoLog(0, undo_log);
      }
    }
    // update base tuple
    table_heap_->UpdateTupleInPlace(new_tuple_meta, new_tuple, *rid);
    // add to write set
    modified_rids.emplace_back(*rid);

    // update index: [TODO in task 4.2]
    // for (auto index : indices_) {
    //   auto old_key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    //   auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    //   // delete old entry from index
    //   index->index_->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
    //   // insert new entry to index
    //   index->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
    // }
    ++cnt;
  }
  finished_ = true;
  std::vector<Value> values;
  values.emplace_back(INTEGER, cnt);
  auto schema = plan_->OutputSchema();
  *tuple = Tuple(values, &schema);
  // add all modified tuples
  for (const auto &modified_rid : modified_rids) {
    txn_->AppendWriteSet(plan_->table_oid_, modified_rid);
  }
  return true;
}

}  // namespace bustub
