//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <exception>
#include <memory>
#include <vector>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  table_name_ = table_info_->name_;
  table_heap_ = table_info_->table_.get();
  indices_ = catalog->GetTableIndexes(table_name_);
  child_executor_->Init();
  finished_ = false;  // reset at initialization
  txn_ = exec_ctx_->GetTransaction();
  txn_manager_ = exec_ctx_->GetTransactionManager();
  // store all tuples from child executor locally to avoid halloween problems
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuple_rid_pairs_.emplace_back(tuple, rid);
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  std::vector<RID> modified_rids;
  int cnt = 0;
  for (auto &tuple_rid_pair : tuple_rid_pairs_) {
    *tuple = tuple_rid_pair.first;
    *rid = tuple_rid_pair.second;
    // prepare meta data for deletion
    auto old_tuple_meta = table_heap_->GetTupleMeta(*rid);
    auto new_tuple_meta = TupleMeta{txn_->GetTransactionTempTs(), true};
    auto old_undo_link = txn_manager_->GetUndoLink(*rid);
    std::vector<bool> modified_fields;
    for (size_t idx = 0; idx < GetOutputSchema().GetColumnCount(); idx++) {
      modified_fields.emplace_back(true);
    }
    // check current tuple timestamp
    bool tuple_commited = (old_tuple_meta.ts_ & TXN_START_ID) == 0;
    // detect write-write conflict
    if (IsWriteWriteConflict(old_tuple_meta.ts_, txn_)) {
      finished_ = true;
      // set tainted and throw exception
      txn_->SetTainted();
      throw ExecutionException("write-write conflict detected!");
    }
    if (tuple_commited) {
      // generate a undo log containing full data
      auto undo_log = UndoLog{false, modified_fields, *tuple, old_tuple_meta.ts_};
      if (old_undo_link && old_undo_link->IsValid()) {
        undo_log.prev_version_ = *old_undo_link;
      }
      // store undo log in current transaction
      auto new_undo_link = txn_->AppendUndoLog(undo_log);
      // update version link of tuple to point to the new undo log
      txn_manager_->UpdateUndoLink(*rid, new_undo_link);
    }
    // update base tuple meta
    table_heap_->UpdateTupleMeta(new_tuple_meta, *rid);
    // add to write set
    modified_rids.emplace_back(*rid);
    // delete from index
    for (auto index : indices_) {
      auto key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    ++cnt;
  }
  std::vector<Value> values;
  values.emplace_back(INTEGER, cnt);
  auto schema = plan_->OutputSchema();
  *tuple = Tuple(values, &schema);
  finished_ = true;
  // add all modified tuples
  for (const auto &modified_rid : modified_rids) {
    txn_->AppendWriteSet(plan_->table_oid_, modified_rid);
  }
  return true;
}

}  // namespace bustub
