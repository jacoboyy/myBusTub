//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <utility>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  table_name_ = table_info_->name_;
  table_heap_ = table_info_->table_.get();
  indices_ = catalog->GetTableIndexes(table_name_);
  child_executor_->Init();
  finished_ = false;  // reset at each init
  txn_ = exec_ctx_->GetTransaction();
  txn_manager_ = exec_ctx_->GetTransactionManager();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  int cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    // insert into table
    TupleMeta tuple_meta = {txn_->GetTransactionTempTs(), false};
    auto next_rid = table_heap_->InsertTuple(tuple_meta, *tuple);
    if (next_rid) {
      // add to write set
      txn_->AppendWriteSet(plan_->table_oid_, *next_rid);
      // insert into index
      for (auto index : indices_) {
        auto key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(key, *next_rid, exec_ctx_->GetTransaction());
      }
    }
    ++cnt;
  }
  std::vector<Value> values;
  values.emplace_back(INTEGER, cnt);
  auto schema = plan_->OutputSchema();
  *tuple = Tuple(values, &schema);
  finished_ = true;
  return true;
}

}  // namespace bustub