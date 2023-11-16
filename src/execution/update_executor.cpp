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

#include "execution/executors/update_executor.h"

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
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  int cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    // delete from table
    TupleMeta delete_tuple_meta = {INVALID_TXN_ID, true};
    table_heap_->UpdateTupleMeta(delete_tuple_meta, *rid);
    // create new tuple first
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    for (auto &e : plan_->target_expressions_) {
      new_values.emplace_back(e->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    auto new_tuple = Tuple(new_values, &child_executor_->GetOutputSchema());
    // insert new tuple to table
    TupleMeta insert_tuple_meta = {INVALID_TXN_ID, false};
    auto optional_new_rid = table_heap_->InsertTuple(insert_tuple_meta, new_tuple);
    if (!optional_new_rid.has_value()) {
      break;
    }
    auto new_rid = optional_new_rid.value();

    // update index
    for (auto index : indices_) {
      auto old_key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      // delete old entry from index
      index->index_->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
      // insert new entry to index
      index->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
    }
    ++cnt;
  }
  finished_ = true;
  std::vector<Value> values;
  values.emplace_back(INTEGER, cnt);
  auto schema = plan_->OutputSchema();
  *tuple = Tuple(values, &schema);
  return true;
}

}  // namespace bustub
