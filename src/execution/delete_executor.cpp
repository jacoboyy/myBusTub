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

#include <memory>

#include "execution/executors/delete_executor.h"

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
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  int cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    // delete from table
    TupleMeta tuple_meta = {INVALID_TXN_ID, true};
    table_heap_->UpdateTupleMeta(tuple_meta, *rid);
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
  return true;
}

}  // namespace bustub
