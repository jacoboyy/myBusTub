//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>
#include <vector>
#include "catalog/schema.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/table/table_iterator.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_.get();
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
  std::vector<Value> values;
  values.emplace_back(plan_->pred_key_->val_);
  auto key = Tuple(values, &index_info_->key_schema_);
  htable_->ScanKey(key, &result_, exec_ctx_->GetTransaction());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (idx_ < result_.size()) {
    *rid = result_[idx_];
    auto pair = table_heap_->GetTuple(*rid);
    ++idx_;
    if (pair.first.is_deleted_) {
      return false;
    }
    *tuple = pair.second;
    return true;
  }
  return false;
}

}  // namespace bustub
