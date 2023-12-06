#include "execution/execution_common.h"
#include <cstdio>
#include <optional>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  auto res_tuple = base_tuple;
  auto res_meta = base_meta;
  for (const auto &log : undo_logs) {
    if (log.is_deleted_) {
      res_meta.is_deleted_ = true;
    } else {
      res_meta.is_deleted_ = false;
      std::vector<Column> updated_columns;
      for (size_t idx = 0; idx < schema->GetColumnCount(); idx++) {
        if (log.modified_fields_[idx]) {
          updated_columns.emplace_back(schema->GetColumn(idx));
        }
      }
      if (!updated_columns.empty()) {
        const Schema updated_schema = Schema(updated_columns);
        std::vector<Value> updated_values;
        size_t updated_idx = 0;
        for (size_t idx = 0; idx < schema->GetColumnCount(); idx++) {
          if (log.modified_fields_[idx]) {
            updated_values.emplace_back(log.tuple_.GetValue(&updated_schema, updated_idx));
            updated_idx++;
          } else {
            updated_values.emplace_back(res_tuple.GetValue(schema, idx));
          }
        }
        res_tuple = Tuple(updated_values, schema);
      }
    }
  }
  if (!res_meta.is_deleted_) {
    return res_tuple;
  }
  return {};
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  TableIterator table_iterator = table_heap->MakeEagerIterator();
  while (!table_iterator.IsEnd()) {
    auto rid = table_iterator.GetRID();
    auto tuple_meta = table_iterator.GetTuple().first;
    auto tuple = table_iterator.GetTuple().second;

    // print current tuple
    auto delete_marker = tuple_meta.is_deleted_ ? "<delete_marker>" : "";
    if ((tuple_meta.ts_ & TXN_START_ID) != 0) {
      fmt::println(stderr, "RID:{}/{} ts=txn{} {} tuple={}", rid.GetPageId(), rid.GetSlotNum(),
                   (tuple_meta.ts_ ^ TXN_START_ID), delete_marker, tuple.ToString(&table_info->schema_));
    } else {
      fmt::println(stderr, "RID:{}/{} ts={} {} tuple={}", rid.GetPageId(), rid.GetSlotNum(), tuple_meta.ts_,
                   delete_marker, tuple.ToString(&table_info->schema_));
    }

    // print version link
    auto undo_link = txn_mgr->GetUndoLink(rid);
    if (undo_link.has_value()) {
      while (undo_link->IsValid()) {
        auto undo_log = txn_mgr->GetUndoLog(undo_link.value());
        if (undo_log.is_deleted_) {
          fmt::println(stderr, "   txn{}@{} <del> ts={}", (undo_link->prev_txn_ ^ TXN_START_ID),
                       undo_link->prev_log_idx_, undo_log.ts_);
        } else {
          std::vector<Column> updated_columns;
          for (size_t idx = 0; idx < table_info->schema_.GetColumnCount(); idx++) {
            if (undo_log.modified_fields_[idx]) {
              updated_columns.emplace_back(table_info->schema_.GetColumn(idx));
            }
          }
          const Schema updated_schema = Schema(updated_columns);
          fmt::println(stderr, "   txn{}@{} {} ts={}", (undo_link->prev_txn_ ^ TXN_START_ID), undo_link->prev_log_idx_,
                       undo_log.tuple_.ToString(&updated_schema), undo_log.ts_);
        }
        undo_link = undo_log.prev_version_;
      }
    }
    ++table_iterator;
  }

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

auto IsWriteWriteConflict(timestamp_t tuple_ts, Transaction *txn) -> bool {
  bool tuple_commited = (tuple_ts & TXN_START_ID) == 0;
  return (tuple_commited && tuple_ts > txn->GetReadTs()) ||
         (!tuple_commited && tuple_ts != txn->GetTransactionTempTs());
}

}  // namespace bustub
