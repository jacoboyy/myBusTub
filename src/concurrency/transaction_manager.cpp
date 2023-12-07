//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_.store(last_commit_ts_);

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_ + 1;
  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  // update the timestamp of all tuples in the write set
  for (auto &it : txn->write_set_) {
    auto table_oid = it.first;
    auto rids = it.second;
    auto &table_heap = catalog_->GetTable(table_oid)->table_;
    for (const auto &rid : rids) {
      auto tuple_meta = table_heap->GetTupleMeta(rid);
      tuple_meta.ts_ = commit_ts;
      table_heap->UpdateTupleMeta(tuple_meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_.store(commit_ts);
  ++last_commit_ts_;

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // all the transactions are initialized placed in the garbage list
  std::unordered_set<txn_id_t> garbages;
  for (auto &pair : txn_map_) {
    garbages.insert(pair.first);
  }
  // traverse all tables
  for (const auto &name : catalog_->GetTableNames()) {
    auto table_info = catalog_->GetTable(name);
    auto &table_heap = table_info->table_;
    // traverse the table heap
    auto it = table_heap->MakeIterator();
    while (!it.IsEnd()) {
      auto rid = it.GetRID();
      auto undo_link = GetUndoLink(rid);
      auto tuple_meta = it.GetTuple().first;
      auto tuple = it.GetTuple().second;
      if (tuple_meta.ts_ <= GetWatermark()) {
        if (undo_link) {
          undo_link->prev_txn_ = INVALID_TXN_ID;
        }
      }
      // traverse the version chain
      while (undo_link && undo_link->IsValid()) {
        auto undo_log = GetUndoLog(*undo_link);
        auto txn_id = undo_link->prev_txn_;
        auto next_tuple = ReconstructTuple(&table_info->schema_, tuple, tuple_meta, {undo_log});
        if (!next_tuple) {
          break;
        }
        // remove the current transaction from the garbage set
        if (garbages.find(txn_id) != garbages.end()) {
          garbages.erase(txn_id);
        }
        // stop the version train traversal when the timestamp of the undo log is below or equal to the watermark
        if (undo_log.ts_ <= GetWatermark()) {
          // remove dangling pointers
          undo_log.prev_version_.prev_txn_ = INVALID_TXN_ID;
          break;
        }
        tuple = *next_tuple;
        undo_link = undo_log.prev_version_;
      }
      ++it;
    }

    // erase garabe transaction from txn_map_
    for (auto &txn_id : garbages) {
      if (txn_map_.find(txn_id) != txn_map_.end()) {
        auto txn = txn_map_.at(txn_id);
        // only remove commited or aborted transactions
        if (txn->state_ == TransactionState::COMMITTED || txn->state_ == TransactionState::ABORTED) {
          txn_map_.erase(txn_id);
        }
      }
    }
  }
}

}  // namespace bustub
