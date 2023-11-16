//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct JoinKey {
  /** The join key values */
  std::vector<Value> values_;

  /**
   * Compares two join keys for equality.
   * @param other the other join key to be compared with
   * @return `true` if both join keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const JoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.values_.size(); i++) {
      if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {

/** Implements std::hash on JoinKey */
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.values_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** The left table executor */
  std::unique_ptr<AbstractExecutor> left_executor_;

  /** The right table executor */
  std::unique_ptr<AbstractExecutor> right_executor_;

  /** Left table schema*/
  Schema left_schema_;

  /** Right table schema*/
  Schema right_schema_;

  /** Hashes built on the right table*/
  std::unordered_map<JoinKey, std::vector<Tuple>> right_hashmap_;

  /** Join type*/
  JoinType join_type_;

  /** Current left table tuple*/
  Tuple left_tuple_;

  /** Current idx of the right tuple hashes*/
  size_t right_idx_;

  /** @return The tuple as an JoinKey */
  auto MakeLeftJoinKey(const Tuple *tuple) -> JoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, left_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an JoinKey */
  auto MakeRightJoinKey(const Tuple *tuple) -> JoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, right_executor_->GetOutputSchema()));
    }
    return {keys};
  }
};

}  // namespace bustub
