//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/integer_type.h"
#include "type/type_id.h"

namespace bustub {
/** PartitionKey represents a partition key in an window operation */
struct PartitionKey {
  /** The group-by values */
  std::vector<Value> partition_bys_;

  /**
   * Compares two aggregate keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const PartitionKey &other) const -> bool {
    for (uint32_t i = 0; i < other.partition_bys_.size(); i++) {
      if (partition_bys_[i].CompareEquals(other.partition_bys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }

  auto operator<(const PartitionKey &other) const -> bool {
    for (uint32_t i = 0; i < other.partition_bys_.size(); i++) {
      if (partition_bys_[i].CompareLessThan(other.partition_bys_[i]) == CmpBool::CmpTrue) {
        return true;
      }
    }
    return false;
  }
};
}  // namespace bustub

namespace std {

/** Implements std::hash on PartitionKey */
template <>
struct hash<bustub::PartitionKey> {
  auto operator()(const bustub::PartitionKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.partition_bys_) {
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
 * A simplified hash table that has all the necessary functionality for window.
 */
class SimpleWindowHashTable {
 public:
  SimpleWindowHashTable(const WindowFunctionType window_function_type, bool has_order_by)
      : window_function_type_(window_function_type), has_order_by_(has_order_by) {}

  /**
   * Combines the input into the window result.
   * @param values The list of aggregated window values
   * @param input The input value
   */
  void InsertWindowValues(std::vector<Value> &values, const Value &input) {
    switch (window_function_type_) {
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::CountStarAggregate:
        if (values.empty()) {
          values.emplace_back(INTEGER, 1);
        } else {
          if (has_order_by_) {
            values.emplace_back(values.back().Add({INTEGER, 1}));
          } else {
            for (auto &value : values) {
              value = value.Add({INTEGER, 1});
            }
            values.emplace_back(values.back().Copy());
          }
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (values.empty()) {
          values.emplace_back(input);
        } else {
          if (has_order_by_) {
            values.emplace_back(values.back().Max(input));
          } else {
            for (auto &value : values) {
              value = value.Max(input);
            }
            values.emplace_back(values.back().Copy());
          }
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (values.empty()) {
          values.emplace_back(input);
        } else {
          if (has_order_by_) {
            values.emplace_back(values.back().Min(input));
          } else {
            for (auto &value : values) {
              value = value.Min(input);
            }
            values.emplace_back(values.back().Copy());
          }
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (values.empty()) {
          values.emplace_back(input);
        } else {
          if (has_order_by_) {
            values.emplace_back(values.back().Add(input));
          } else {
            for (auto &value : values) {
              value = value.Add(input);
            }
            values.emplace_back(values.back().Copy());
          }
        }
        break;
      case WindowFunctionType::Rank:
        if (values.empty()) {
          values.emplace_back(INTEGER, 1);
        } else {
          if (input.CompareEquals(ori_values_.back()) == CmpBool::CmpTrue) {
            values.emplace_back(values.back().Copy());
          } else {
            values.emplace_back(INTEGER, static_cast<int>(ori_values_.size() + 1));
          }
        }
        ori_values_.emplace_back(input);
    }
  }

  /**
   * Inserts a value into the hash table
   * @param partition_key the key to be inserted
   * @param function_value the value to be inserted
   */
  void Insert(const PartitionKey &partition_key, const Value &function_value) {
    if (ht_.count(partition_key) == 0) {
      ht_.insert({partition_key, {}});
    }
    InsertWindowValues(ht_[partition_key], function_value);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /**
   * Get original value for Rank()
   */
  auto GetOriVal(size_t idx) -> Value { return ori_values_.at(idx); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::map<PartitionKey, std::vector<Value>>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const PartitionKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const std::vector<Value> & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::map<PartitionKey, std::vector<Value>>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

 private:
  std::map<PartitionKey, std::vector<Value>> ht_{};
  /** The type of window function that we have */
  const WindowFunctionType window_function_type_;
  bool has_order_by_;
  std::vector<Value> ori_values_;  // only used for Rank()
};

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functions and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** @return The tuple as an PartitionKey */
  auto MakePartitionKey(const Tuple *tuple, size_t idx) -> PartitionKey {
    std::vector<Value> keys;
    auto window_function = plan_->window_functions_.at(idx);
    keys.reserve(window_function.partition_by_.size());
    for (const auto &expr : window_function.partition_by_) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an window function value */
  auto MakeFunctionValue(const Tuple *tuple, size_t idx) -> Value {
    auto window_function = plan_->window_functions_.at(idx);
    return window_function.type_ != WindowFunctionType::Rank
               ? window_function.function_->Evaluate(tuple, child_executor_->GetOutputSchema())
               : window_function.order_by_[0].second->Evaluate(tuple, child_executor_->GetOutputSchema());
  }

  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** Container to store the sorted tuples*/
  using TupleRID = std::pair<Tuple, RID>;
  std::vector<TupleRID> sorted_tuples_;

  std::vector<TupleRID>::iterator sorted_iterator_;

  std::vector<SimpleWindowHashTable> window_hts_;

  std::vector<std::pair<SimpleWindowHashTable::Iterator, size_t>> window_ht_iterators_;

  size_t cur_idx_{0};
};
}  // namespace bustub
