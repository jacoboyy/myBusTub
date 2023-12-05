#include "concurrency/watermark.h"
#include <exception>
#include <utility>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  if (current_reads_.find(read_ts) != current_reads_.end()) {
    // exists previous transaction with the same read timestamp
    current_reads_[read_ts]++;
  } else {
    if (read_ts < watermark_) {
      watermark_ = read_ts;
    }
    current_reads_.insert(std::make_pair(read_ts, 1));
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  current_reads_[read_ts]--;
  if (current_reads_[read_ts] == 0) {
    current_reads_.erase(read_ts);
    if (watermark_ == read_ts) {
      // update watermark when the removed read_ts is the watermark
      if (!current_reads_.empty()) {
        watermark_ = current_reads_.begin()->first;
      } else {
        watermark_ = commit_ts_;
      }
    }
  }
}

}  // namespace bustub
