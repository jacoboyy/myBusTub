//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <mutex>
#include <stdexcept>
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> slk(latch_);
  bool evicted = false;
  if (!less_k_lst_.empty()) {
    for (auto it = less_k_lst_.begin(); it != less_k_lst_.end(); ++it) {
      if (node_store_[*it].is_evictable_) {
        *frame_id = *it;
        less_k_lst_.erase(it);
        evicted = true;
        break;
      }
    }
  }
  if (!evicted && !main_lst_.empty()) {
    for (auto it = main_lst_.begin(); it != main_lst_.end(); ++it) {
      if (node_store_[*it].is_evictable_) {
        *frame_id = *it;
        main_lst_.erase(it);
        evicted = true;
        break;
      }
    }
  }
  if (evicted) {
    node_store_.erase(*frame_id);
    --curr_size_;
  }
  return evicted;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> slk(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument("invalid frame_id");
  }
  size_t &num_hits = ++node_store_[frame_id].num_hits_;
  if (num_hits == 1) {
    less_k_lst_.emplace_back(frame_id);
    node_store_[frame_id].pos_ = std::prev(less_k_lst_.end());
  }
  if (num_hits == k_) {
    less_k_lst_.erase(node_store_[frame_id].pos_);
    main_lst_.emplace_back(frame_id);
    node_store_[frame_id].pos_ = std::prev(main_lst_.end());
  } else if (num_hits > k_) {
    main_lst_.erase(node_store_[frame_id].pos_);
    main_lst_.emplace_back(frame_id);
    node_store_[frame_id].pos_ = std::prev(main_lst_.end());
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> slk(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument("invalid frame_id");
  }
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  if (node_store_[frame_id].is_evictable_ && !set_evictable) {
    --curr_size_;
  } else if (!node_store_[frame_id].is_evictable_ && set_evictable) {
    ++curr_size_;
  }
  node_store_[frame_id].is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> slk(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument("invalid frame_id");
  }
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  if (!node_store_[frame_id].is_evictable_) {
    throw std::runtime_error("Remove: frame is not evictable");
  }

  if (node_store_[frame_id].num_hits_ < k_) {
    less_k_lst_.erase(node_store_[frame_id].pos_);
  } else {
    main_lst_.erase(node_store_[frame_id].pos_);
  }
  node_store_.erase(frame_id);
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> slk(latch_);
  return curr_size_;
}

}  // namespace bustub