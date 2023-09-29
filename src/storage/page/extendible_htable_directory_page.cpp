//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  bucket_page_ids_[0] = INVALID_PAGE_ID;
  local_depths_[0] = 0;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1 << global_depth_) - 1; }

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  auto res = (1 << local_depths_[bucket_idx]) - 1;
  return static_cast<uint32_t>(res);
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  uint32_t mask = 1 << (local_depths_[bucket_idx] - 1);
  return bucket_idx ^ mask;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // update local depth
  auto offset = 1 << global_depth_;
  for (auto i = 0; i < offset; i++) {
    auto split_image_idx = i + offset;
    local_depths_[split_image_idx] = local_depths_[i];
    bucket_page_ids_[split_image_idx] = bucket_page_ids_[i];
  }
  // update global depth
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  for (auto i = (1 << (global_depth_ - 1)); i < (1 << global_depth_); i++) {
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
  global_depth_--;
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (auto i = 0; i < (1 << global_depth_); i++) {
    if (GetLocalDepth(i) == GetGlobalDepth()) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1 << max_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

}  // namespace bustub