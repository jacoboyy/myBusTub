//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  max_size_ = max_size;
  size_ = 0;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  for (uint32_t i = 0; i < size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      // set value and return
      value = array_[i].second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  // check if full
  if (IsFull()) {
    return false;
  }
  // check if key already exists
  for (uint32_t i = 0; i < size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      return false;
    }
  }
  // insert key and increment size
  array_[size_++] = {key, value};
  return true;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Append(std::pair<K, V> pair) {
  array_[size_++] = pair;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  int idx = -1;
  for (auto i = 0; i < static_cast<int>(size_); i++) {
    if (cmp(array_[i].first, key) == 0) {
      // key match
      idx = i;
      break;
    }
  }
  // remove if key is found and shift the remaining entry
  if (idx != -1) {
    for (auto i = idx; i < static_cast<int>(size_) - 1; i++) {
      array_[i] = array_[i + 1];
    }
    // clear the last entry and reduce size
    array_[--size_] = {};
    return true;
  }
  return false;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  if (bucket_idx < size_) {
    // shift remaining entries
    for (auto i = bucket_idx; i < size_ - 1; i++) {
      array_[i] = array_[i + 1];
    }
    // clear the last entry and reduce size
    array_[--size_] = {};
  }
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Pop() -> std::pair<K, V> {
  auto res = array_[--size_];
  array_[size_] = {};
  return res;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Swap(int i, int j) {
  auto tmp = array_[i];
  array_[i] = array_[j];
  array_[j] = tmp;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub