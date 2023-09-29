//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <exception>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // create header page
  header_page_id_ = INVALID_PAGE_ID;
  WritePageGuard header_guard = bpm_->NewPageGuarded(&header_page_id_).UpgradeWrite();
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto hash = Hash(key);
  // fetch header page
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  // fetch directory page
  auto directory_page_id =
      static_cast<page_id_t>(header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash)));
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard directory_page_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_page_guard.As<ExtendibleHTableDirectoryPage>();
  header_guard.Drop();  // unpin header page after obtaining directory page
  // fetch bucket page
  auto bucket_page_id = directory_page->GetBucketPageId(directory_page->HashToBucketIndex(hash));
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard bucket_page_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  directory_page_guard.Drop();  // unpin directory page after obtaining bucket page
  // search key in the bucket_page
  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->emplace_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto hash = Hash(key);
  // fetch header Page
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  // fetch directory Page
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(directory_idx));
  WritePageGuard directory_page_guard;
  ExtendibleHTableDirectoryPage *directory_page;
  if (directory_page_id == INVALID_PAGE_ID) {
    directory_page_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
    header_page->SetDirectoryPageId(directory_idx, directory_page_id);
    directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
    directory_page->Init(directory_max_depth_);
  } else {
    directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
    directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  }
  header_guard.Drop();  // unlatch header page after getting the directory page
  // fetch bucket page
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  WritePageGuard bucket_page_guard;
  ExtendibleHTableBucketPage<K, V, KC> *bucket_page;
  if (bucket_page_id == INVALID_PAGE_ID) {
    bucket_page_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
    directory_page->SetBucketPageId(bucket_idx, bucket_page_id);
    bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page->Init(bucket_max_size_);
  } else {
    bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
    bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  }
  V temp;
  // check key exists
  if (bucket_page->Lookup(key, temp, cmp_)) {
    return false;
  }
  // check if bucket is full
  if (!bucket_page->IsFull()) {
    bucket_page->Append({key, value});
    return true;
  }
  // grow directory, if needed
  auto local_depth = directory_page->GetLocalDepth(bucket_idx);
  auto global_depth = directory_page->GetGlobalDepth();
  if (local_depth == global_depth) {
    if (global_depth == directory_page->GetMaxDepth()) {
      // hash index is full
      return false;
    }
    directory_page->IncrGlobalDepth();
  }
  // create new buckets
  auto new_bucket_page_id = INVALID_PAGE_ID;
  WritePageGuard new_bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
  auto new_bucket_page = new_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket_page->Init(bucket_max_size_);
  // rehash keys in the original bucket
  int idx = 0;
  int last_idx = bucket_page->Size() - 1;
  while (idx <= last_idx) {
    if (Hash(bucket_page->KeyAt(idx)) & (1 << local_depth)) {
      // swap elements with high bit of 1 to the back
      bucket_page->Swap(idx, last_idx);
      last_idx--;
    } else {
      idx++;
    }
  }
  int ori_bucket_size = bucket_page->Size();
  for (int i = 0; i < ori_bucket_size - last_idx - 1; i++) {
    new_bucket_page->Append(bucket_page->Pop());
  }
  // reassign buckets in the directory page
  bucket_page_guard.Drop();
  new_bucket_page_guard.Drop();
  for (uint32_t i = 0; i < directory_page->Size(); i++) {
    if (directory_page->GetBucketPageId(i) == bucket_page_id) {
      if (i & (1 << local_depth)) {
        directory_page->SetBucketPageId(i, new_bucket_page_id);
      }
      directory_page->IncrLocalDepth(i);
    }
  }
  // unlatch and unpin pages
  directory_page_guard.Drop();
  // recursive insert
  return Insert(key, value, transaction);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto hash = Hash(key);
  // fetch header page
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  // fetch directory page
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(directory_idx));
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  header_guard.Drop();  // unlatch header page after getting the directory page
  // fetch bucket page
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  // try to remove key from the bucket
  if (!bucket_page->Remove(key, cmp_)) {
    // key not found
    return false;
  }
  // merge
  directory_page_guard.Drop();
  bucket_page_guard.Drop();
  Merge(directory_idx, bucket_idx);
  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::Merge(uint32_t directory_idx, uint32_t bucket_idx) {
  // fetch header page
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  // fetch directory Page
  auto directory_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(directory_idx));
  if (directory_page_id == INVALID_PAGE_ID) {
    return;
  }
  WritePageGuard directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  header_guard.Drop();  // unlatch header page after getting the directory page
  // fetch bucket page
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return;
  }
  ReadPageGuard bucket_page_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  if (!bucket_page->IsEmpty() || directory_page->GetLocalDepth(bucket_idx) == 0) {
    return;
  }
  // make sure split_image has the same length
  auto split_image_idx = directory_page->GetSplitImageIndex(bucket_idx);
  if (directory_page->GetLocalDepth(bucket_idx) != directory_page->GetLocalDepth(split_image_idx)) {
    return;
  }
  directory_page->SetBucketPageId(bucket_idx, directory_page->GetBucketPageId(split_image_idx));
  // decrement local depth and global depth
  directory_page->DecrLocalDepth(bucket_idx);
  directory_page->DecrLocalDepth(split_image_idx);
  if (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }
  // recursively split on the new split image
  if (bucket_idx >= directory_page->Size()) {
    bucket_idx -= directory_page->Size();
  }
  if (directory_page->GetLocalDepth(bucket_idx) == 0) {
    return;
  }
  auto new_split_image_idx = directory_page->GetSplitImageIndex(bucket_idx);
  // unlatch
  directory_page_guard.Drop();
  bucket_page_guard.Drop();
  Merge(directory_idx, new_split_image_idx);
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub