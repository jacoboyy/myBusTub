//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <iterator>
#include <stdexcept>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::HasFreeFrame(frame_id_t *frame_id) -> bool {
  // frame available in free list
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  // at least one evictable frame
  if (replacer_->Evict(frame_id)) {
    // write dirty page to disk
    if (pages_[*frame_id].is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[*frame_id].data_, pages_[*frame_id].page_id_, std::move(promise)});
      if (!future.get()) {
        throw std::runtime_error("Eviction: disk manager failed to write to disk");
      }
      pages_[*frame_id].is_dirty_ = false;
    }
    // reset memory and meta data
    pages_[*frame_id].ResetMemory();
    pages_[*frame_id].pin_count_ = 0;
    page_table_[pages_[*frame_id].page_id_] = -1;
    pages_[*frame_id].page_id_ = INVALID_PAGE_ID;
    return true;
  }
  return false;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> slk(latch_);
  frame_id_t frame_id;
  if (HasFreeFrame(&frame_id)) {
    *page_id = AllocatePage();
    // update page table entry
    page_table_.emplace_back(frame_id);
    // update page entry
    pages_[frame_id].page_id_ = *page_id;
    ++pages_[frame_id].pin_count_;
    // record access and set evictable
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  page_id = nullptr;
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> slk(latch_);
  frame_id_t frame_id;
  // page already in buffered pool
  if (static_cast<size_t>(page_id) < page_table_.size() && page_table_[page_id] >= 0) {
    frame_id = page_table_[page_id];
    // pin page
    ++pages_[frame_id].pin_count_;
    // record access and disable eviction in replacer
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  // acquire a newly-evicted page
  if (HasFreeFrame(&frame_id)) {
    // update page table entry
    page_table_[page_id] = frame_id;
    // update page entry
    pages_[frame_id].page_id_ = page_id;
    ++pages_[frame_id].pin_count_;
    // read from disk
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({false, pages_[frame_id].data_, page_id, std::move(promise)});
    if (!future.get()) {
      throw std::runtime_error("failed to read page from disk");
    }
    // record access and disable eviction in replacer
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> slk(latch_);
  // page_id not in buffer pool
  if (static_cast<size_t>(page_id) >= page_table_.size() || page_table_[page_id] < 0) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }

  --pages_[frame_id].pin_count_;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  // only set dirty if is_dirty == true
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> slk(latch_);
  // page not in buffer pool
  if (static_cast<size_t>(page_id) >= page_table_.size() || page_table_[page_id] < 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, pages_[frame_id].data_, page_id, std::move(promise)});
  if (!future.get()) {
    throw std::runtime_error("FlushPage: failed to flush page to disk");
  }
  // update dirty flag
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> slk(latch_);
  for (auto it = page_table_.begin(); it != page_table_.end(); ++it) {
    if (*it >= 0) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      page_id_t page_id = std::distance(page_table_.begin(), it);
      disk_scheduler_->Schedule({true, pages_[*it].data_, page_id, std::move(promise)});
      if (!future.get()) {
        throw std::runtime_error("FlushAllPages: failed to flush page to disk");
      }
      pages_[*it].is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> slk(latch_);
  if (static_cast<size_t>(page_id) >= page_table_.size() || page_table_[page_id] < 0) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  // page is already unpinned
  page_table_[page_id] = -1;
  replacer_->Remove(frame_id);
  free_list_.emplace_back(frame_id);
  // update pages
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  // de-allocate
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  if (page == nullptr) {
    throw std::runtime_error("buffer pool is full: cannot obtain new page");
  }
  return {this, page};
}

}  // namespace bustub