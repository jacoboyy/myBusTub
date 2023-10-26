#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // drop currently-held page, if any
  this->Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { this->Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  // acquire read-lock
  page_->RLatch();
  auto read_guard = ReadPageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return read_guard;
};

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  // acquire write-lock
  page_->WLatch();
  auto write_guard = WritePageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return write_guard;
};

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = BasicPageGuard(std::move(that.guard_)); };

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // drop currently-held page, if any
  this->Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  // unlatch
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  // unpin
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = BasicPageGuard(std::move(that.guard_)); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  // drop currently-held page, if any
  this->Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  // unlatch
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  // unpin
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { this->Drop(); }  // NOLINT

}  // namespace bustub
