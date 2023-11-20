#include "storage/page/page_guard.h"
#include <utility>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

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
  if (this != &that) {
    if (this->page_ != nullptr) {
      this->bpm_->UnpinPage(this->page_->GetPageId(), this->is_dirty_);
    }

    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;

    that.bpm_ = nullptr;
    that.page_ = nullptr;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  this->guard_.bpm_ = that.guard_.bpm_;
  this->guard_.page_ = that.guard_.page_;
  this->guard_.is_dirty_ = that.guard_.is_dirty_;

  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    if (this->guard_.page_ != nullptr) {
      this->guard_.page_->RUnlatch();
      this->guard_.bpm_->UnpinPage(this->guard_.page_->GetPageId(), this->guard_.is_dirty_);
    }

    this->guard_.bpm_ = that.guard_.bpm_;
    this->guard_.page_ = that.guard_.page_;
    this->guard_.is_dirty_ = that.guard_.is_dirty_;

    that.guard_.bpm_ = nullptr;
    that.guard_.page_ = nullptr;
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
  }
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  this->guard_.bpm_ = that.guard_.bpm_;
  this->guard_.page_ = that.guard_.page_;
  this->guard_.is_dirty_ = that.guard_.is_dirty_;

  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    if (this->guard_.page_ != nullptr) {
      this->guard_.page_->WUnlatch();
      this->guard_.bpm_->UnpinPage(this->guard_.page_->GetPageId(), this->guard_.is_dirty_);
    }

    this->guard_.bpm_ = that.guard_.bpm_;
    this->guard_.page_ = that.guard_.page_;
    this->guard_.is_dirty_ = that.guard_.is_dirty_;

    that.guard_.bpm_ = nullptr;
    that.guard_.page_ = nullptr;
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
  }
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
}

WritePageGuard::~WritePageGuard() { this->Drop(); }

}  // namespace bustub
