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

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  Page *page_ptr;
  frame_id_t new_frame_id;
  if (!free_list_.empty()) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
    page_ptr = &pages_[new_frame_id];
  } else {
    replacer_->Evict(&new_frame_id);
    page_ptr = &pages_[new_frame_id];
    page_id_t old_page_id = page_ptr->GetPageId();
    if (page_ptr->IsDirty()) {
      disk_manager_->WritePage(old_page_id, page_ptr->GetData());
      page_ptr->is_dirty_ = false;
    }
    page_ptr->ResetMemory();
    page_table_.erase(old_page_id);
  }
  page_id_t new_page_id = AllocatePage();
  *page_id = new_page_id;
  page_ptr->page_id_ = new_page_id;
  page_table_[new_page_id] = new_frame_id;
  page_ptr->pin_count_++;
  replacer_->RecordAccess(new_frame_id);
  replacer_->SetEvictable(new_frame_id, false);
  return page_ptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  Page *page_ptr;
  frame_id_t new_frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    Page *page_ptr = &pages_[page_table_[page_id]];
    page_ptr->pin_count_++;
    return page_ptr;
  }
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  if (!free_list_.empty()) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
    page_ptr = &pages_[new_frame_id];
  } else {
    replacer_->Evict(&new_frame_id);
    page_ptr = &pages_[new_frame_id];
    page_id_t old_page_id = page_ptr->GetPageId();
    if (page_ptr->IsDirty()) {
      disk_manager_->WritePage(old_page_id, page_ptr->GetData());
      page_ptr->is_dirty_ = false;
    }
    page_ptr->ResetMemory();
    page_table_.erase(old_page_id);
  }
  page_ptr->page_id_ = page_id;
  page_table_[page_id] = new_frame_id;
  disk_manager_->ReadPage(page_id, page_ptr->GetData());
  page_ptr->pin_count_++;
  replacer_->RecordAccess(new_frame_id);
  replacer_->SetEvictable(new_frame_id, false);
  return page_ptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  Page *page_ptr = &pages_[page_table_[page_id]];
  if (page_ptr->GetPinCount() <= 0) {
    return false;
  }
  page_ptr->pin_count_--;
  if (page_ptr->GetPinCount() == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  page_ptr->is_dirty_ = is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  Page *page_ptr = &pages_[page_table_[page_id]];
  disk_manager_->WritePage(page_id, page_ptr->GetData());
  page_ptr->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto it : page_table_) {
    Page *page_ptr = &pages_[it.first];
    disk_manager_->WritePage(it.first, page_ptr->GetData());
    page_ptr->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page_ptr = &pages_[frame_id];
  if (page_ptr->GetPinCount() > 0) {
    return false;
  }
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  page_ptr->ResetMemory();
  page_ptr->pin_count_ = 0;
  page_ptr->is_dirty_ = false;
  page_ptr->page_id_ = INVALID_PAGE_ID;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page_ptr = FetchPage(page_id);
  return {this, page_ptr};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  if (page_table_.find(page_id) == page_table_.end()) {
    return {this, nullptr};
  }
  Page *page_ptr = &pages_[page_table_[page_id]];
  page_ptr->RLatch();
  FetchPage(page_id);
  return ReadPageGuard{this, page_ptr};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  if (page_table_.find(page_id) == page_table_.end()) {
    return {this, nullptr};
  }
  Page *page_ptr = &pages_[page_table_[page_id]];
  page_ptr->WLatch();
  FetchPage(page_id);
  return WritePageGuard{this, page_ptr};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  std::lock_guard<std::mutex> lock(latch_);
  NewPage(page_id);
  if (page_id == nullptr) {
    return {this, nullptr};
  }
  Page *page_ptr = &pages_[page_table_[*page_id]];
  return BasicPageGuard{this, page_ptr};
}

}  // namespace bustub
