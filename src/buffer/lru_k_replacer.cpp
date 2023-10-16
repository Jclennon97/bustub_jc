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
#include <algorithm>
#include <iterator>
#include <memory>
#include <type_traits>
#include <utility>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (evictable_size_ == 0) {
    return false;
  }
  for (auto temp_it : temp_pool_) {
    if (!node_store_[temp_it]->GetEvictable()) {
      continue;
    }
    *frame_id = temp_it;
    temp_pool_.erase(temp_pool_map_[temp_it]);
    temp_pool_map_.erase(temp_it);
    node_store_[temp_it].reset();
    node_store_.erase(temp_it);
    evictable_size_--;
    return true;
  }
  for (auto it = cache_pool_.begin(); it != cache_pool_.end(); it++) {
    auto cache_it = *it;
    if (!node_store_[cache_it.first]->GetEvictable()) {
      continue;
    }
    *frame_id = cache_it.first;
    cache_pool_.erase(cache_pool_map_[cache_it.first]);
    cache_pool_map_.erase(cache_it.first);
    node_store_[cache_it.first].reset();
    node_store_.erase(cache_it.first);
    evictable_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  BUSTUB_ENSURE(static_cast<size_t>(frame_id) <= replacer_size_, "frame id is invalid(larger than replacer_size_)");
  ++current_timestamp_;
  auto it = node_store_.find(frame_id);
  // 不存在页面，创建并加入到temp_pool中
  if (it == node_store_.end()) {
    auto newnode = std::make_shared<LRUKNode>(k_, frame_id);
    node_store_[frame_id] = newnode;
    newnode->PushFront(current_timestamp_);
    temp_pool_.push_back(frame_id);
    temp_pool_map_[frame_id] = std::prev(temp_pool_.end());
    return;
  }
  // 更新访问
  it->second->PushFront(current_timestamp_);
  auto backk = it->second->BackK();

  auto temp_it = temp_pool_map_.find(frame_id);
  auto cache_it = cache_pool_map_.find(frame_id);
  // 页面在temp_pool中
  if (temp_it != temp_pool_map_.end()) {
    // 若访问次数少于k次，则只更新访问记录，无需调整排序
    // 若访问次数等于或大于k次(backk 不为 0)，从temp_pool删除并加入到cache_pool中
    if (backk != 0) {
      temp_pool_.erase(temp_it->second);
      temp_pool_map_.erase(temp_it);
      auto new_pair = std::make_pair(frame_id, backk);
      auto insert_pos = std::upper_bound(cache_pool_.begin(), cache_pool_.end(), new_pair,
                                         [](const std::pair<frame_id_t, size_t> &a,
                                            const std::pair<frame_id_t, size_t> &b) { return a.second < b.second; });
      auto insert_it = cache_pool_.insert(insert_pos, new_pair);
      cache_pool_map_[new_pair.first] = insert_it;
    }
  } else {
    // 页面在cache_pool中，先把cache_pool中原来的删除，然后再插入cache中合适的位置，使得list从小到大排序
    cache_pool_.erase(cache_it->second);
    cache_pool_map_.erase(cache_it);
    auto new_pair = std::make_pair(frame_id, backk);
    auto insert_pos = std::upper_bound(cache_pool_.begin(), cache_pool_.end(), new_pair,
                                       [](const std::pair<frame_id_t, size_t> &a,
                                          const std::pair<frame_id_t, size_t> &b) { return a.second < b.second; });
    auto insert_it = cache_pool_.insert(insert_pos, new_pair);
    cache_pool_map_[new_pair.first] = insert_it;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  BUSTUB_ENSURE(static_cast<size_t>(frame_id) <= replacer_size_, "frame id is invalid(larger than replacer_size_)");
  auto frame_it = node_store_.find(frame_id);
  if (frame_it == node_store_.end()) {
    return;
  }
  auto frame_ptr = frame_it->second;
  if (frame_ptr->GetEvictable() && !set_evictable) {
    frame_ptr->SetEvictable(set_evictable);
    evictable_size_--;
  } else if (!frame_ptr->GetEvictable() && set_evictable) {
    frame_ptr->SetEvictable(set_evictable);
    evictable_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto frame_it = node_store_.find(frame_id);
  if (frame_it == node_store_.end()) {
    return;
  }
  auto frame_ptr = frame_it->second;
  BUSTUB_ENSURE(frame_ptr->GetEvictable() == false, "Remove is called on a non-evictable frame");
  auto temp_it = temp_pool_map_.find(frame_id);
  if (temp_it != temp_pool_map_.end()) {
    temp_pool_.erase(temp_it->second);
    temp_pool_map_.erase(temp_it);
  }
  auto cache_it = cache_pool_map_.find(frame_id);
  if (cache_it != cache_pool_map_.end()) {
    cache_pool_.erase(cache_it->second);
    cache_pool_map_.erase(cache_it);
  }
  node_store_.erase(frame_id);
  frame_ptr.reset();
  evictable_size_--;
}

auto LRUKReplacer::Size() -> size_t { return evictable_size_; }

}  // namespace bustub
