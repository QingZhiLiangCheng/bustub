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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // 上锁
  std::scoped_lock<std::mutex> lock(latch_);

  bool page_find = false;

  if (!history_list_.empty()) {
    // FIFO history list
    // 淘汰 末尾 第一个 evictable 为 false的
    for (auto it = history_list_.rbegin(); it != history_list_.rend(); ++it) {
      if (entries_[*it].is_evictable_) {
        *frame_id = *it;
        history_list_.erase(std::next(it).base());
        page_find = true;
        break;
      }
    }
  }

  if (!page_find && !lru_list_.empty()) {
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
      if (entries_[*it].is_evictable_) {
        *frame_id = *it;
        lru_list_.erase(std::next(it).base());
        page_find = true;
        break;
      }
    }
  }

  if (page_find) {
    entries_.erase(*frame_id);
    --curr_size_;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 上锁
  std::scoped_lock<std::mutex> lock(latch_);
  // 合法性 特殊情况判断
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument("Invalid frame_id");
  }

  size_t new_count = ++entries_[frame_id].hit_count_;
  if (new_count == 1) {
    // 新插入
    history_list_.emplace_front(frame_id);
    entries_[frame_id].pos_ = history_list_.begin();
  } else {
    /*if (new_count > 1 && new_count < k_) {
      // 在history list中转移
      history_list_.erase(entries_[frame_id].pos_);
      history_list_.emplace_front(frame_id);
      entries_[frame_id].pos_ = history_list_.begin();
    } else*/
    if (new_count == k_) {
      // 转移
      history_list_.erase(entries_[frame_id].pos_);
      lru_list_.emplace_front(frame_id);
      entries_[frame_id].pos_ = lru_list_.begin();
    } else if (new_count > k_) {
      // 在lru list中转移
      lru_list_.erase(entries_[frame_id].pos_);
      lru_list_.emplace_front(frame_id);
      entries_[frame_id].pos_ = lru_list_.begin();
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // 上锁
  std::scoped_lock<std::mutex> lock(latch_);
  // 合法性 特殊情况判断
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument("Invalid frame_id");
  }
  if (entries_.find(frame_id) == entries_.end()) {
    return;
  }

  if (!entries_[frame_id].is_evictable_) {
    throw std::logic_error("can't remove this frame");
  }
  if (entries_[frame_id].hit_count_ < k_) {
    history_list_.erase(entries_[frame_id].pos_);
  } else {
    lru_list_.erase(entries_[frame_id].pos_);
  }
  --curr_size_;
  entries_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // 上锁
  std::scoped_lock<std::mutex> lock(latch_);
  // 合法性 特殊情况判断
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument("Invalid frame_id");
  }
  if (entries_.find(frame_id) == entries_.end()) {
    return;
  }
  bool current_evictable = entries_[frame_id].is_evictable_;
  if (!current_evictable && set_evictable) {
    curr_size_++;
  } else if (current_evictable && !set_evictable) {
    curr_size_--;
  }
  entries_[frame_id].is_evictable_ = set_evictable;
}

}  // namespace bustub