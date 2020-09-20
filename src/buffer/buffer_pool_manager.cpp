//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  LOG_DEBUG("Fetching page %d", page_id);
  // Search the page table for the requested page P.
  // If P exists, pin it and return it immediately.
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t fid = page_table_.at(page_id);
    pages_[fid].pin_count_++;
    if (pages_[fid].pin_count_ == 1)
      replacer_->Pin(fid);
    latch_.unlock();
    return &pages_[fid];
  }
  // If P does not exist, find a replacement page (R) from either the free list or the replacer.
  // Note that pages are always found from the free list first.
  frame_id_t victim;
  if (!FindVictim(&victim)) {
    latch_.unlock();
    return nullptr;
  }
  // If R is dirty, write it back to the disk.
  if (pages_[victim].is_dirty_) {
    disk_manager_->WritePage(pages_[victim].page_id_, pages_[victim].data_);
    pages_[victim].is_dirty_ = false;
  }
  // Delete R from the page table and insert P.
  page_table_.erase(pages_[victim].page_id_);
  page_table_[page_id] = victim;
  pages_[victim].page_id_ = page_id;
  pages_[victim].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[victim].data_);
  latch_.unlock();
  return &pages_[victim];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  LOG_DEBUG("Unpinning page %d, is_dirty=%d", page_id, is_dirty);
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t fid = page_table_.at(page_id);
    if (pages_[fid].pin_count_ > 0) {
      pages_[fid].pin_count_--;
      if (pages_[fid].pin_count_ == 0)
        replacer_->Unpin(fid);
    }
    pages_[fid].is_dirty_ = is_dirty;
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  latch_.lock();
  // Make sure you call DiskManager::WritePage!
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t fid = page_table_.at(page_id);
    if (pages_[fid].is_dirty_) {
      disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
      pages_[fid].is_dirty_ = false;
      latch_.unlock();
      return true;
    }
  }
  LOG_DEBUG("Flushed page %d to disk", page_id);
  latch_.unlock();
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  LOG_DEBUG("Creating new page");
  // Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t victim;
  if (!FindVictim(&victim)) {
    // If all the pages in the buffer pool are pinned, return nullptr.
    latch_.unlock();
    LOG_DEBUG("Failed to create new page, buffer pool full");
    return nullptr;
  }
  // If P is dirty, write it back to the disk.
  if (pages_[victim].is_dirty_) {
    disk_manager_->WritePage(pages_[victim].page_id_, pages_[victim].data_);
    pages_[victim].is_dirty_ = false;
  }
  // Set the page ID output parameter.
  *page_id = disk_manager_->AllocatePage();
  // Update P's metadata, zero out memory and add P to the page table.
  page_table_.erase(pages_[victim].page_id_);
  pages_[victim].ResetMemory();
  pages_[victim].page_id_ = *page_id;
  pages_[victim].pin_count_ = 1;
  replacer_->Pin(victim);
  page_table_[*page_id] = victim;
  //  Return a pointer to P.
  LOG_DEBUG("Created new page %d", *page_id);
  latch_.unlock();
  return &pages_[victim];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  LOG_DEBUG("Deleting page %d", page_id);
  // Search the page table for the requested page (P).
  if (page_table_.find(page_id) != page_table_.end()) {
    // If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    frame_id_t fid = page_table_.at(page_id);
    if (pages_[fid].pin_count_ > 0) {  // pin count is not zero
      latch_.unlock();
      return false;
    } else {    // pin count is zero
      // P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
      replacer_->Unpin(fid);
      disk_manager_->DeallocatePage(page_id);
      page_table_.erase(page_id);
      pages_[fid].ResetMemory();
      pages_[fid].page_id_ = INVALID_PAGE_ID;
      pages_[fid].pin_count_ = 0;
      pages_[fid].is_dirty_ = false;
      free_list_.emplace_back(fid);
      latch_.unlock();
      return true;
    }
  }
  latch_.unlock();
  // If P does not exist, return true.
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  latch_.lock();
  LOG_DEBUG("Flushing all pages");
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID && pages_[i].is_dirty_) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
  }
  latch_.unlock();
}

// must be called while holding latch
bool BufferPoolManager::FindVictim(frame_id_t *victim) {
  bool success = false;
  if (!free_list_.empty()) {  // find from free list
    *victim = free_list_.front();
    free_list_.pop_front();
    success = true;
    LOG_DEBUG("Found free page %d", *victim);
  } else {  // find from replacer
    success = replacer_->Victim(victim);
    if (success) {
      LOG_DEBUG("Found victim %d", *victim);
    } else {
      LOG_DEBUG("No victim found");
    }
  }
  return success;
}

// must be called while holding latch
void BufferPoolManager::DebugPrint() {
  return;
  LOG_DEBUG("Page table:");
  for (auto& t : page_table_) {
    LOG_DEBUG("%d -> %d", t.first, t.second);
  }
  LOG_DEBUG("frame\tpage_id\tpin_count\tis_dirty");
  for (size_t i = 0; i < pool_size_; i++) {
    LOG_DEBUG("%lu\t%d\t%d\t%d", i, pages_[i].page_id_, pages_[i].pin_count_, pages_[i].is_dirty_);
  }
}

}  // namespace bustub
