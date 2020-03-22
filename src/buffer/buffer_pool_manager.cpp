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
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() == page_id) {
      // P exists, pin it and return
      pages_[i].pin_count_++;
      replacer_->Pin(i);
      return &pages_[i];
    }
  }
  // not found, need to find replacement
  frame_id_t victim;
  if (!FindVictim(&victim)) {
    return nullptr;
  }
  if (pages_[victim].IsDirty()) {
    FlushPageImpl(pages_[victim].GetPageId());   // page is dirty, write back to disk
  }
  pages_[victim].page_id_ = page_id;
  disk_manager_->ReadPage(page_id, pages_[victim].GetData());

  return &pages_[victim];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() == page_id) {
      if (pages_[i].GetPinCount() <= 0) {
        return false;
      } else {
        pages_[i].pin_count_ = pages_[i].pin_count_ > 0? pages_[i].pin_count_ - 1 : 0;
      }
      if (pages_[i].pin_count_ == 0) {
        replacer_->Unpin(i);
      }
      return true;
    }
  }
  return false;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == page_id && pages_[i].IsDirty()) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
      return true;
    }
  }
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t victim;
  if (!FindVictim(&victim)) {
    return nullptr;
  }
  pages_[victim].ResetMemory();
  page_id_t new_page_id = disk_manager_->AllocatePage();
  pages_[victim].page_id_ = new_page_id;
  pages_[victim].pin_count_++;
  *page_id = new_page_id;
  return &pages_[victim];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == page_id) {  // P exist
      if (pages_[i].GetPinCount() > 0) {  // pin count is non-zero
        return false;
      } else {    // pin count is zero
        disk_manager_->DeallocatePage(page_id);
        pages_[i].ResetMemory();
        free_list_.emplace_back(i);
        return true;
      }
    }
  }
  
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID && pages_[i].IsDirty()) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
    }
  }
}

bool BufferPoolManager::FindVictim(frame_id_t *victim) {
  bool success = false;
  if (!free_list_.empty()) {  // find from free list
    *victim = free_list_.front();
    free_list_.pop_front();
    success = true;
  } else {  // find from replacer
    if (replacer_->Victim(victim)) {
      success = true;
    }
  }
  return success;
}

}  // namespace bustub
