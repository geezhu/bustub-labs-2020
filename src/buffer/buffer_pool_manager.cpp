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
using unique_lock = std::unique_lock<std::mutex>;
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
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
  assert(page_id != INVALID_PAGE_ID);
  unique_lock lock(latch_);
  auto get_frame = [this]() -> frame_id_t {
    if (free_list_.empty()) {
      return -1;
    }
    frame_id_t frameId = free_list_.front();
    free_list_.pop_front();
    return frameId;
  };
  Page *page = nullptr;
  if (page_table_.count(page_id) == 0) {
    frame_id_t frameId = get_frame();
    if (frameId == -1) {
      // LRU
      if (replacer_->Victim(&frameId)) {
        page = &pages_[frameId];
        if (page->is_dirty_) {
          disk_manager_->WritePage(page->page_id_, page->data_);
        }
        page_table_.erase(page->page_id_);
      } else {
        return page;
      }
    }
    page = &pages_[frameId];
    page->ResetMemory();
    page->page_id_ = page_id;
    page->pin_count_ = 0;
    page->is_dirty_ = false;
    // read data
    disk_manager_->ReadPage(page_id, page->data_);
    // set pagetable
    page_table_[page_id] = frameId;
  }
  page = &pages_[page_table_[page_id]];
  if (page->pin_count_ == 0) {
    replacer_->Pin(page_table_[page_id]);
  }
  page->pin_count_ += 1;
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  assert(page_id != INVALID_PAGE_ID);
  unique_lock page_lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frameId = page_table_[page_id];
  auto &page = pages_[frameId];
  if (page.pin_count_ == 0) {
    return false;
  }
  page.is_dirty_ = is_dirty || page.is_dirty_;
  if (page.pin_count_ == 1) {
    replacer_->Unpin(frameId);
  }
  page.pin_count_ -= 1;
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  assert(page_id != INVALID_PAGE_ID);
  unique_lock page_lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frameId = page_table_[page_id];
  auto &page = pages_[frameId];
  page.WLatch();
  // do not use dirty when flush
  disk_manager_->WritePage(page_id, page.data_);
  page.WUnlatch();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  //  unique_lock page_lock(latch_[map(page_id)]);
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  unique_lock lock(latch_);
  if (replacer_->Size() == 0 && free_list_.empty()) {
    return nullptr;
  }
  Page *page = nullptr;
  frame_id_t frameId;
  if (!free_list_.empty()) {
    frameId = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Victim(&frameId)) {
    // DONE: Write Old Page To disk,if is dirty
    // pincount ==0 ,no worry about concurrent
    page = &pages_[frameId];
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
    }
    page_table_.erase(page->page_id_);
  } else {
    return page;
  }
  *page_id = disk_manager_->AllocatePage();
  page = &pages_[frameId];
  page->ResetMemory();
  page->page_id_ = *page_id;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->pin_count_ += 1;
  page_table_[*page_id] = frameId;
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  assert(page_id != INVALID_PAGE_ID);
  unique_lock lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frameId = page_table_[page_id];
  Page *page = &pages_[frameId];
  if (page->pin_count_ != 0) {
    return false;
  }
  replacer_->Pin(frameId);
  // when pin count==0 no need to lock
  disk_manager_->DeallocatePage(page->page_id_);
  page_table_.erase(page->page_id_);
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->ResetMemory();
  free_list_.emplace_back(frameId);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  unique_lock lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    auto &page = pages_[i];
    page.WLatch();
    if (page.page_id_ != INVALID_PAGE_ID) {
      // do not use dirty when flush
      disk_manager_->WritePage(page.page_id_, page.data_);
    }
    page.WUnlatch();
  }
}

}  // namespace bustub
