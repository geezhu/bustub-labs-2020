//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */
// #define INDEX_ARGS KeyType,ValueType,KeyComparator
// INDEX_TEMPLATE_ARGUMENTS
#define PAGE_TEMPLATE_ARGUMENTS template <typename targetPage>
PAGE_TEMPLATE_ARGUMENTS
class page_ptr {
 public:
  page_ptr() = default;
  page_ptr(BufferPoolManager *bpm, page_id_t page_id) : bpm_(bpm), page_(bpm->FetchPage(page_id)) {}
  page_ptr(BufferPoolManager *bpm, page_id_t page_id, bool new_page) : bpm_(bpm) {
    if (new_page) {
      page_ = bpm->NewPage(&page_id);
      // dirty true for new page need to init
      dirty_ = true;
    } else {
      page_ = bpm->FetchPage(page_id);
    }
  }
  page_ptr(BufferPoolManager *bpm, Page *page, bool dirty) : bpm_(bpm), page_(page), dirty_(dirty) {}
  page_ptr(page_ptr &&bp) noexcept
      : bpm_(bp.bpm_), page_(bp.page_), dirty_(bp.dirty_), r_locking(bp.r_locking), w_locking(bp.w_locking) {
    bp.dirty_ = false;
    bp.page_ = nullptr;
    bp.bpm_ = nullptr;
    bp.r_locking = false;
    bp.w_locking = false;
  }
  ~page_ptr() { deconstructor(); }

  targetPage *operator->() {
    if (page_ == nullptr) {
      return nullptr;
    }
    return reinterpret_cast<targetPage *>(page_->GetData());
  }
  const targetPage *operator->() const {
    if (page_ == nullptr) {
      return nullptr;
    }
    return reinterpret_cast<targetPage *>(page_->GetData());
  }
  page_ptr<targetPage> &operator=(page_ptr<targetPage> &&bp) noexcept {
    deconstructor();
    bpm_ = bp.bpm_;
    page_ = bp.page_;
    dirty_ = bp.dirty_;
    delete_ = bp.delete_;
    r_locking = bp.r_locking;
    w_locking = bp.w_locking;
    bp.bpm_ = nullptr;
    bp.page_ = nullptr;
    bp.dirty_ = false;
    bp.delete_ = false;
    bp.r_locking = false;
    bp.w_locking = false;
    return *this;
  }
  void mark_dirty(bool dirty) { dirty_ = dirty_ || dirty; }
  void set_dirty(bool dirty) { dirty_ = dirty; }
  void mark_delete(bool _delete) { delete_ = _delete; }
  bool expect_delete() { return delete_; }
  Page *get_page() { return page_; }
  bool is_null() const { return page_ == nullptr; }
  bool is_null() { return page_ == nullptr; }
  page_id_t GetPageId() {
    if (is_null()) {
      return -1;
    }
    return page_->GetPageId();
  }
  //  LeafPage * cast_leaf(){
  //    return reinterpret_cast<LeafPage*>(page_);
  //  }
  //  InternalPage* cast_internal(){
  //    return (reinterpret_cast<InternalPage*>(page_));
  //  }
  targetPage *cast() { return (reinterpret_cast<targetPage *>(page_->GetData())); }
  Page *move_page_out() {
    assert(!dirty_);
    assert(!delete_);
    assert(!r_locking);
    assert(!w_locking);
    Page *page = page_;
    page_ = nullptr;
    bpm_ = nullptr;
    return page;
  }
  Page *force_move_page_out() {
    assert(!dirty_);
    assert(!delete_);
    r_locking = false;
    w_locking = false;
    Page *page = page_;
    page_ = nullptr;
    bpm_ = nullptr;
    return page;
  }
  template <typename PageType>
  PageType *page_cast() {
    return (reinterpret_cast<PageType *>(page_->GetData()));
  }
  void flush() {
    if (page_ != nullptr && bpm_ != nullptr) {
      bpm_->FlushPage(page_->GetPageId());
    }
  }
  void read_lock() {
    page_->RLatch();
    r_locking = true;
  }
  void mark_read_lock(bool r) { r_locking = r; }
  void read_unlock() {
    if (page_ == nullptr) {
      return;
    }
    page_->RUnlatch();
    r_locking = false;
  }
  void write_lock() {
    if (page_ == nullptr) {
      return;
    }
    page_->WLatch();
    w_locking = true;
  }
  void mark_write_lock(bool w) { w_locking = w; }
  void write_unlock() {
    page_->WUnlatch();
    w_locking = false;
  }

 private:
  void deconstructor() {
    if (w_locking) {
      page_->WUnlatch();
    }
    if (r_locking) {
      page_->RUnlatch();
    }
    if (page_ != nullptr && bpm_ != nullptr) {
      page_id_t pageId = page_->GetPageId();
      bpm_->UnpinPage(pageId, dirty_);
      if (delete_) {
        bpm_->DeletePage(pageId);
      }
    }
  }
  BufferPoolManager *bpm_ = nullptr;
  Page *page_ = nullptr;
  bool dirty_ = false;
  bool delete_ = false;
  bool r_locking = false;
  bool w_locking = false;
};
PAGE_TEMPLATE_ARGUMENTS
page_ptr<targetPage> make_page(BufferPoolManager *bpm, page_id_t page_id) { return page_ptr<targetPage>(bpm, page_id); }
PAGE_TEMPLATE_ARGUMENTS
page_ptr<targetPage> make_newpage(BufferPoolManager *bpm) { return page_ptr<targetPage>(bpm, INVALID_PAGE_ID, true); }
class BPlusTreePage {
 public:
  bool IsLeafPage() const;
  bool IsRootPage() const;
  void SetPageType(IndexPageType page_type);

  int GetSize() const;
  void SetSize(int size);
  void IncreaseSize(int amount);

  int GetMaxSize() const;
  void SetMaxSize(int max_size);
  int GetMinSize() const;

  page_id_t GetParentPageId() const;
  void SetParentPageId(page_id_t parent_page_id);

  page_id_t GetPageId() const;
  void SetPageId(page_id_t page_id);

  void SetLSN(lsn_t lsn = INVALID_LSN);

 private:
  // member variable, attributes that both internal and leaf page share
  IndexPageType page_type_ __attribute__((__unused__));
  lsn_t lsn_ __attribute__((__unused__));
  int size_ __attribute__((__unused__));
  int max_size_ __attribute__((__unused__));
  page_id_t parent_page_id_ __attribute__((__unused__));
  page_id_t page_id_ __attribute__((__unused__));
};

}  // namespace bustub
