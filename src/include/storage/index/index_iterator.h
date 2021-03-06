//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator(Page *page, int index, BufferPoolManager *bpm);
  IndexIterator() = delete;
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();
  IndexIterator &operator=(IndexIterator &&rhs) noexcept {
    this->leaf_ = std::move(rhs.leaf_);
    index_ = rhs.index_;
    bpm_ = rhs.bpm_;
    return *this;
  }
  bool operator==(const IndexIterator &itr) const {
    if (leaf_.is_null() || itr.leaf_.is_null()) {
      return leaf_.is_null() == itr.leaf_.is_null();
    }
    return leaf_->GetPageId() == itr.leaf_->GetPageId() && index_ == itr.index_;
  }

  bool operator!=(const IndexIterator &itr) const { return !operator==(itr); }

 private:
  // add your own private member variables here
  page_ptr<LeafPage> leaf_;
  int index_;
  BufferPoolManager *bpm_;
};

}  // namespace bustub
