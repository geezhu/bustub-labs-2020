/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *page, int index, BufferPoolManager *bpm)
    : leaf_(page_ptr<LeafPage>(bpm, page, false)), index_(index), bpm_(bpm) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  assert(!leaf_.is_null());
  return leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize() - 1;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  assert(!leaf_.is_null());
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (leaf_.is_null()) {
  } else if (index_ != leaf_->GetSize() - 1) {
    index_++;
  } else if (isEnd()) {
    leaf_ = page_ptr<LeafPage>(nullptr, nullptr, false);
    index_ = 0;
  } else {
    index_ = 0;
    // const dereference ,no modification there
    leaf_ = make_page<LeafPage>(bpm_, leaf_->GetNextPageId());
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
