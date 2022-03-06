//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include <cmath>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetLSN();
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  // use binary search
  for (int i = 0, size = GetSize(); i < size; ++i) {
    if (comparator(key, array[i].first) <= 0) {
      return i;
    }
  }
  return GetSize();
  //  int left = 0;
  //  int right = GetSize();
  //  int mid;
  //  while(left<right)
  //  {
  //    mid=left+(right-left)/2;
  //    if(comparator(array[mid].first,key)>=0){
  //      right=mid;//if eq there might another eq
  //    }
  //    else{
  //      left=mid+1;//if less,move to right
  //    }
  //  }
  //  return left;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index < GetSize());
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  assert(index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // maybe max-1 for leaf
  assert(GetSize() + 1 <= GetMaxSize() - 1);
  auto index = KeyIndex(key, comparator);
  RightShift(index, 1);
  array[index] = {key, value};
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  // maybe max-1 for leaf
  assert(GetSize() == GetMaxSize() - 1);
  int size = ceil(GetSize() / 2.0);
  recipient->CopyNFrom(array + size, GetSize() - size);
  IncreaseSize(size - GetSize());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  assert(GetSize() == 0);
  Copy(0, items, size);
  SetSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  //  binary_search
  for (int i = 0, size = GetSize(); i < size; ++i) {
    switch (comparator(key, array[i].first)) {
      case -1:
        return false;
      case 0:
        *value = array[i].second;
        return true;
      case 1:
        break;
    }
  }
  return false;
  //  auto index = KeyIndex(key, comparator);
  //  assert(value != nullptr);
  //  *value = array[index].second;
  //  return comparator(array[index].first, key) == 0;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  for (int i = 0, size = GetSize(); i < size; ++i) {
    switch (comparator(key, array[i].first)) {
      case -1:
        return GetSize();
      case 0:
        LeftShift(i, 1);
        return GetSize();
      case 1:
        break;
    }
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  // maybe max-1 for leaf
  assert(recipient->GetSize() + GetSize() <= recipient->GetMaxSize() - 1);
  recipient->Copy(recipient->GetSize(), array, GetSize());
  recipient->IncreaseSize(GetSize());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  assert(GetSize() > GetMinSize());
  recipient->CopyLastFrom(array[0]);
  LeftShift(0, 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  // maybe max-1 for leaf
  assert(GetSize() + 1 <= GetMaxSize() - 1);
  auto index = GetSize();
  array[index] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  // maybe max-1 for leaf
  assert(recipient->GetSize() + 1 <= recipient->GetMaxSize());
  //   replace recipient's parent entry: middle_key=array[GetSize()-1].first
  assert(GetSize() > GetMinSize());
  recipient->CopyFirstFrom(array[GetSize() - 1]);
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  assert(GetSize() + 1 <= GetMaxSize());
  RightShift(0, 1);
  array[0] = item;
  // no need to modify the array[1].first
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::LeftShift(int index, int size) {
  for (int i = index; i < GetSize() - size; ++i) {
    array[i] = array[i + size];
  }
  IncreaseSize(-size);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RightShift(int index, int size) {
  assert(GetSize() + size <= GetMaxSize());
  for (int i = GetSize() + size - 1; i >= size + index; --i) {
    array[i].first = array[i - size].first;
    array[i].second = array[i - size].second;
  }
  IncreaseSize(size);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Copy(int startIndex, MappingType *items, int size) {
  assert(startIndex + size <= GetMaxSize());
  for (int i = 0; i < size; ++i) {
    array[startIndex + i] = items[i];
  }
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
