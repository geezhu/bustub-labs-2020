//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include <cmath>
#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"
namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetLSN();
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = GetSize() - 1; i >= 0; --i) {
    if (array[i].second == value) {
      return i;
    }
  }
  LOG_DEBUG("warning: index of value %d not found", value);
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(index < GetSize());
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // use binary search
  assert(GetSize() >= GetMinSize());
  auto binary_search = [this, &comparator, &key]() {
    int left = 1;
    int max = GetSize() - 1;
    int right = max;
    //    int max=GetSize();
    int mid;
    if (comparator(key, array[left].first) < 0) {
      return left - 1;
    }
    if (comparator(key, array[right].first) >= 0) {
      return right;
    }
    while (left < right) {
      mid = (left + right) / 2;
      if (comparator(key, array[mid].first) == 0) {
        return mid;
      }
      if (comparator(key, array[mid - 1].first) == 0) {
        return mid - 1;
      }
      if (comparator(key, array[mid + 1].first) < 0 && comparator(key, array[mid].first) >= 0) {
        return mid;
      }
      if (comparator(key, array[mid].first) < 0 && (comparator(key, array[mid - 1].first) >= 0)) {
        return mid - 1;
      }
      if (comparator(key, array[mid].first) > 0) {
        left = mid + 1;
      }
      if (comparator(key, array[mid - 1].first) < 0) {
        right = mid - 1;
      }
    }
    throw Exception("fail!");
  };
  return array[binary_search()].second;
//    int left = 1;
//    int right = GetSize();
//    int mid;
//    while(left<right)
//    {
//      mid=left+(right-left)/2;
//      if(comparator(array[mid].first,key)>=0)
//        right=mid;//如果相等的话，前面可能还会有所以往前，大于也要往前
//      else
//        left=mid+1;//小于说明要往后
//    }
//    if (comparator(array[left].first,key)!=0){
//      left--;
//    }
//    return array[left].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  assert(GetSize() == 0);
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  IncreaseSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  //  assert(GetSize() >= GetMinSize());
  int old_index = ValueIndex(old_value);
  assert(old_index != -1);
  //  assert(index != -1);
  assert(GetSize() + 1 <= GetMaxSize());
  //  auto right_shift=[this](int index,int step){
  //    assert(GetSize()+step<GetMaxSize());
  //    auto copy_index=GetSize()-1+step;
  //    auto stop_index=index+step-1;
  //    while (copy_index!=stop_index){
  //      array[copy_index]=array[copy_index-step];
  //      copy_index--;
  //    }
  //    IncreaseSize(step);
  //  };
  RightShift(old_index + 1, 1);
  array[old_index + 1] = {new_key, new_value};
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() >= GetMinSize());
  int size = ceil(GetSize() / 2.0);
  recipient->CopyNFrom(array + size, GetSize() - size, buffer_pool_manager);
  IncreaseSize(size - GetSize());
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() == 0);
  Copy(0, items, size);
  SetSize(size);
  for (int i = 0; i < size; ++i) {
    ChangeParentId(array[i].second, buffer_pool_manager);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(index < GetSize());
  LeftShift(index, 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  assert(GetSize() == 1);
  SetSize(0);
  return array[0].second;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  //  move to left sibling
  //  change parent id of its all subnode(leaf or internal node),and write to disk.
  array[0].first = middle_key;  // maybe unused if array[0] is used for middle_key
  recipient->Copy(recipient->GetSize(), array, GetSize());
  for (int i = 0; i < GetSize(); ++i) {
    recipient->ChangeParentId(array[i].second, buffer_pool_manager);
  }
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  // redistribute to left sibling
  assert(GetSize() > GetMinSize());
  array[0].first = middle_key;  // maybe unused if array[0] is used for middle_key
  recipient->CopyLastFrom(array[0], buffer_pool_manager);
  LeftShift(0, 1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 < GetMaxSize());
  auto index = GetSize();
  array[index] = pair;
  //  change parent id of its all subnode(leaf or internal node),and write to disk.
  ChangeParentId(pair.second, buffer_pool_manager);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(array[GetSize() - 1], buffer_pool_manager);
  //  opt replace parent : middle_key=array[GetSize()-1].first=recipient.array[0].first;
  //  maybe unused for saving middle key
  recipient->array[1].first = middle_key;
  assert(GetSize() - 1 >= GetMinSize());
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 < GetMaxSize());
  //  need to modify the array[1].first
  RightShift(0, 1);
  array[0] = pair;
  //  change parent id of its all subnode(leaf or internal node).
  ChangeParentId(pair.second, buffer_pool_manager);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::LeftShift(int index, int size) {
  //  assert(GetSize() - size >= GetMinSize());
  for (int i = index; i < GetSize() - size; ++i) {
    array[i] = array[i + size];
  }
  IncreaseSize(-size);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RightShift(int index, int size) {
  assert(GetSize() + size <= GetMaxSize());
  for (int i = GetSize() + size - 1; i >= size + index; --i) {
    array[i] = array[i - size];
  }
  IncreaseSize(size);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Copy(int startIndex, std::pair<KeyType, ValueType> *items, int size) {
  assert(startIndex + size <= GetMaxSize());
  for (int i = 0; i < size; ++i) {
    array[startIndex + i] = items[i];
  }
  IncreaseSize(size);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ChangeParentId(page_id_t page_id, BufferPoolManager *buffer_pool_manager) {
  page_ptr<B_PLUS_TREE_INTERNAL_PAGE_TYPE> page =
      make_page<B_PLUS_TREE_INTERNAL_PAGE_TYPE>(buffer_pool_manager, page_id);
  page->SetParentPageId(GetPageId());
  page.mark_dirty(true);
}
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int left = 0;
  int right = GetSize() - 1;
  int mid;
  int cmp;
  while (left <= right) {
    mid = (left + right) / 2;
    cmp = comparator(key, array[mid].first);
    if (cmp == 0) {
      return mid;
    }
    if (cmp > 0) {
      left = mid + 1;
    }
    if (cmp < 0) {
      right = mid - 1;
    }
  }
  return -1;
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
