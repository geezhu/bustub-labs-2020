//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"
// #define OLD_LOCK
#define NEW_LOCK
namespace bustub {
using std::mutex;
using std::shared_lock;
using std::shared_mutex;
using std::unique_lock;
using std::unique_ptr;
constexpr BPlusTreeOperation INSERT = BPlusTreeOperation::INSERT;
constexpr BPlusTreeOperation SEARCH = BPlusTreeOperation::SEARCH;
constexpr BPlusTreeOperation DELETE = BPlusTreeOperation::DELETE;
constexpr BPlusTreeOperation NONE = BPlusTreeOperation::NONE;
#define SetKeyOfChild(parent, child) ((parent)->SetKeyAt((parent)->ValueIndex((child)->GetPageId()), (child)->KeyAt(0)))
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // use find leaf Page
  page_ptr<LeafPage> leaf = FindLeafPage(key, SEARCH, transaction);
  ValueType val;
  if (leaf->Lookup(key, &val, comparator_)) {
    result->push_back(val);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  assert(transaction->GetPageSet()->empty());
  while (true) {
    try {
      {
        unique_lock<shared_mutex> root_writelock(root_latch_);
        if (root_page_id_ == INVALID_PAGE_ID) {
          StartNewTree(key, value);
          return true;
        }
      }
      return InsertIntoLeaf(key, value, transaction);
    } catch (std::logic_error &E) {
      LOG_DEBUG("%s,redo insert %ld", E.what(), key.ToString());
    }
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  assert(root_page_id_ == INVALID_PAGE_ID);
  page_ptr<LeafPage> new_root_page = make_newpage<LeafPage>(buffer_pool_manager_);
  if (new_root_page.is_null()) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "StartNew tree");
  }
  new_root_page.mark_dirty(true);
  assert(!new_root_page.is_null());
  root_page_id_ = new_root_page.GetPageId();
  UpdateRootPageId();
  new_root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  new_root_page->SetNextPageId(INVALID_PAGE_ID);
  if (new_root_page->Insert(key, value, comparator_) != 1) {
    LOG_DEBUG("Multiple Key&Value Pair in a NewTree");
  }
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  page_ptr<LeafPage> leaf = FindLeafPage(key, INSERT, transaction);
  if (leaf.is_null()) {
    throw std::logic_error("Leaf Is Removed");
  }
  KeyType old_key = leaf->KeyAt(0);
  bool HoldingRoot = HoldingRootPage(leaf.cast(), transaction);
  assert(!leaf.is_null());
  int index = leaf->KeyIndex(key, comparator_);
  if (index < leaf->GetSize() && comparator_(key, leaf->GetItem(index).first) == 0) {
    // key exists
#ifdef OLD_LOCK
    if (leaf->IsRootPage()) {
      root_guard_.unlock();
      LOG_DEBUG("txn %d unlock guard", transaction->GetTransactionId());
    }
#endif
#ifdef NEW_LOCK
    if (HoldingRoot) {
      root_guard_.unlock();
      //      LOG_DEBUG("Insert: txn %d unlock guard",transaction->GetTransactionId());
    }
#endif
    ClearPage(transaction, INSERT);
    return false;
  }
  // must change leaf without duplicate key
  leaf.mark_dirty(true);
  if (leaf->GetSize() == leaf->GetMaxSize() - 1) {
    // after insert size=maxsize
    // and split when size==maxsize
    unique_ptr<page_ptr<LeafPage>> raw_leaf(Split(&leaf));
    page_ptr<LeafPage> &new_leaf = *raw_leaf;
    // write lock to avoid concurrent op
    new_leaf.write_lock();
    // leaf has ceil(size/2) elems
    // newleaf has size-ceil(size/2) elems
    if (comparator_(key, leaf->GetItem(leaf->GetSize() - 1).first) < 0) {
      // less than the last elem in leaf ,insert to leaf ,
      // and move last elem to newleaf to keep satisfy greater than minsize
      leaf->Insert(key, value, comparator_);
      leaf->MoveLastToFrontOf(new_leaf.cast());
    } else {
      // kv can direct insert into newleaf
      new_leaf->Insert(key, value, comparator_);
    }
    // deal with parent
    // when no parent(root page),populate new root
    if (leaf->IsRootPage()) {
      unique_lock<shared_mutex> root_writelock(root_latch_);
      page_ptr<InternalPage> new_root_page = make_newpage<InternalPage>(buffer_pool_manager_);
      assert(!new_root_page.is_null());
      root_page_id_ = new_root_page.GetPageId();
      UpdateRootPageId();
      leaf->SetParentPageId(root_page_id_);
      new_leaf->SetParentPageId(root_page_id_);
      new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      new_root_page->PopulateNewRoot(leaf.GetPageId(), new_leaf->KeyAt(0), new_leaf.GetPageId());
      SetKeyOfChild(new_root_page, leaf);
#ifdef OLD_LOCK
      root_guard_.unlock();
      LOG_DEBUG("txn %d unlock guard", transaction->GetTransactionId());
#endif
      //      new_root_page->SetKeyAt(0,leaf->KeyAt(0));
    } else {
      InsertIntoParent(leaf.cast(), new_leaf->KeyAt(0), new_leaf.cast(), transaction);
    }
#ifdef NEW_LOCK
    if (HoldingRoot) {
      root_guard_.unlock();
      //      LOG_DEBUG("INSERT txn %d unlock guard",transaction->GetTransactionId());
    }
#endif
    return true;
  }
  // when no overflow
  leaf->Insert(key, value, comparator_);
  if (comparator_(key, old_key) < 0 && !leaf->IsRootPage()) {
    InsertIntoParent(leaf.cast(), old_key, leaf.cast(), transaction);
  }
  if (leaf->IsRootPage()) {
//    LOG_DEBUG("leaf root unlock");
#ifdef OLD_LOCK
    root_guard_.unlock();
    LOG_DEBUG("txn %d unlock guard", transaction->GetTransactionId());
#endif
  }
  assert(transaction->GetPageSet()->empty());
#ifdef NEW_LOCK
  if (HoldingRoot) {
    root_guard_.unlock();
    //    LOG_DEBUG("INSERT txn %d unlock guard",transaction->GetTransactionId());
  }
#endif
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  N *new_node = new N(buffer_pool_manager_, INVALID_PAGE_ID, true);
  page_id = (*new_node).GetPageId();
  if ((*node)->IsLeafPage()) {
    page_ptr<LeafPage> &new_page = *reinterpret_cast<page_ptr<LeafPage> *>(new_node);
    page_ptr<LeafPage> &old_page = *reinterpret_cast<page_ptr<LeafPage> *>(node);
    new_page->Init(page_id, old_page->GetParentPageId(), leaf_max_size_);
    old_page->MoveHalfTo(new_page.cast());
    new_page->SetNextPageId(old_page->GetNextPageId());
    old_page->SetNextPageId(new_page->GetPageId());
  } else {
    page_ptr<InternalPage> &new_page = *reinterpret_cast<page_ptr<InternalPage> *>(new_node);
    page_ptr<InternalPage> &old_page = *reinterpret_cast<page_ptr<InternalPage> *>(node);
    new_page->Init(page_id, old_page->GetParentPageId(), internal_max_size_);
    old_page->MoveHalfTo(new_page.cast(), buffer_pool_manager_);
  }
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  assert(old_node->GetParentPageId() == new_node->GetParentPageId());
  // pageset
  // when old_node->page_id==new_node->page_id,need to update old key to key
  const page_id_t old_page_id = old_node->GetPageId();
  const page_id_t new_page_id = new_node->GetPageId();
  page_ptr<InternalPage> old_internal = FetchPage<InternalPage>(transaction, old_node->GetParentPageId(), INSERT);
  // parent internal must change,dirty should be marked
  old_internal.mark_dirty(true);
  const int size = old_internal->GetSize();
  const int max_size = old_internal->GetMaxSize();
  // there is maxsize-1 keys in parent,it's time to overflow
  if (size == max_size && old_page_id != new_page_id) {
    unique_ptr<page_ptr<InternalPage>> raw_internal(Split(&old_internal));
    page_ptr<InternalPage> &new_internal = *raw_internal;
    // key is sorted and unique
    if (comparator_(key, new_internal->KeyAt(0)) < 0) {
      old_internal->InsertNodeAfter(old_page_id, key, new_page_id);
      // deal with first key of old_node
      if (old_node->IsLeafPage()) {
        SetKeyOfChild(old_internal, reinterpret_cast<LeafPage *>(old_node));
        //        old_internal->SetKeyAt(old_internal->ValueIndex(old_page_id),reinterpret_cast<LeafPage*>(old_node)->KeyAt(0));
      } else {
        SetKeyOfChild(old_internal, reinterpret_cast<InternalPage *>(old_node));
        //        old_internal->SetKeyAt(old_internal->ValueIndex(old_page_id),reinterpret_cast<InternalPage
        //        *>(old_node)->KeyAt(0));
      }
      old_internal->MoveLastToFrontOf(new_internal.cast(), new_internal->KeyAt(0), buffer_pool_manager_);
    } else {
      new_internal->InsertNodeAfter(old_page_id, key, new_page_id);
      // deal with first key of old_node
      if (old_node->IsLeafPage()) {
        SetKeyOfChild(new_internal, reinterpret_cast<LeafPage *>(old_node));
        //        new_internal->SetKeyAt(new_internal->ValueIndex(old_page_id),reinterpret_cast<LeafPage*>(old_node)->KeyAt(0));
      } else {
        SetKeyOfChild(new_internal, reinterpret_cast<InternalPage *>(old_node));
        //        new_internal->SetKeyAt(new_internal->ValueIndex(old_page_id),reinterpret_cast<InternalPage
        //        *>(old_node)->KeyAt(0));
      }
      new_node->SetParentPageId(new_internal->GetPageId());
    }
    // deal with parent
    // no parent here(root page)
    if (old_internal->IsRootPage()) {
      unique_lock<shared_mutex> root_writelock(root_latch_);
      page_ptr<InternalPage> new_root_page = make_newpage<InternalPage>(buffer_pool_manager_);
      assert(!new_root_page.is_null());
      root_page_id_ = new_root_page.GetPageId();
      UpdateRootPageId();
      new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      old_internal->SetParentPageId(root_page_id_);
      new_internal->SetParentPageId(root_page_id_);
      new_root_page->PopulateNewRoot(old_internal->GetPageId(), new_internal->KeyAt(0), new_internal->GetPageId());
      // deal with first key of old_internal
      SetKeyOfChild(new_root_page, old_internal);
#ifdef OLD_LOCK
      root_guard_.unlock();
      LOG_DEBUG("txn %d unlock guard", transaction->GetTransactionId());
#endif
      //      new_root_page->SetKeyAt(0,old_internal->KeyAt(0));
    } else {
      InsertIntoParent(old_internal.cast(), new_internal->KeyAt(0), new_internal.cast(), transaction);
    }
  } else if (size < max_size && old_page_id != new_page_id) {
    old_internal->InsertNodeAfter(old_page_id, key, new_page_id);
    if (old_node->IsLeafPage()) {
      SetKeyOfChild(old_internal, reinterpret_cast<LeafPage *>(old_node));
      //      old_internal->SetKeyAt(old_internal->ValueIndex(old_page_id),reinterpret_cast<LeafPage*>(old_node)->KeyAt(0));
    } else {
      SetKeyOfChild(old_internal, reinterpret_cast<InternalPage *>(old_node));
      //      old_internal->SetKeyAt(old_internal->ValueIndex(old_page_id),reinterpret_cast<InternalPage
      //      *>(old_node)->KeyAt(0));
    }
  } else if (old_page_id == new_page_id) {
    // insert corner case
    int key_index = old_internal->KeyIndex(key, comparator_);
    //    KeyType new_key;
    if (old_node->IsLeafPage()) {
      SetKeyOfChild(old_internal, reinterpret_cast<LeafPage *>(old_node));
      //      new_key = reinterpret_cast<LeafPage *>(old_node)->KeyAt(0);
    } else {
      SetKeyOfChild(old_internal, reinterpret_cast<InternalPage *>(old_node));
      //      new_key = reinterpret_cast<InternalPage *>(old_node)->KeyAt(0);
    }
    //    SetKeyOfChild(old_internal,reinterpret_cast<InternalPage *>(old_node));
    //    old_internal->SetKeyAt(key_index, new_key);
    if (key_index == 0 && (!old_internal->IsRootPage())) {
      InsertIntoParent(old_internal.cast(), key, old_internal.cast(), transaction);
    }
  } else {
    LOG_DEBUG("insert when parent->size > parent->maxsize");
  }
#ifdef OLD_LOCK
  if (old_internal->IsRootPage()) {
    root_guard_.unlock();
    LOG_DEBUG("txn %d unlock guard", transaction->GetTransactionId());
  }
#endif
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  {
    shared_lock<shared_mutex> root_readlock(root_latch_);
    if (IsEmpty()) {
      return;
    }
  }
  // fetch page
  page_ptr<LeafPage> leaf = FindLeafPage(key, DELETE, transaction);
  if (leaf.is_null()) {
    // concurrent remove keys might make empty pageset
    LOG_DEBUG("Leaf Is Removed");
    return;
  }
  leaf->RemoveAndDeleteRecord(key, comparator_);
  //  LOG_DEBUG("after remove %d",size);
  leaf.mark_dirty(true);
#ifdef OLD_LOCK
  bool leaf_is_root = leaf->IsRootPage();
#endif
#ifdef NEW_LOCK
  bool HoldingRoot = HoldingRootPage(leaf.cast(), transaction);
#endif
  if (CoalesceOrRedistribute(leaf.cast(), transaction)) {
    leaf.mark_delete(true);
    transaction->AddIntoDeletedPageSet(leaf.GetPageId());
  }
#ifdef OLD_LOCK
  if (leaf_is_root) {
    root_guard_.unlock();
    LOG_DEBUG("txn %d unlock guard", transaction->GetTransactionId());
  }
#endif
#ifdef NEW_LOCK
  if (HoldingRoot) {
    root_guard_.unlock();
    //    LOG_DEBUG("txn %d unlock guard",transaction->GetTransactionId());
  }
#endif
  // set arr[0] in parent
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  // fix parent
  page_ptr<InternalPage> parent_internal = FetchPage<InternalPage>(transaction, node->GetParentPageId(), DELETE);
  int index = parent_internal->ValueIndex(node->GetPageId());
  if (node->GetSize() >= node->GetMinSize()) {
    //    LOG_DEBUG("txn %d Set Key",transaction->GetTransactionId());
    // above minsize
    //    KeyType new_key;
    if (node->IsLeafPage()) {
      SetKeyOfChild(parent_internal, reinterpret_cast<LeafPage *>(node));
      //      new_key = reinterpret_cast<LeafPage *>(node)->KeyAt(0);
    } else {
      SetKeyOfChild(parent_internal, reinterpret_cast<InternalPage *>(node));
      //      new_key = reinterpret_cast<InternalPage *>(node)->KeyAt(0);
    }
    //    SetKeyOfChild(parent_internal,reinterpret_cast<LeafPage *>(node));
    //    parent_internal->SetKeyAt(parent_internal->ValueIndex(node->GetPageId()), new_key);
    if (index == 0 && !parent_internal->IsRootPage()) {
      assert(CoalesceOrRedistribute(parent_internal.cast(), transaction) == false);
    }
#ifdef OLD_LOCK
    if (parent_internal->IsRootPage()) {
      root_guard_.unlock();
      LOG_DEBUG("txn %d unlock guard", transaction->GetTransactionId());
    }
#endif
    return false;
  }
  assert(index != -1);
  int end_index = parent_internal->GetSize() - 1;
  page_ptr<N> candidate_page(nullptr, nullptr, false);
  // page decision(prefer redistribute)
  if (index == 0) {
    // coalesce and redistribute r_sibling
    candidate_page = make_page<N>(buffer_pool_manager_, parent_internal->ValueAt(index + 1));
  } else if (index == end_index) {
    // coalesce and redistribute l_sibling
    candidate_page = make_page<N>(buffer_pool_manager_, parent_internal->ValueAt(index - 1));
    index = -index;  // negative index to mark l sibling
  } else if (index > 0 && index < end_index) {
    // coalesce and redistribute l_sibling or r_sibling
    candidate_page = make_page<N>(buffer_pool_manager_, parent_internal->ValueAt(index - 1));
    index = -index;
    if (candidate_page->GetSize() == candidate_page->GetMinSize()) {
      // now is merge l sibling, but prefer redistribute if r sibling is available
      auto right_sibling = make_page<N>(buffer_pool_manager_, parent_internal->ValueAt((-index) + 1));
      if (candidate_page->GetSize() > candidate_page->GetMinSize()) {
        index = -index;  // positive index to mark r sibling
        candidate_page = std::move(right_sibling);
      }
    }
  }
  candidate_page.write_lock();
  candidate_page.mark_dirty(true);
  // redistribute
  if (candidate_page->GetSize() > candidate_page->GetMinSize()) {
    Redistribute(candidate_page.cast(), node, index);
    if (index >= 0) {
      // right redistribute
      SetKeyOfChild(parent_internal, candidate_page);
      //      parent_internal->SetKeyAt(index + 1, candidate_page->KeyAt(0));
    } else {
      // left redistribute
      SetKeyOfChild(parent_internal, node);
      //      parent_internal->SetKeyAt(-index, node->KeyAt(0));
    }
#ifdef OLD_LOCK
    if (parent_internal->IsRootPage()) {
      root_guard_.unlock();
      LOG_DEBUG("txn %d unlock guard", transaction->GetTransactionId());
    }
#endif
    return false;
  }
  // merge
  N *sibling_page = candidate_page.cast();
  InternalPage *parent_page = parent_internal.cast();
  parent_internal.mark_delete(Coalesce(&sibling_page, &node, &parent_page, index));
  // right coalesce will delete candidate
  // while left will delete node out of this function
  candidate_page.mark_delete(index >= 0);
  // redistribute will not be root page
#ifdef OLD_LOCK
  if (parent_internal->IsRootPage()) {
    root_guard_.unlock();
    LOG_DEBUG("txn %d unlock guard", transaction->GetTransactionId());
  }
#endif
  return index < 0;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  N *source_node;
  N *target_node;
  if (index >= 0) {
    source_node = *neighbor_node;
    target_node = *node;
    index = index + 1;  // remove index for right sibling
  } else {
    source_node = *node;
    target_node = *neighbor_node;
    index = -index;  // remove index for node
  }
  InternalPage *raw_parent_internal = *parent;
  if (source_node->IsLeafPage()) {
    auto source_page = reinterpret_cast<LeafPage *>(source_node);
    auto target_page = reinterpret_cast<LeafPage *>(target_node);
    source_page->MoveAllTo(target_page);
    target_page->SetNextPageId(source_page->GetNextPageId());
    SetKeyOfChild(raw_parent_internal, target_page);
    //    raw_parent_internal->SetKeyAt(index-1,target_page->KeyAt(0));
  } else {
    auto source_page = reinterpret_cast<InternalPage *>(source_node);
    auto target_page = reinterpret_cast<InternalPage *>(target_node);
    source_page->MoveAllTo(target_page, source_page->KeyAt(0), buffer_pool_manager_);
    SetKeyOfChild(raw_parent_internal, target_page);
    //    raw_parent_internal->SetKeyAt(index-1,target_page->KeyAt(0));
  }
  raw_parent_internal->Remove(index);
  //  LOG_DEBUG("size =%d,min=%d",raw_parent_internal->GetSize(),raw_parent_internal->GetMinSize());
  if (raw_parent_internal->GetSize() < raw_parent_internal->GetMinSize()) {
    return CoalesceOrRedistribute(raw_parent_internal, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (node->IsLeafPage()) {
    LeafPage *source_page = reinterpret_cast<LeafPage *>(neighbor_node);
    LeafPage *target_page = reinterpret_cast<LeafPage *>(node);
    if (index >= 0) {
      source_page->MoveFirstToEndOf(target_page);
    } else {
      source_page->MoveLastToFrontOf(target_page);
    }
  } else {
    InternalPage *source_page = reinterpret_cast<InternalPage *>(neighbor_node);
    InternalPage *target_page = reinterpret_cast<InternalPage *>(node);
    if (index >= 0) {
      source_page->MoveFirstToEndOf(target_page, source_page->KeyAt(0), buffer_pool_manager_);
    } else {
      source_page->MoveLastToFrontOf(target_page, target_page->KeyAt(0), buffer_pool_manager_);
    }
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  unique_lock<shared_mutex> root_writelock(root_latch_);
  if (old_root_node->GetSize() >= old_root_node->GetMinSize()) {
    return false;
  }
  if (old_root_node->IsLeafPage()) {
    root_page_id_ = INVALID_PAGE_ID;
  } else {
    InternalPage *rootPage = reinterpret_cast<InternalPage *>(old_root_node);
    root_page_id_ = rootPage->RemoveAndReturnOnlyChild();
    // update  parentPageId
    page_ptr<BPlusTreePage> page = make_page<BPlusTreePage>(buffer_pool_manager_, root_page_id_);
    page->SetParentPageId(INVALID_PAGE_ID);
    page.mark_dirty(true);
  }
  UpdateRootPageId();
  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  return INDEXITERATOR_TYPE(FindLeafPage(KeyType(), true), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *leaf = FindLeafPage(key);
  int index = reinterpret_cast<LeafPage *>(leaf)->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(leaf, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  shared_lock<shared_mutex> root_readlock(root_latch_);
  if (IsEmpty()) {
    return nullptr;
  }
  page_id_t child_page_id = root_page_id_;
  page_ptr<BPlusTreePage> Page = make_page<BPlusTreePage>(buffer_pool_manager_, child_page_id);
  while (!Page->IsLeafPage()) {
    if (leftMost) {
      child_page_id = Page.page_cast<InternalPage>()->ValueAt(0);
    } else {
      child_page_id = Page.page_cast<InternalPage>()->Lookup(key, comparator_);
    }
    Page = make_page<BPlusTreePage>(buffer_pool_manager_, child_page_id);
  }
  return Page.move_page_out();
}
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPageImpl(const KeyType &key, const BPlusTreeOperation type, Transaction *transaction,
                                       bool leftMost) {
  // when search,insert,delete ,need txn ,otherwise no txn need
  assert((type == NONE && transaction == nullptr) || (type != NONE && transaction != nullptr));
  root_guard_.lock();
  //  LOG_DEBUG("txn %d get guard",transaction->GetTransactionId());
  unique_lock<shared_mutex> root_writelock(root_latch_);
  if (IsEmpty()) {
    root_guard_.unlock();
    //    LOG_DEBUG("txn %d unlock guard",transaction->GetTransactionId());
    return nullptr;
  }

  page_id_t child_page_id = root_page_id_;
  page_id_t safe_page_id = INVALID_PAGE_ID;
  bool no_more_release = false;
  auto page_latch_handler = [&](const BPlusTreeOperation type, page_ptr<BPlusTreePage> &Page, const KeyType first_key,
                                const int key_index) -> void {
    if (type != NONE) {
      if (type == SEARCH) {
        Page.read_lock();
      } else {
        Page.write_lock();
      }
      if (!no_more_release) {
        const int size = Page->GetSize();
        const int max_size = (Page->IsLeafPage() ? (Page->GetMaxSize() - 1) : Page->GetMaxSize());  //# of valid key
        const int min_size = Page->GetMinSize();
        if ((type == DELETE && size > min_size) || (type == INSERT && size < max_size) || (type == SEARCH)) {
          safe_page_id = Page->GetPageId();
        }
        // insert corner case ,deal with less than INVALID key(first key)
        if (type == INSERT && comparator_(key, first_key) < 0) {
          safe_page_id = Page->GetParentPageId();
          if (safe_page_id == INVALID_PAGE_ID) {
            // deal with root Page
            safe_page_id = root_page_id_;
          }
          no_more_release = true;
        }
        // delete corner case ,deal with existing key in the parent
        if (type == DELETE && key_index != -1) {
          safe_page_id = Page->GetParentPageId();
          if (safe_page_id == INVALID_PAGE_ID) {
            // deal with root Page
            safe_page_id = root_page_id_;
          }
          no_more_release = true;
        }
      }
      transaction->AddIntoPageSet(Page.force_move_page_out());
    }
  };
  page_ptr<BPlusTreePage> Page = make_page<BPlusTreePage>(buffer_pool_manager_, child_page_id);
  // when no page is safe use the root page
  safe_page_id = root_page_id_;
  while (!Page->IsLeafPage()) {
    if (leftMost) {
      child_page_id = Page.page_cast<InternalPage>()->ValueAt(0);
    } else {
      child_page_id = Page.page_cast<InternalPage>()->Lookup(key, comparator_);
    }
    page_latch_handler(type, Page, Page.page_cast<InternalPage>()->KeyAt(0),
                       Page.page_cast<InternalPage>()->KeyIndex(key, comparator_));
    Page = make_page<BPlusTreePage>(buffer_pool_manager_, child_page_id);
  }
  page_latch_handler(type, Page, Page.page_cast<LeafPage>()->KeyAt(0),
                     Page.page_cast<LeafPage>()->KeyIndex(key, comparator_));
  if (type == NONE) {
    root_guard_.unlock();
    //    LOG_DEBUG("txn %d unlock guard",transaction->GetTransactionId());
    return Page.force_move_page_out();
  }
  if (safe_page_id != root_page_id_) {
    root_guard_.unlock();
    //    LOG_DEBUG("txn %d unlock guard",transaction->GetTransactionId());
  } else if (type == SEARCH) {
    root_guard_.unlock();
    //    LOG_DEBUG("txn %d unlock guard type==SEARCH",transaction->GetTransactionId());
  }
  bustub::Page *page;
  auto page_set = transaction->GetPageSet();
  // release ancestors
  while ((page = page_set->front())->GetPageId() != safe_page_id) {
    Page = page_ptr<BPlusTreePage>(buffer_pool_manager_, page, false);
    if (type == SEARCH) {
      Page.read_unlock();
    } else {
      Page.write_unlock();
    }
    page_set->pop_front();
  }
  //  for (auto& page1 : *page_set) {
  //    LOG_DEBUG("txn %d lock page %d",transaction->GetTransactionId(),page1->GetPageId());
  //  }
  //  LOG_DEBUG("FINAL PAGESET=%zu",transaction->GetPageSet()->size());
  // unlock safe child,only lock the head
  //  auto begin = page_set->begin();
  //  auto end = page_set->end();
  //  assert(begin!=end);
  //  assert((*begin)->GetPageId() == safe_page_id);
  //  begin++;
  //  while (begin != end) {
  //    page = *begin;
  //    if (type == SEARCH) {
  //      page->RUnlatch();
  //    } else {
  //      page->WUnlatch();
  //    }
  //    begin++;
  //  }
  // find page from pageset
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "size=" << leaf->GetSize() << ",max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "size=" << inner->GetSize() << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i >= 0) {
        out << inner->KeyAt(i) << "," << inner->ValueAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}
INDEX_TEMPLATE_ARGUMENTS
template <typename targetPage>
page_ptr<targetPage> BPLUSTREE_TYPE::FetchPage(Transaction *transaction, page_id_t page_id, BPlusTreeOperation type) {
  if (transaction == nullptr) {
    assert(page_id != INVALID_PAGE_ID);
    return page_ptr<targetPage>(buffer_pool_manager_, page_id);
  }
  assert(type != NONE);

  assert(!transaction->GetPageSet()->empty());
  Page *raw_page = transaction->GetPageSet()->back();
  transaction->GetPageSet()->pop_back();
  page_ptr<targetPage> page = page_ptr<targetPage>(buffer_pool_manager_, raw_page, type != SEARCH);
  if (type == SEARCH) {
    page.mark_read_lock(true);
  } else {
    page.mark_write_lock(true);
  }
  return page_ptr<targetPage>(std::move(page));
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ClearPage(Transaction *transaction, BPlusTreeOperation type) {
  if (transaction == nullptr) {
    return;
  }
#ifdef OLD_LOCK
  if (!transaction->GetPageSet()->empty() && transaction->GetPageSet()->front()->GetPageId() == root_page_id_) {
    root_guard_.unlock();
    LOG_DEBUG("txn %d unlock guard", transaction->GetTransactionId());
  }
#endif
  while (!transaction->GetPageSet()->empty() && !FetchPage<Page>(transaction, INVALID_PAGE_ID, type).is_null()) {
  }
}
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::HoldingRootPage(BPlusTreePage *leaf, Transaction *transaction) {
  if (transaction == nullptr) {
    return false;
  }

  if (transaction->GetPageSet()->empty() && leaf != nullptr && leaf->IsRootPage()) {
    return true;
  }
  return !transaction->GetPageSet()->empty() &&
         reinterpret_cast<BPlusTreePage *>(transaction->GetPageSet()->front()->GetData())->IsRootPage();
}
#define LEAF_PAGE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
INDEX_TEMPLATE_ARGUMENTS
page_ptr<LEAF_PAGE> BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, BPlusTreeOperation type, Transaction *transaction,
                                                 bool leftMost) {
  if (transaction == nullptr) {
    return page_ptr<LeafPage>(buffer_pool_manager_, FindLeafPageImpl(key, NONE, nullptr, leftMost), false);
  }
  FindLeafPageImpl(key, type, transaction, leftMost);
  if (transaction->GetPageSet()->empty()) {
    return page_ptr<LeafPage>(nullptr, nullptr, false);
  }
  return FetchPage<LeafPage>(transaction, INVALID_PAGE_ID, type);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
