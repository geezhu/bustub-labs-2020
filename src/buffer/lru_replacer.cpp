//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <cassert>
namespace bustub {
using namespace std;
using unique_lock = std::unique_lock<std::mutex>;
LRUReplacer::LRUReplacer(size_t num_pages) {
  head_.next=&head_;
  head_.prev=&head_;
  empty_list_= make_unique<double_linklist[]>(num_pages);
  npages_=0;
  for (size_t i = 0; i < num_pages; ++i) {
    empty_list_[i].next= nullptr;
    empty_list_[i].prev= nullptr;
  }
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  assert(frame_id!= nullptr);
  unique_lock lock(latch_);
  if(head_.prev!=&head_){
    *frame_id=head_.prev-&empty_list_[0];
    //a.k.a Pin(*frame_id) without lock
    empty_list_[*frame_id].prev->next=empty_list_[*frame_id].next;
    empty_list_[*frame_id].next->prev=empty_list_[*frame_id].prev;
    empty_list_[*frame_id].next= nullptr;
    empty_list_[*frame_id].prev= nullptr;
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  assert(frame_id>=0);
  unique_lock lock(latch_);
  if(empty_list_[frame_id].next== nullptr||empty_list_[frame_id].prev== nullptr){
    return;
  }
  assert(empty_list_[frame_id].next!= nullptr);
  assert(empty_list_[frame_id].prev!= nullptr);
  empty_list_[frame_id].prev->next=empty_list_[frame_id].next;
  empty_list_[frame_id].next->prev=empty_list_[frame_id].prev;
  empty_list_[frame_id].next= nullptr;
  empty_list_[frame_id].prev= nullptr;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  assert(frame_id>=0);
  unique_lock lock(latch_);
  if(empty_list_[frame_id].next!= nullptr && empty_list_[frame_id].next!= nullptr){
    return;
  }
  assert(empty_list_[frame_id].next== nullptr);
  assert(empty_list_[frame_id].prev== nullptr);
  empty_list_[frame_id].next=head_.next;
  empty_list_[frame_id].prev=&head_;
  head_.next->prev=&empty_list_[frame_id];
  head_.next=&empty_list_[frame_id];
  npages_+=1;
}

size_t LRUReplacer::Size() { return npages_; }

}  // namespace bustub
