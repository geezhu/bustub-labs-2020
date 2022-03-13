//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <set>
#include <utility>
#include <vector>
#include "concurrency/transaction_manager.h"

namespace bustub {
using std::set;
using std::vector;
auto ThrowTransactionException(Transaction *txn, const AbortReason &reason) -> void {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), reason);
};
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  auto IsoLevel = txn->GetIsolationLevel();
  auto TxnState = txn->GetState();
  if (IsoLevel == READ_UNCOMMITTED) {
    ThrowTransactionException(txn, LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  // for read uncommitted ,no shrinking limit
  // for read committed ,no shrinking limit
  // for repeatable read ,need shrinking limit
  if (IsoLevel == REPEATABLE_READ && TxnState != GROWING) {
    ThrowTransactionException(txn, LOCK_ON_SHRINKING);
  }
  auto XLockSet = txn->GetExclusiveLockSet();
  auto SLockSet = txn->GetSharedLockSet();
  if (XLockSet->count(rid) != 0 || SLockSet->count(rid) != 0) {
    ThrowTransactionException(txn, DEADLOCK);
  }
  SLockSet->emplace(rid);
  if (lock_table_.count(rid) == 0) {
    lock_table_.try_emplace(rid);
  }
  txn_id_t txnId = txn->GetTransactionId();
  lock_table_[rid].request_queue_.emplace_back(txnId, SHARED);
  lock_table_[rid].cv_.wait(latch, [this, txn, &rid]() -> bool { return shared_predicate(txn, rid); });
  if (txn->GetState() == ABORTED) {
    ThrowTransactionException(txn, DEADLOCK);
  }
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  auto IsoLevel = txn->GetIsolationLevel();
  auto TxnState = txn->GetState();
  // for read uncommitted ,no shrinking limit
  // for read committed ,no shrinking limit
  // for repeatable read ,need shrinking limit
  if (IsoLevel == REPEATABLE_READ && TxnState != GROWING) {
    ThrowTransactionException(txn, LOCK_ON_SHRINKING);
  }
  auto XLockSet = txn->GetExclusiveLockSet();
  auto SLockSet = txn->GetSharedLockSet();
  if (XLockSet->count(rid) != 0 || SLockSet->count(rid) != 0) {
    ThrowTransactionException(txn, DEADLOCK);
  }
  XLockSet->emplace(rid);
  txn_id_t txnId = txn->GetTransactionId();
  if (lock_table_.count(rid) == 0) {
    lock_table_.try_emplace(rid);
  }
  lock_table_[rid].request_queue_.emplace_back(txnId, EXCLUSIVE);
  lock_table_[rid].cv_.wait(latch, [this, txn, &rid]() -> bool { return unique_predicate(txn, rid); });
  if (txn->GetState() == ABORTED) {
    ThrowTransactionException(txn, DEADLOCK);
  }
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  auto IsoLevel = txn->GetIsolationLevel();
  auto TxnState = txn->GetState();
  auto XLockSet = txn->GetExclusiveLockSet();
  auto SLockSet = txn->GetSharedLockSet();
  assert(lock_table_.count(rid) != 0);
  if (IsoLevel == READ_UNCOMMITTED) {
    ThrowTransactionException(txn, LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (IsoLevel == REPEATABLE_READ && TxnState != GROWING) {
    ThrowTransactionException(txn, LOCK_ON_SHRINKING);
  }
  if (lock_table_[rid].upgrading_) {
    ThrowTransactionException(txn, UPGRADE_CONFLICT);
  }
  if (lock_table_.count(rid) == 0 && SLockSet->count(rid) == 0) {
    return false;
    //    ThrowTransactionException(txn,UNLOCK_ON_SHRINKING);
  }
  if (XLockSet->count(rid) != 0) {
    ThrowTransactionException(txn, DEADLOCK);
  }
  lock_table_[rid].upgrading_ = true;
  SLockSet->erase(rid);
  XLockSet->emplace(rid);
  txn_id_t txnId = txn->GetTransactionId();
  lock_table_[rid].request_queue_.emplace_back(txnId, EXCLUSIVE);
  EraseLockRequest(txnId, rid, true);
  lock_table_[rid].cv_.wait(latch, [this, txn, &rid]() -> bool { return unique_predicate(txn, rid); });
  if (txn->GetState() == ABORTED) {
    ThrowTransactionException(txn, DEADLOCK);
  }
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  return UnlockWithoutLatch(txn, rid);
}
bool LockManager::UnlockWithoutLatch(Transaction *txn, const RID &rid) {
  auto IsoLevel = txn->GetIsolationLevel();
  auto TxnState = txn->GetState();
  auto XLockSet = txn->GetExclusiveLockSet();
  auto SLockSet = txn->GetSharedLockSet();
  txn_id_t txnId = txn->GetTransactionId();
  if (TxnState == GROWING) {
    // for read committed,after 1st wunlock ,begin shrinking
    bool read_committed_shrinking = IsoLevel == READ_COMMITTED && XLockSet->count(rid) != 0;
    if (read_committed_shrinking || IsoLevel != READ_COMMITTED) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  if (lock_table_.count(rid) == 0 && XLockSet->count(rid) == 0 && SLockSet->count(rid) == 0) {
    return false;
    //    ThrowTransactionException(txn,UNLOCK_ON_SHRINKING);
  }
  SLockSet->erase(rid);
  XLockSet->erase(rid);
  assert(lock_table_.count(rid) != 0);
  EraseLockRequest(txnId, rid, false);
  if (lock_table_[rid].request_queue_.empty()) {
    lock_table_.erase(rid);
    return true;
  }
  lock_table_[rid].cv_.notify_all();
  return true;
}
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.count(t1) == 0) {
    waits_for_.try_emplace(t1);
  }
  waits_for_[t1].emplace_back(t2);
  if (reverse_wait_for_.count(t2) == 0) {
    reverse_wait_for_.try_emplace(t1);
  } else {
    reverse_wait_for_[t2].emplace_back(t1);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.count(t1) == 0) {
    return;
  }
  vector<txn_id_t> &to_ids = waits_for_[t1];
  auto begin = to_ids.begin();
  auto end = to_ids.end();
  begin = std::remove(begin, end, t2);
  assert(begin != end);
  to_ids.erase(begin, end);
  if (to_ids.empty()) {
    waits_for_.erase(t1);
  }
  if (reverse_wait_for_.count(t2) == 0) {
    return;
  }
  vector<txn_id_t> &reverse_to_ids = reverse_wait_for_[t2];
  auto rbegin = reverse_to_ids.begin();
  auto rend = reverse_to_ids.end();
  begin = std::remove(rbegin, rend, t1);
  assert(rbegin != rend);
  reverse_to_ids.erase(rbegin, rend);
  if (reverse_to_ids.empty()) {
    reverse_wait_for_.erase(t1);
  }
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  set<txn_id_t> visited;
  set<txn_id_t> no_loop;
  auto DFS = [&visited, &no_loop, this](auto &&self, txn_id_t from_id, txn_id_t to_id, txn_id_t *txnId) -> bool {
    if (no_loop.count(to_id) == 1) {
      return true;
    }
    if (visited.count(to_id) == 0) {
      visited.emplace(to_id);
      vector<txn_id_t> &to_ids = waits_for_[to_id];
      for (auto id : to_ids) {
        if (!self(self, to_id, id, txnId)) {
          return false;
        }
      }
      return true;
    }
    // not in no loop set and already visited once, cycle here.
    *txnId = from_id;
    return false;
  };
  vector<txn_id_t> txn_ids;
  for (auto &from : waits_for_) {
    txn_ids.emplace_back(from.first);
    // visited in a deterministic order
    sort(from.second.begin(), from.second.end());
  }
  // visited in a deterministic order
  std::sort(txn_ids.begin(), txn_ids.end());
  for (auto from_id : txn_ids) {
    if (no_loop.count(from_id) != 0) {
      continue;
    }
    if (!DFS(DFS, -1, from_id, txn_id)) {
      return true;
    }
    no_loop.merge(visited);
    visited.clear();
  }
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (auto &from : waits_for_) {
    for (auto to : from.second) {
      result.emplace_back(from.first, to);
    }
  }
  return result;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      waits_for_.clear();
      reverse_wait_for_.clear();
      // construct reverse_wait_for for the
      std::unordered_map<txn_id_t, std::set<txn_id_t>> reverse_wait_for;
      for (auto &lock_pair : lock_table_) {
        std::vector<txn_id_t> granted_set;
        for (auto &request : lock_pair.second.request_queue_) {
          if (request.granted_) {
            granted_set.emplace_back(request.txn_id_);
          } else {
            // granted in sequential
            for (auto &to_id : granted_set) {
              AddEdge(request.txn_id_, to_id);
            }
          }
        }
      }
      txn_id_t txnId;
      while (HasCycle(&txnId)) {
        auto txn = TransactionManager::GetTransaction(txnId);
        txn->SetState(TransactionState::ABORTED);
        std::unordered_set<RID> locked_rid_set;
        for (auto item : *txn->GetExclusiveLockSet()) {
          locked_rid_set.emplace(item);
        }
        for (auto item : *txn->GetSharedLockSet()) {
          locked_rid_set.emplace(item);
        }
        for (auto locked_rid : locked_rid_set) {
          UnlockWithoutLatch(txn, locked_rid);
        }
        for (auto &to_id : waits_for_[txnId]) {
          RemoveEdge(txnId, to_id);
        }
      }
      continue;
    }
  }
}
bool LockManager::shared_predicate(Transaction *txn, const RID &rid) {
  if (txn->GetState() == ABORTED) {
    return true;
  }
  std::list<LockRequest> &lrq = lock_table_[rid].request_queue_;
  const txn_id_t txnId = txn->GetTransactionId();
  for (auto &lr : lrq) {
    if (lr.lock_mode_ == EXCLUSIVE) {
      return false;
    }
    if (lr.txn_id_ == txnId) {
      lr.granted_ = true;
      break;
    }
  }
  return true;
}
bool LockManager::unique_predicate(Transaction *txn, const RID &rid) {
  if (txn->GetState() == ABORTED) {
    return true;
  }
  std::list<LockRequest> &lrq = lock_table_[rid].request_queue_;
  const txn_id_t txnId = txn->GetTransactionId();
  assert(!lrq.empty());
  LockRequest &lr = lrq.front();
  if (lr.txn_id_ == txnId) {
    assert(lr.lock_mode_ == EXCLUSIVE);
    lr.granted_ = true;
    return true;
  }
  return false;
}
bool LockManager::EraseLockRequest(txn_id_t txnId, const RID &rid, bool acquire_granted_) {
  std::list<LockRequest> &lrq = lock_table_[rid].request_queue_;
  auto lr = lrq.begin();
  auto end = lrq.end();
  while (lr != end && lr->txn_id_ != txnId) {
    lr++;
  }
  assert(lr != end);
  if (acquire_granted_) {
    assert(lr->granted_);
  }
  lrq.erase(lr);
  return true;
}
}  // namespace bustub
