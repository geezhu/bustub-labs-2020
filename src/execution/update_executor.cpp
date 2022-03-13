//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto make_key_tuple = [this](Tuple &tuple, std::unique_ptr<Index> &index) -> Tuple {
    return tuple.KeyFromTuple(*child_executor_->GetOutputSchema(), *index->GetKeySchema(), index->GetKeyAttrs());
  };
  auto update_table_and_index = [this, &make_key_tuple](Tuple &old_tuple, RID *rid) -> bool {
    auto txn = exec_ctx_->GetTransaction();
    auto catalog = exec_ctx_->GetCatalog();
    auto lock_manager = exec_ctx_->GetLockManager();
    auto IsoLevel = txn->GetIsolationLevel();
    Tuple new_tuple = GenerateUpdatedTuple(old_tuple);
    if (txn->IsSharedLocked(*rid)) {
      lock_manager->LockUpgrade(txn, *rid);
    } else if (!txn->IsExclusiveLocked(*rid)) {
      lock_manager->LockExclusive(txn, *rid);
    }
    if (table_info_->table_->UpdateTuple(new_tuple, *rid, txn)) {
      auto index_set = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_set) {
        std::unique_ptr<Index> &index = index_info->index_;
        index->DeleteEntry(make_key_tuple(old_tuple, index), RID(), txn);
        index->InsertEntry(make_key_tuple(new_tuple, index), *rid, txn);
        IndexWriteRecord indexWriteRecord(*rid, table_info_->oid_, WType::UPDATE, new_tuple, index_info->index_oid_,
                                          catalog);
        indexWriteRecord.old_tuple_ = old_tuple;
        txn->GetIndexWriteSet()->emplace_back(indexWriteRecord);
      }
      if (IsoLevel == IsolationLevel::READ_UNCOMMITTED) {
        lock_manager->Unlock(txn, *rid);
      }
      return true;
    }
    if (IsoLevel == IsolationLevel::READ_UNCOMMITTED) {
      lock_manager->Unlock(txn, *rid);
    }
    return false;
  };
  Tuple tmp_tuple;
  if (child_executor_->Next(&tmp_tuple, rid)) {
    return update_table_and_index(tmp_tuple, rid);
  }
  return false;
}
}  // namespace bustub
