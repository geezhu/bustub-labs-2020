//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tmp_tuple;
  if (child_executor_->Next(&tmp_tuple, rid)) {
    auto txn = exec_ctx_->GetTransaction();
    auto catalog = exec_ctx_->GetCatalog();
    auto lock_manager = exec_ctx_->GetLockManager();
    auto IsoLevel = txn->GetIsolationLevel();
    auto table_oid = table_info_->oid_;
    if (txn->IsSharedLocked(*rid)) {
      lock_manager->LockUpgrade(txn, *rid);
    } else if (!txn->IsExclusiveLocked(*rid)) {
      lock_manager->LockExclusive(txn, *rid);
    }
    // delete from table
    if (table_info_->table_->MarkDelete(*rid, txn)) {
      // delete from all index
      auto &child_schema = *child_executor_->GetOutputSchema();
      std::vector<IndexInfo *> index_set = catalog->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_set) {
        std::unique_ptr<Index> &index = index_info->index_;
        index->DeleteEntry(tmp_tuple.KeyFromTuple(child_schema, *index->GetKeySchema(), index->GetKeyAttrs()), *rid,
                           txn);
        txn->GetIndexWriteSet()->emplace_back(*rid, table_oid, WType::DELETE, tmp_tuple, index_info->index_oid_,
                                              catalog);
      }
      if (IsoLevel == IsolationLevel::READ_UNCOMMITTED) {
        lock_manager->Unlock(txn, *rid);
      }
      return true;
    }
    if (IsoLevel == IsolationLevel::READ_UNCOMMITTED) {
      lock_manager->Unlock(txn, *rid);
    }
    // no more tuple after abort
    return false;
  }
  // no more tuple from child to delete
  return false;
}

}  // namespace bustub
