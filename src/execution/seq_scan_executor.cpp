//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      iterator_(table_info_->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() { iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto make_tuple = [](const Tuple *tuple, const Schema *out_schema, const Schema &tuple_schema) -> Tuple {
    std::vector<Value> values;
    for (auto &column : out_schema->GetColumns()) {
      values.emplace_back(column.GetExpr()->Evaluate(tuple, &tuple_schema));
    }
    return Tuple(values, out_schema);
  };
  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  auto IsoLevel = txn->GetIsolationLevel();
  while (iterator_ != table_info_->table_->End()) {
    if (plan_->GetPredicate() == nullptr ||
        plan_->GetPredicate()->Evaluate(&*iterator_, &table_info_->schema_).GetAs<bool>()) {
      *rid = iterator_->GetRid();
      *tuple = *iterator_;
      if (!txn->IsExclusiveLocked(*rid) && !txn->IsSharedLocked(*rid)) {
        if (IsoLevel != IsolationLevel::READ_UNCOMMITTED) {
          lock_manager->LockShared(txn, *rid);
          if (!table_info_->table_->GetTuple(*rid, tuple, txn)) {
            // tuple might be removed
            iterator_++;
            if (IsoLevel == IsolationLevel::READ_COMMITTED) {
              lock_manager->Unlock(txn, *rid);
            }
            // unable to unlock repeatable_read
            continue;
          }
        }
        if (IsoLevel == IsolationLevel::READ_COMMITTED) {
          lock_manager->Unlock(txn, *rid);
        }
      }
      iterator_++;
      *tuple = make_tuple(tuple, plan_->OutputSchema(), table_info_->schema_);
      return true;
    }
    iterator_++;
  }
  return false;
}

}  // namespace bustub
