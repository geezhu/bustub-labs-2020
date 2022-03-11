//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())) {
  if (plan_->IsRawInsert()) {
    iterator_ = plan_->RawValues().cbegin();
  }
}

void InsertExecutor::Init() {
  if (plan_->IsRawInsert()) {
    iterator_ = plan_->RawValues().cbegin();
  } else {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto make_key_tuple = [this](Tuple &tuple, std::unique_ptr<Index> &index) -> Tuple {
    return tuple.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
  };
  auto insert_into_table_and_index = [this, &make_key_tuple](Tuple &tuple, RID *rid) -> bool {
    auto txn = exec_ctx_->GetTransaction();
    if (table_info_->table_->InsertTuple(tuple, rid, exec_ctx_->GetTransaction())) {
      auto index_set = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_set) {
        std::unique_ptr<Index> &index = index_info->index_;
        index->InsertEntry(make_key_tuple(tuple, index), *rid, txn);
      }
      return true;
    }
    return false;
  };
  Tuple tmp_tuple;
  RID tmp_rid;
  if (plan_->IsRawInsert()) {
    if (iterator_ == plan_->RawValues().end()) {
      return false;
    }
    tmp_tuple = Tuple(*(iterator_++), &table_info_->schema_);
  } else {
    if (!child_executor_->Next(&tmp_tuple, &tmp_rid)) {
      return false;
    }
  }
  return insert_into_table_and_index(tmp_tuple, rid);
}

}  // namespace bustub
