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
    // delete from table
    if (table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      // delete from all index
      std::vector<IndexInfo *> index_set = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_set) {
        std::unique_ptr<Index> &index = index_info->index_;
        index->DeleteEntry(
            tmp_tuple.KeyFromTuple(*child_executor_->GetOutputSchema(), *index->GetKeySchema(), index->GetKeyAttrs()),
            *rid, exec_ctx_->GetTransaction());
      }
      return true;
    }
    // no more tuple after abort
    return false;
  }
  // no more tuple from child to delete
  return false;
}

}  // namespace bustub
