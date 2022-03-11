//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <set>
using std::set;
using std::vector;
namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), iterator_(rids_.end()) {}

void IndexScanExecutor::Init() {
  rids_.clear();
  IndexInfo *index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  // init table info
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);

  const AbstractExpression *predicate = plan_->GetPredicate();
  set<uint32_t> columns_idx;
  for (auto &key_index : index_info_->index_->GetKeyAttrs()) {
    columns_idx.insert(key_index);
  }
  auto &columns = table_info_->schema_.GetColumns();
  auto make_tuple = [&columns, &columns_idx, this](const std::vector<Value> &&key_values) -> Tuple {
    std::vector<Value> values;
    uint32_t i = 0;
    auto iter = key_values.begin();
    for (auto &column : columns) {
      if (columns_idx.count(i) == 1) {
        values.emplace_back(*(iter++));
      } else {
        values.emplace_back(Value(column.GetType()));
      }
      i++;
    }
    return Tuple(values, &table_info_->schema_);
  };
  auto columns_count = index_info_->key_schema_.GetColumnCount();
  auto extract_values = [&columns_count](const GenericKey<8> &key, Schema &key_schema) -> std::vector<Value> {
    std::vector<Value> values;
    for (uint32_t i = 0; i < columns_count; ++i) {
      values.emplace_back(key.ToValue(&key_schema, i));
    }
    return values;
  };
  auto index = dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get());
  auto end = index->GetEndIterator();
  auto iterator = index->GetBeginIterator();
  while (iterator != end) {
    auto tmp_tuple = make_tuple(extract_values((*iterator).first, index_info_->key_schema_));
    if (predicate == nullptr || predicate->Evaluate(&tmp_tuple, &table_info_->schema_).GetAs<bool>()) {
      rids_.emplace_back((*iterator).second);
    }
    ++iterator;
  }
  iterator_ = rids_.begin();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto make_out_tuple = [](const Tuple *tuple, const Schema *out_schema, const Schema &tuple_schema) -> Tuple {
    vector<Value> values;
    auto &columns = out_schema->GetColumns();
    values.reserve(out_schema->GetColumnCount());
    for (auto &column : columns) {
      values.emplace_back(column.GetExpr()->Evaluate(tuple, &tuple_schema));
    }
    return Tuple(values, out_schema);
  };
  auto fetch_tuple = [this](Tuple *tuple, const RID &rid) -> bool {
    return table_info_->table_->GetTuple(rid, tuple, exec_ctx_->GetTransaction());
  };
  if (iterator_ != rids_.end() && fetch_tuple(tuple, *iterator_)) {
    *rid = (*iterator_);
    // convert to out_schema
    *tuple = make_out_tuple(tuple, plan_->OutputSchema(), table_info_->schema_);
    iterator_++;
    return true;
  }
  return false;
}

}  // namespace bustub
