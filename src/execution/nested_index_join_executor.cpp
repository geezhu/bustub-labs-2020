//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <set>
using std::set;
using std::vector;
namespace bustub {
using BPlusIndex = BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>;
NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      right_table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())),
      predicate_(plan_->Predicate()) {}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), right_table_info_->name_);
  key_schema_ = &index_info_->key_schema_;
  if (!child_executor_->Next(&left_tuple_, &left_rid_)) {
    throw std::logic_error("left executor is empty!");
  }
  iterator_ = dynamic_cast<BPlusIndex *>(index_info_->index_.get())->GetBeginIterator();
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  auto left_schema = plan_->OuterTableSchema();
  auto right_schema = plan_->InnerTableSchema();
  auto GenerateJoinTuple = [this](const Tuple &left_tup, const Tuple &right_tup, const Schema *left_schema,
                                  const Schema *right_schema) -> Tuple {
    const Schema *join_schema = plan_->OutputSchema();
    uint32_t col_count = join_schema->GetColumnCount();
    std::vector<Value> values;
    values.reserve(col_count);
    for (auto &column : join_schema->GetColumns()) {
      values.emplace_back(column.GetExpr()->EvaluateJoin(&left_tup, left_schema, &right_tup, right_schema));
    }
    return Tuple(values, join_schema);
  };

  auto convert_key_to_tuple = [this](const GenericKey<8> &key) -> Tuple {
    std::set<uint32_t> columns_idx;
    for (auto &key_index : index_info_->index_->GetKeyAttrs()) {
      columns_idx.insert(key_index);
    }
    auto make_tuple = [&columns_idx, this](const std::vector<Value> &&key_values) -> Tuple {
      auto columns = right_table_info_->schema_.GetColumns();
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
      return Tuple(values, &right_table_info_->schema_);
    };
    auto extract_values = [](const GenericKey<8> &key, Schema &key_schema) -> std::vector<Value> {
      auto columns_count = key_schema.GetColumnCount();
      std::vector<Value> values;
      for (uint32_t i = 0; i < columns_count; ++i) {
        values.emplace_back(key.ToValue(&key_schema, i));
      }
      return values;
    };
    return make_tuple(extract_values(key, *key_schema_));
  };

  auto Next = [&convert_key_to_tuple, this](Tuple &right_tuple, RID &right_rid) -> bool {
    auto index = dynamic_cast<BPlusIndex *>(index_info_->index_.get());
    auto end = index->GetEndIterator();
    if (iterator_ == end) {
      if (!child_executor_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }
      iterator_ = index->GetBeginIterator();
      if (iterator_ == end) {
        return false;
      }
    }
    right_tuple = convert_key_to_tuple((*iterator_).first);
    right_rid = (*iterator_).second;
    ++iterator_;
    return true;
  };
  Tuple right_tuple;
  RID right_rid;
  if (!Next(right_tuple, right_rid)) {
    return false;
  }
  while (predicate_ != nullptr &&
         !predicate_->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
    if (!Next(right_tuple, right_rid)) {
      return false;
    }
  }
  // fetch right tuple by rid
  assert(right_table_info_->table_->GetTuple(right_rid, &right_tuple, exec_ctx_->GetTransaction()));
  *tuple = GenerateJoinTuple(left_tuple_, right_tuple, left_schema, right_schema);
  return true;
}

}  // namespace bustub
