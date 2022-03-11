//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()),
      having_(plan->GetHaving()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  Tuple aggregation_tuple;
  RID aggregation_rid;
  child_->Init();
  if (aht_.Begin() == aht_.End()) {
    while (child_->Next(&aggregation_tuple, &aggregation_rid)) {
      aht_.InsertCombine(MakeKey(&aggregation_tuple), MakeVal(&aggregation_tuple));
    }
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  auto make_out_tuple = [](const AggregateKey &key, const AggregateValue &value, const Schema *out_schema) -> Tuple {
    Value column_value;
    std::vector<Value> values;
    auto columns = out_schema->GetColumns();
    for (auto &column : columns) {
      column_value = column.GetExpr()->EvaluateAggregate(key.group_bys_, value.aggregates_);
      values.emplace_back(column_value);
    }
    return Tuple(values, out_schema);
  };
  while (aht_iterator_ != aht_.End()) {
    *tuple = make_out_tuple(aht_iterator_.Key(), aht_iterator_.Val(), plan_->OutputSchema());
    if (having_ == nullptr ||
        having_->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_).GetAs<bool>()) {
      ++aht_iterator_;
      return true;
    }
    ++aht_iterator_;
  }
  return false;
}

}  // namespace bustub
