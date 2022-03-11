//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      predicate_(plan_->Predicate()) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
    throw std::logic_error("left executor is empty!");
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
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
  auto Next = [this](Tuple &inner_tuple, RID &inner_rid) -> bool {
    if (!right_executor_->Next(&inner_tuple, &inner_rid)) {
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }
      right_executor_->Init();
      if (!right_executor_->Next(&inner_tuple, &inner_rid)) {
        return false;
      }
    }
    return true;
  };
  auto outer_schema = plan_->GetLeftPlan()->OutputSchema();
  auto inner_schema = plan_->GetRightPlan()->OutputSchema();
  Tuple inner_tuple;
  RID inner_rid;
  if (!Next(inner_tuple, inner_rid)) {
    return false;
  }
  while (predicate_ != nullptr &&
         !predicate_->EvaluateJoin(&left_tuple_, outer_schema, &inner_tuple, inner_schema).GetAs<bool>()) {
    if (!Next(inner_tuple, inner_rid)) {
      // no more available tuple
      return false;
    }
  }
  *tuple = GenerateJoinTuple(left_tuple_, inner_tuple, outer_schema, inner_schema);
  return true;
}

}  // namespace bustub
