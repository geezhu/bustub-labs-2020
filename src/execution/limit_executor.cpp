//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), offset_{1} {}

void LimitExecutor::Init() {
  child_executor_->Init();
  offset_ = 1;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  auto make_tuple = [](const Tuple *tuple, const Schema *out_schema, const Schema &tuple_schema) -> Tuple {
    std::vector<Value> values;
    for (auto &column : out_schema->GetColumns()) {
      values.emplace_back(column.GetExpr()->Evaluate(tuple, &tuple_schema));
    }
    return Tuple(values, out_schema);
  };
  auto fetch_tuple = [tuple, rid, &make_tuple, this]() {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
    offset_++;
    *tuple = make_tuple(tuple, plan_->OutputSchema(), *child_executor_->GetOutputSchema());
    return true;
  };
  // remove the tuple before offset
  while (offset_ < plan_->GetOffset()) {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
    offset_++;
  }
  if (offset_ >= plan_->GetOffset() + plan_->GetLimit()) {
    return false;
  }
  return fetch_tuple();
}

}  // namespace bustub
