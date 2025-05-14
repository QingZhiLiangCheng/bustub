//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  has_insert_ = false;
}

void InsertExecutor::Init() {
  child_executor_->Init();
  has_insert_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_insert_) {
    return false;
  }
  has_insert_ = true;
  int count = 0;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto table_schema = table_info->schema_;
  auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  while (child_executor_->Next(tuple, rid)) {
    count++;
    std::optional<RID> new_rid_optional = table_info->table_->InsertTuple(TupleMeta{0, false}, *tuple);
    RID new_rid = new_rid_optional.value();
    for (auto &index_info : index_infos) {
      auto key = tuple->KeyFromTuple(table_schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, new_rid, exec_ctx_->GetTransaction());
    }
  }
  std::vector<Value> result = {{TypeId::INTEGER, count}};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;
}

}  // namespace bustub
