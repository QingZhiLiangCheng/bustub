//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  has_deleted_ = false;
}

void DeleteExecutor::Init() {
  child_executor_->Init();
  has_deleted_ = false;
}
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_deleted_) {
    return false;
  }
  has_deleted_ = true;
  int count = 0;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  while (child_executor_->Next(tuple, rid)) {
    count++;
    table_info->table_->UpdateTupleMeta({0, true}, *rid);
    for (auto &index_info : index_infos) {
      auto index = index_info->index_.get();
      auto key_attrs = index->GetKeyAttrs();
      auto key = tuple->KeyFromTuple(table_info->schema_, *index->GetKeySchema(), key_attrs);
      index->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
  }
  std::vector<Value> result = {{TypeId::INTEGER, count}};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;
}

}  // namespace bustub
