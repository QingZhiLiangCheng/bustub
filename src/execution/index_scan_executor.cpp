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

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void IndexScanExecutor::Init() {
  rids_.clear();
  has_scan_ = false;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_heap_ = table_info->table_.get();

  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  hash_table_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());

  auto table_schema = index_info->key_schema_;
  auto key = plan_->pred_key_;
  auto value = key->val_;
  std::vector<Value> values{value};
  Tuple index_key(values, &table_schema);
  hash_table_->ScanKey(index_key, &rids_, exec_ctx_->GetTransaction());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_scan_) {
    return false;
  }
  has_scan_ = true;
  if (rids_.empty()) {
    return false;
  }
  TupleMeta meta{};
  meta = table_heap_->GetTuple(*rids_.begin()).first;
  if (!meta.is_deleted_) {
    *tuple = table_heap_->GetTuple(*rids_.begin()).second;
    *rid = *rids_.begin();
  }
  return true;
}

}  // namespace bustub
