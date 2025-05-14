//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_heap_(nullptr),
      iterator_(nullptr)
{}

void SeqScanExecutor::Init() {
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  iterator_ = std::make_unique<TableIterator>(table_heap_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iterator_->IsEnd()) {
    auto current_tuple_pair = iterator_->GetTuple();
    auto current_tuple = current_tuple_pair.second;
    auto tuple_meta = current_tuple_pair.first;

    if (tuple_meta.is_deleted_) {
      ++(*iterator_);
      continue;
    }

    // 谓词下移 filter提前
    if (plan_->filter_predicate_ != nullptr) {
      Value expr_result = plan_->filter_predicate_->Evaluate(
          &current_tuple, GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->schema_);
      if (expr_result.IsNull() || !expr_result.GetAs<bool>()) {
        ++(*iterator_);
        continue;
      }
    }

    *tuple = current_tuple;
    *rid = current_tuple.GetRid();

    ++(*iterator_);
    return true;
  }
  return false;
}

}  // namespace bustub
