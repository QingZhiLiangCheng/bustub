//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  this->left_child_->Init();
  this->right_child_->Init();
  left_bool_ = left_child_->Next(&left_tuple_, &left_rid_);
  Tuple right_tuple{};
  RID right_rid{};
  // 构建哈希表
  jht_ = std::make_unique<SimpleHashJoinHashTable>();
  // 遍历子执行器，将右子执行器中的获取的数据插入到join哈希表中
  while (right_child_->Next(&right_tuple, &right_rid)) {
    jht_->InsertKey(GetRightJoinKey(&right_tuple), right_tuple);
  }
  // 获取左侧元组的hash key
  auto left_hash_key = GetLeftJoinKey(&left_tuple_);
  // 在哈希表中查找与左侧元组匹配的右侧元组
  right_tuple_ = jht_->GetValue(left_hash_key);
  if (right_tuple_ != nullptr) {
    jht_iterator_ = right_tuple_->begin();
    // 标记为true，防止next函数中重复输出
    has_done_ = true;
  } else {
    has_done_ = false;
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    // 如果right_tuple_不为空，且jht_iterator_未遍历完，则遍历输出
    // 一个左tuple可能匹配多个右tuple
    if (right_tuple_ != nullptr && jht_iterator_ != right_tuple_->end()) {
      std::vector<Value> values;
      auto right_tuple = *jht_iterator_;
      for (uint32_t i = 0; i < this->left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&this->left_child_->GetOutputSchema(), i));
      }
      // 连接操作右边元组的值均不为null
      for (uint32_t i = 0; i < this->right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(right_tuple.GetValue(&this->right_child_->GetOutputSchema(), i));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      ++jht_iterator_;
      return true;
    }
    // 如果right_tuple_为空，或者jht_iterator_遍历完，且为左连接
    // 如果has_done_为false，则说明左连接没有匹配的元组，需要输出右元组为null的情况
    if (plan_->GetJoinType() == JoinType::LEFT && !has_done_) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < this->left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&this->left_child_->GetOutputSchema(), i));
      }
      // 连接操作右边元组的值均不为null
      for (uint32_t i = 0; i < this->right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(
            ValueFactory::GetNullValueByType(this->right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      has_done_ = true;
      return true;
    }
    // 如果不是左连接，或者为左连接，但有有效输出，则继续遍历下一个左元组进行匹配
    // 如果left_bool_为false，左边找完了
    left_bool_ = left_child_->Next(&this->left_tuple_, &this->left_rid_);
    if (!left_bool_) {
      return false;
    }
    auto left_hash_key = GetLeftJoinKey(&left_tuple_);
    // 在哈希表中查找与左侧元组匹配的右侧元组
    right_tuple_ = jht_->GetValue(left_hash_key);
    if (right_tuple_ != nullptr) {
      jht_iterator_ = right_tuple_->begin();
      has_done_ = true;
    } else {
      has_done_ = false;
    }
  }
}
}  // namespace bustub