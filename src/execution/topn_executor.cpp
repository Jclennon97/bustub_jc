#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  std::priority_queue<Tuple, std::vector<Tuple>, CompareRule> topn_res(
      CompareRule(plan_, child_executor_->GetOutputSchema()));
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    topn_res.emplace(tuple);
  }
  int num = plan_->GetN();
  while (num > 0 && !topn_res.empty()) {
    top_entries_.push_back(topn_res.top());
    topn_res.pop();
    num--;
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!top_entries_.empty()) {
    *tuple = top_entries_.front();
    top_entries_.pop_front();
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); };

}  // namespace bustub
