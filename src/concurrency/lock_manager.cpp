//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 1.测试合理性
  if (txn->GetState() == TransactionState::SHRINKING) {
    // 在 REPEATABLE_READ 下，事务中止
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    // 在 READ_COMMITTED 下，若为X/IX/SIX锁则中止
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    // 在 READ_UNCOMMITTED 下，若为 IX/X 锁抛 LOCK_ON_SHRINKING 否则抛 LOCK_SHARED_ON_READ_UNCOMMITTED
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }
  // 2.尝试获得锁
  std::shared_ptr<LockRequestQueue> lock_queue;

  std::unique_lock<std::mutex> map_lock(table_lock_map_latch_);
  // 若queue不存在，则创建并授予锁，返回true
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    auto new_lock_request_queue = std::make_shared<LockRequestQueue>();
    new_lock_request_queue->request_queue_.push_back(
        std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
    new_lock_request_queue->request_queue_.front()->granted_ = true;
    table_lock_map_[oid] = new_lock_request_queue;
    TxnTableLockInsert(txn, lock_mode, oid);
    map_lock.unlock();
    return true;
  }
  lock_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> queue_lock(lock_queue->latch_);
  map_lock.unlock();

  // 3.判断是否需要升级
  bool can_upgrade = false;

  for (auto it = lock_queue->request_queue_.begin(); it != lock_queue->request_queue_.end(); it++) {
    auto lock_request = *it;
    // 如果此事务曾经已经申请该表的锁，则可能会锁升级
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      // 曾经的锁一定是授予的，不然事务会阻塞
      assert(lock_request->granted_ == true);
      if (lock_request->lock_mode_ == lock_mode) {
        return false;
      }
      if (lock_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!CanLockUpgrade(lock_request->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // 删除事务记录并删除queue中旧的锁请求
      TxnTableLockDelete(txn, lock_request->lock_mode_, oid);
      lock_queue->request_queue_.erase(it);
      can_upgrade = true;
      break;
    }
  }

  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  // 将一条新的记录插入在队列最前面一条未授予的记录之前
  if (can_upgrade) {
    if (lock_queue->request_queue_.empty()) {
      lock_queue->request_queue_.push_back(new_lock_request);
      lock_queue->upgrading_ = txn->GetTransactionId();
    } else {
      auto it = lock_queue->request_queue_.begin();
      for (; it != lock_queue->request_queue_.end(); ++it) {
        auto lock_request = *it;
        if (!lock_request->granted_) {
          lock_queue->request_queue_.insert(it, new_lock_request);
          lock_queue->upgrading_ = txn->GetTransactionId();
          break;
        }
      }
      if (it == lock_queue->request_queue_.end()) {
        lock_queue->request_queue_.insert(it, new_lock_request);
        lock_queue->upgrading_ = txn->GetTransactionId();
      }
    }
  } else {
    // 不用升级，平凡的锁请求
    lock_queue->request_queue_.push_back(new_lock_request);
  }

  while (!GrantLock(txn, lock_queue.get())) {
    lock_queue->cv_.wait(queue_lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      for (auto it = lock_queue->request_queue_.begin(); it != lock_queue->request_queue_.end(); ++it) {
        if ((*it)->txn_id_ == txn->GetTransactionId()) {
          lock_queue->request_queue_.erase(it);
          break;
        }
      }
      lock_queue->cv_.notify_all();
      return false;
    }
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  GrantNewLocksIfPossible(txn, lock_queue.get());
  TxnTableLockInsert(txn, lock_mode, oid);

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 检查是否持有锁
  if (!IsTableLockExist(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  // 检查是否table下的所有row的锁已经释放
  if (!CanTableUnlock(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  // 获取对应的 lock request queue
  std::shared_ptr<LockRequestQueue> lock_queue;
  std::unique_lock<std::mutex> map_lock(table_lock_map_latch_);
  assert(table_lock_map_.find(oid) != table_lock_map_.end());
  lock_queue = table_lock_map_[oid];

  std::unique_lock<std::mutex> queue_lock(lock_queue->latch_);
  map_lock.unlock();

  for (auto it = lock_queue->request_queue_.begin(); it != lock_queue->request_queue_.end(); ++it) {
    auto lock_request = *it;
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      ChangTxnState(txn, lock_request.get());
      TxnTableLockDelete(txn, lock_request->lock_mode_, oid);
      lock_queue->request_queue_.erase(it);
      break;
    }
  }
  lock_queue->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    // 在 REPEATABLE_READ 下，事务中止
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    // 在 READ_COMMITTED 下，若为X锁则中止
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    // 在 READ_UNCOMMITTED 下，若为 X 锁抛 LOCK_ON_SHRINKING 否则抛 LOCK_SHARED_ON_READ_UNCOMMITTED
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }
  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  std::shared_ptr<LockRequestQueue> lock_queue;

  std::unique_lock<std::mutex> map_lock(row_lock_map_latch_);
  // 若queue不存在，则创建并授予锁，返回true
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    auto new_lock_request_queue = std::make_shared<LockRequestQueue>();
    new_lock_request_queue->request_queue_.push_back(
        std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid));
    new_lock_request_queue->request_queue_.front()->granted_ = true;
    row_lock_map_[rid] = new_lock_request_queue;
    TxnRowLockInsert(txn, lock_mode, oid, rid);
    map_lock.unlock();
    return true;
  }
  lock_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> queue_lock(lock_queue->latch_);
  map_lock.unlock();

  bool can_upgrade = false;

  for (auto it = lock_queue->request_queue_.begin(); it != lock_queue->request_queue_.end(); it++) {
    auto lock_request = *it;
    // 如果此事务曾经已经申请该表的锁，则可能会锁升级
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      // 曾经的锁一定是授予的，不然事务会阻塞
      assert(lock_request->granted_ == true);
      if (lock_request->lock_mode_ == lock_mode) {
        return false;
      }
      if (lock_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!CanLockUpgrade(lock_request->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // 删除事务记录并删除queue中旧的锁请求
      TxnRowLockDelete(txn, lock_request->lock_mode_, oid, rid);
      lock_queue->request_queue_.erase(it);
      can_upgrade = true;
      break;
    }
  }

  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);

  if (can_upgrade) {
    if (lock_queue->request_queue_.empty()) {
      lock_queue->request_queue_.push_back(new_lock_request);
      lock_queue->upgrading_ = txn->GetTransactionId();
    } else {
      auto it = lock_queue->request_queue_.begin();
      for (; it != lock_queue->request_queue_.end(); ++it) {
        auto lock_request = *it;
        if (!lock_request->granted_) {
          lock_queue->request_queue_.insert(it, new_lock_request);
          lock_queue->upgrading_ = txn->GetTransactionId();
          break;
        }
      }
      if (it == lock_queue->request_queue_.end()) {
        lock_queue->request_queue_.insert(it, new_lock_request);
        lock_queue->upgrading_ = txn->GetTransactionId();
      }
    }
  } else {
    // 不用升级，平凡的锁请求
    lock_queue->request_queue_.push_back(new_lock_request);
  }

  while (!GrantLock(txn, lock_queue.get())) {
    lock_queue->cv_.wait(queue_lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      for (auto it = lock_queue->request_queue_.begin(); it != lock_queue->request_queue_.end(); ++it) {
        if ((*it)->txn_id_ == txn->GetTransactionId()) {
          lock_queue->request_queue_.erase(it);
          break;
        }
      }
      lock_queue->cv_.notify_all();
      return false;
    }
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  GrantNewLocksIfPossible(txn, lock_queue.get());
  TxnRowLockInsert(txn, lock_mode, oid, rid);

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  // 检查是否持有锁
  if (!IsRowLockExist(txn, oid, rid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  // 获取对应的 lock request queue
  std::shared_ptr<LockRequestQueue> lock_queue;
  std::unique_lock<std::mutex> map_lock(row_lock_map_latch_);
  assert(row_lock_map_.find(rid) != row_lock_map_.end());
  lock_queue = row_lock_map_[rid];

  std::unique_lock<std::mutex> queue_lock(lock_queue->latch_);
  map_lock.unlock();

  for (auto it = lock_queue->request_queue_.begin(); it != lock_queue->request_queue_.end(); ++it) {
    auto lock_request = *it;
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (!force) {
        ChangTxnState(txn, lock_request.get());
      }
      TxnRowLockDelete(txn, lock_request->lock_mode_, oid, rid);
      lock_queue->request_queue_.erase(it);
      break;
    }
  }
  lock_queue->cv_.notify_all();
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // t1 等 t2
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_.emplace(t1, std::vector<txn_id_t>{t2});
  } else {
    for (auto tmp : waits_for_[t1]) {
      if (tmp == t2) {
        return;
      }
    }
    waits_for_[t1].push_back(t2);
  }
  txn_set_.insert(t1);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) != waits_for_.end()) {
    for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); ++it) {
      if ((*it) == t2) {
        waits_for_[t1].erase(it);
        break;
      }
    }
    if (waits_for_[t1].empty()) {
      waits_for_.erase(t1);
      txn_set_.erase(t1);
    }
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  while (!txn_set_.empty()) {
    if (!HasCycleHelper(txn_id)) {
      txn_set_.erase(txn_set_.begin());
      continue;
    }
    txn_set_.erase(*txn_id);
    return true;
  }
  return false;
}

auto LockManager::HasCycleHelper(txn_id_t *txn_id) -> bool {
  std::set<txn_id_t, std::greater<>> on_path;
  auto lowest_txn = *(txn_set_.begin());
  *txn_id = lowest_txn;
  return FindCycle(lowest_txn, waits_for_[lowest_txn], on_path, txn_id);
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &map_pair : waits_for_) {
    auto t1 = map_pair.first;
    for (auto t2 : map_pair.second) {
      edges.emplace_back(std::make_pair(t1, t2));
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      waits_for_.clear();
      txn_set_.clear();
      {
        std::scoped_lock<std::mutex> table_lock(table_lock_map_latch_);
        for (const auto &table_map_pair : table_lock_map_) {
          auto lock_quests = table_map_pair.second->request_queue_;
          if (lock_quests.begin() == lock_quests.end()) {
            continue;
          }
          auto ungranted_it = lock_quests.begin();
          while (ungranted_it != lock_quests.end() && (*ungranted_it)->granted_) {
            ++ungranted_it;
          }
          auto granted_end = ungranted_it;
          for (; ungranted_it != lock_quests.end(); ++ungranted_it) {
            for (auto granted_it = lock_quests.begin(); granted_it != granted_end; ++granted_it) {
              AddEdge((*ungranted_it)->txn_id_, (*granted_it)->txn_id_);
            }
          }
        }
      }
      {
        std::scoped_lock<std::mutex> row_lock(row_lock_map_latch_);
        for (const auto &row_map_pair : row_lock_map_) {
          auto lock_quests = row_map_pair.second->request_queue_;
          if (lock_quests.begin() == lock_quests.end()) {
            continue;
          }
          auto ungranted_it = lock_quests.begin();
          while (ungranted_it != lock_quests.end() && (*ungranted_it)->granted_) {
            ++ungranted_it;
          }
          auto granted_end = ungranted_it;
          for (; ungranted_it != lock_quests.end(); ++ungranted_it) {
            for (auto granted_it = lock_quests.begin(); granted_it != granted_end; ++granted_it) {
              AddEdge((*ungranted_it)->txn_id_, (*granted_it)->txn_id_);
            }
          }
        }
      }
      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        auto txn = txn_manager_->GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        {
          // std::scoped_lock<std::mutex> lock(row_lock_map_latch_);
          for (const auto &row_map_pair : row_lock_map_) {
            auto lock_queue = row_map_pair.second;
            for (const auto &lock_request : lock_queue->request_queue_) {
              if (lock_request->txn_id_ == txn_id) {
                lock_queue->cv_.notify_all();
                break;
              }
            }
          }
        }
        {
          // std::scoped_lock<std::mutex> lock(table_lock_map_latch_);
          for (const auto &table_map_pair : table_lock_map_) {
            auto lock_queue = table_map_pair.second;
            for (const auto &lock_request : lock_queue->request_queue_) {
              if (lock_request->txn_id_ == txn_id) {
                lock_queue->cv_.notify_all();
                break;
              }
            }
          }
        }
        UpgradeGraph(txn_id);
      }
      waits_for_.clear();
    }
  }
}

}  // namespace bustub
