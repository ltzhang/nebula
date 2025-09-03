/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_KVT_KVTTRANSACTIONMANAGER_H
#define CLIENTS_STORAGE_KVT_KVTTRANSACTIONMANAGER_H

#include <memory>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include "clients/storage/kvt/kvt_inc.h"
#include "common/base/StatusOr.h"

namespace nebula {
namespace storage {

/**
 * Transaction wrapper with RAII semantics
 */
class KVTTransaction {
 public:
  explicit KVTTransaction(uint64_t txId) : txId_(txId), committed_(false) {}
  
  ~KVTTransaction() {
    if (!committed_ && txId_ != 0) {
      // Auto-rollback if not committed
      std::string error;
      kvt_rollback_transaction(txId_, error);
    }
  }
  
  // Disable copy
  KVTTransaction(const KVTTransaction&) = delete;
  KVTTransaction& operator=(const KVTTransaction&) = delete;
  
  // Enable move
  KVTTransaction(KVTTransaction&& other) noexcept
      : txId_(other.txId_), committed_(other.committed_.load()) {
    other.txId_ = 0;
    other.committed_ = true;
  }
  
  KVTTransaction& operator=(KVTTransaction&& other) noexcept {
    if (this != &other) {
      if (!committed_ && txId_ != 0) {
        std::string error;
        kvt_rollback_transaction(txId_, error);
      }
      txId_ = other.txId_;
      committed_ = other.committed_.load();
      other.txId_ = 0;
      other.committed_ = true;
    }
    return *this;
  }
  
  uint64_t getId() const { return txId_; }
  
  Status commit() {
    if (committed_) {
      return Status::Error("Transaction already committed");
    }
    if (txId_ == 0) {
      return Status::Error("Invalid transaction ID");
    }
    
    std::string error;
    KVTError err = kvt_commit_transaction(txId_, error);
    if (err != KVTError::SUCCESS) {
      return Status::Error("Failed to commit transaction: " + error);
    }
    
    committed_ = true;
    return Status::OK();
  }
  
  Status rollback() {
    if (committed_) {
      return Status::Error("Transaction already committed");
    }
    if (txId_ == 0) {
      return Status::Error("Invalid transaction ID");
    }
    
    std::string error;
    KVTError err = kvt_rollback_transaction(txId_, error);
    if (err != KVTError::SUCCESS) {
      return Status::Error("Failed to rollback transaction: " + error);
    }
    
    committed_ = true;  // Mark as handled
    return Status::OK();
  }
  
  bool isCommitted() const { return committed_; }
  
 private:
  uint64_t txId_;
  std::atomic<bool> committed_;
};

/**
 * KVTTransactionManager manages KVT transaction lifecycle
 * 
 * Features:
 * - Transaction pool management
 * - Automatic commit/rollback on scope exit
 * - Retry logic for transaction conflicts
 * - Batch operation optimization
 */
class KVTTransactionManager {
 public:
  KVTTransactionManager() = default;
  ~KVTTransactionManager() = default;
  
  /**
   * Start a new transaction
   * @return Transaction object with RAII semantics
   */
  StatusOr<std::unique_ptr<KVTTransaction>> startTransaction();
  
  /**
   * Execute operations with automatic retry on conflict
   * @param func Function to execute within transaction
   * @param maxRetries Maximum number of retries on conflict
   * @return Status of the operation
   */
  template <typename Func>
  Status executeWithRetry(Func func, int maxRetries = 3);
  
  /**
   * Execute a batch of operations in a single transaction
   * @param ops Batch operations to execute
   * @param txId Optional existing transaction ID (0 for new transaction)
   * @return Results of batch execution
   */
  StatusOr<KVTBatchResults> executeBatch(const KVTBatchOps& ops, 
                                         uint64_t txId = 0);
  
  /**
   * Get statistics about transactions
   */
  struct Stats {
    uint64_t totalStarted{0};
    uint64_t totalCommitted{0};
    uint64_t totalRolledBack{0};
    uint64_t totalRetries{0};
    uint64_t totalConflicts{0};
  };
  
  Stats getStats() const {
    std::lock_guard<std::mutex> lock(statsMutex_);
    return stats_;
  }
  
  /**
   * Reset statistics
   */
  void resetStats() {
    std::lock_guard<std::mutex> lock(statsMutex_);
    stats_ = Stats();
  }
  
 private:
  mutable std::mutex statsMutex_;
  Stats stats_;
  
  void incrementStarted() {
    std::lock_guard<std::mutex> lock(statsMutex_);
    stats_.totalStarted++;
  }
  
  void incrementCommitted() {
    std::lock_guard<std::mutex> lock(statsMutex_);
    stats_.totalCommitted++;
  }
  
  void incrementRolledBack() {
    std::lock_guard<std::mutex> lock(statsMutex_);
    stats_.totalRolledBack++;
  }
  
  void incrementRetries() {
    std::lock_guard<std::mutex> lock(statsMutex_);
    stats_.totalRetries++;
  }
  
  void incrementConflicts() {
    std::lock_guard<std::mutex> lock(statsMutex_);
    stats_.totalConflicts++;
  }
};

// Template implementation
template <typename Func>
Status KVTTransactionManager::executeWithRetry(Func func, int maxRetries) {
  for (int attempt = 0; attempt <= maxRetries; ++attempt) {
    auto txResult = startTransaction();
    if (!txResult.ok()) {
      return txResult.status();
    }
    
    auto tx = std::move(txResult).value();
    
    // Execute the function
    Status execStatus = func(tx->getId());
    
    if (!execStatus.ok()) {
      // If execution failed, rollback and return error
      tx->rollback();
      incrementRolledBack();
      return execStatus;
    }
    
    // Try to commit
    Status commitStatus = tx->commit();
    if (commitStatus.ok()) {
      incrementCommitted();
      return Status::OK();
    }
    
    // Check if it's a conflict that we can retry
    if (commitStatus.message().find("STALE_DATA") != std::string::npos ||
        commitStatus.message().find("LOCKED") != std::string::npos) {
      incrementConflicts();
      if (attempt < maxRetries) {
        incrementRetries();
        // Continue to retry
        continue;
      }
    }
    
    // Non-retryable error or max retries reached
    incrementRolledBack();
    return commitStatus;
  }
  
  return Status::Error("Max retries exceeded");
}

}  // namespace storage
}  // namespace nebula

#endif  // CLIENTS_STORAGE_KVT_KVTTRANSACTIONMANAGER_H