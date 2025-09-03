/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/kvt/KVTTransactionManager.h"
#include "common/base/Logging.h"

namespace nebula {
namespace storage {

StatusOr<std::unique_ptr<KVTTransaction>> KVTTransactionManager::startTransaction() {
  uint64_t txId;
  std::string error;
  
  KVTError err = kvt_start_transaction(txId, error);
  if (err != KVTError::SUCCESS) {
    LOG(ERROR) << "Failed to start transaction: " << error;
    return Status::Error("Failed to start transaction: " + error);
  }
  
  incrementStarted();
  LOG(INFO) << "Started transaction " << txId;
  
  return std::make_unique<KVTTransaction>(txId);
}

StatusOr<KVTBatchResults> KVTTransactionManager::executeBatch(
    const KVTBatchOps& ops, uint64_t txId) {
  
  bool needCommit = false;
  uint64_t actualTxId = txId;
  
  // Start a new transaction if txId is 0
  if (txId == 0) {
    std::string error;
    KVTError err = kvt_start_transaction(actualTxId, error);
    if (err != KVTError::SUCCESS) {
      LOG(ERROR) << "Failed to start transaction for batch: " << error;
      return Status::Error("Failed to start transaction: " + error);
    }
    incrementStarted();
    needCommit = true;
  }
  
  KVTBatchResults results;
  std::string error;
  
  KVTError err = kvt_batch_execute(actualTxId, ops, results, error);
  
  if (needCommit) {
    if (err == KVTError::SUCCESS) {
      // All operations succeeded, commit the transaction
      std::string commitError;
      KVTError commitErr = kvt_commit_transaction(actualTxId, commitError);
      if (commitErr != KVTError::SUCCESS) {
        LOG(ERROR) << "Failed to commit batch transaction: " << commitError;
        kvt_rollback_transaction(actualTxId, commitError);
        incrementRolledBack();
        return Status::Error("Failed to commit batch: " + commitError);
      }
      incrementCommitted();
    } else {
      // Some operations failed, rollback
      std::string rollbackError;
      kvt_rollback_transaction(actualTxId, rollbackError);
      incrementRolledBack();
      
      if (err == KVTError::BATCH_NOT_FULLY_SUCCESS) {
        // Partial success - return results so caller can see what succeeded/failed
        LOG(WARNING) << "Batch execution partially failed: " << error;
        return results;
      } else {
        // Complete failure
        LOG(ERROR) << "Batch execution failed: " << error;
        return Status::Error("Batch execution failed: " + error);
      }
    }
  }
  
  if (err != KVTError::SUCCESS && err != KVTError::BATCH_NOT_FULLY_SUCCESS) {
    LOG(ERROR) << "Batch execution error: " << error;
    return Status::Error("Batch execution error: " + error);
  }
  
  return results;
}

}  // namespace storage
}  // namespace nebula