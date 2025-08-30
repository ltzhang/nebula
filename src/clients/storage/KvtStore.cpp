/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/KvtStore.h"

#include <glog/logging.h>

namespace nebula {
namespace storage {

std::unique_ptr<KvtStore> KvtStore::instance_ = nullptr;
std::once_flag KvtStore::initFlag_;

KvtStore* KvtStore::instance() {
  std::call_once(initFlag_, []() {
    instance_ = std::make_unique<KvtStore>();
  });
  return instance_.get();
}

KvtStore::KvtStore() : kvtInitialized_(false) {
  if (!kvt_initialize()) {
    LOG(FATAL) << "Failed to initialize KVT system in KvtStore";
  }
  kvtInitialized_ = true;
  LOG(INFO) << "KvtStore initialized successfully";
}

KvtStore::~KvtStore() {
  if (kvtInitialized_) {
    kvt_shutdown();
    LOG(INFO) << "KvtStore shutdown completed";
  }
}

bool KvtStore::ensureTable(const std::string& tableName) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (createdTables_.find(tableName) != createdTables_.end()) {
    return true;
  }
  
  std::string error;
  uint64_t tableId = kvt_create_table(tableName, "hash", error);
  if (tableId == 0) {
    if (error.find("already exists") != std::string::npos) {
      // Table already exists, which is fine
      createdTables_.insert(tableName);
      return true;
    }
    LOG(ERROR) << "Failed to create table " << tableName << ": " << error;
    return false;
  }
  
  createdTables_.insert(tableName);
  LOG(INFO) << "Created KVT table: " << tableName << " with ID: " << tableId;
  return true;
}

Status KvtStore::createTable(const std::string& tableName, const std::string& partitionMethod) {
  std::string error;
  uint64_t tableId = kvt_create_table(tableName, partitionMethod, error);
  if (tableId == 0) {
    if (error.find("already exists") != std::string::npos) {
      return Status::OK();
    }
    return Status::Error("Failed to create table %s: %s", tableName.c_str(), error.c_str());
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  createdTables_.insert(tableName);
  return Status::OK();
}

StatusOr<std::string> KvtStore::get(const std::string& tableName, const std::string& key) {
  if (!ensureTable(tableName)) {
    return Status::Error("Failed to ensure table: %s", tableName.c_str());
  }
  
  std::string value, error;
  if (kvt_get(0, tableName, key, value, error)) {  // Use auto-commit
    return value;
  }
  return Status::Error("Failed to get key %s from table %s: %s", 
                       key.c_str(), tableName.c_str(), error.c_str());
}

Status KvtStore::put(const std::string& tableName, const std::string& key, const std::string& value) {
  if (!ensureTable(tableName)) {
    return Status::Error("Failed to ensure table: %s", tableName.c_str());
  }
  
  std::string error;
  if (kvt_set(0, tableName, key, value, error)) {  // Use auto-commit
    return Status::OK();
  }
  return Status::Error("Failed to put key %s to table %s: %s", 
                       key.c_str(), tableName.c_str(), error.c_str());
}

Status KvtStore::remove(const std::string& tableName, const std::string& key) {
  if (!ensureTable(tableName)) {
    return Status::Error("Failed to ensure table: %s", tableName.c_str());
  }
  
  std::string error;
  if (kvt_del(0, tableName, key, error)) {  // Use auto-commit
    return Status::OK();
  }
  return Status::Error("Failed to remove key %s from table %s: %s", 
                       key.c_str(), tableName.c_str(), error.c_str());
}

bool KvtStore::exists(const std::string& tableName, const std::string& key) {
  std::string value, error;
  return kvt_get(0, tableName, key, value, error);
}

Status KvtStore::batchPut(const std::string& tableName, 
                          const std::vector<std::pair<std::string, std::string>>& kvs) {
  if (!ensureTable(tableName)) {
    return Status::Error("Failed to ensure table: %s", tableName.c_str());
  }
  
  std::string error;
  uint64_t txId = kvt_start_transaction(error);
  if (txId == 0) {
    return Status::Error("Failed to start transaction for batch put: %s", error.c_str());
  }
  
  for (const auto& [key, value] : kvs) {
    if (!kvt_set(txId, tableName, key, value, error)) {
      kvt_rollback_transaction(txId, error);
      return Status::Error("Failed to put key %s in batch: %s", key.c_str(), error.c_str());
    }
  }
  
  if (!kvt_commit_transaction(txId, error)) {
    return Status::Error("Failed to commit batch put transaction: %s", error.c_str());
  }
  
  return Status::OK();
}

Status KvtStore::batchRemove(const std::string& tableName, const std::vector<std::string>& keys) {
  if (!ensureTable(tableName)) {
    return Status::Error("Failed to ensure table: %s", tableName.c_str());
  }
  
  std::string error;
  uint64_t txId = kvt_start_transaction(error);
  if (txId == 0) {
    return Status::Error("Failed to start transaction for batch remove: %s", error.c_str());
  }
  
  for (const auto& key : keys) {
    if (!kvt_del(txId, tableName, key, error)) {
      kvt_rollback_transaction(txId, error);
      return Status::Error("Failed to remove key %s in batch: %s", key.c_str(), error.c_str());
    }
  }
  
  if (!kvt_commit_transaction(txId, error)) {
    return Status::Error("Failed to commit batch remove transaction: %s", error.c_str());
  }
  
  return Status::OK();
}

StatusOr<uint64_t> KvtStore::startTransaction() {
  std::string error;
  uint64_t txId = kvt_start_transaction(error);
  if (txId == 0) {
    return Status::Error("Failed to start transaction: %s", error.c_str());
  }
  return txId;
}

Status KvtStore::commitTransaction(uint64_t txId) {
  std::string error;
  if (kvt_commit_transaction(txId, error)) {
    return Status::OK();
  }
  return Status::Error("Failed to commit transaction %lu: %s", txId, error.c_str());
}

Status KvtStore::rollbackTransaction(uint64_t txId) {
  std::string error;
  if (kvt_rollback_transaction(txId, error)) {
    return Status::OK();
  }
  return Status::Error("Failed to rollback transaction %lu: %s", txId, error.c_str());
}

StatusOr<std::string> KvtStore::get(uint64_t txId, const std::string& tableName, const std::string& key) {
  if (!ensureTable(tableName)) {
    return Status::Error("Failed to ensure table: %s", tableName.c_str());
  }
  
  std::string value, error;
  if (kvt_get(txId, tableName, key, value, error)) {
    return value;
  }
  return Status::Error("Failed to get key %s from table %s in transaction %lu: %s", 
                       key.c_str(), tableName.c_str(), txId, error.c_str());
}

Status KvtStore::put(uint64_t txId, const std::string& tableName, 
                     const std::string& key, const std::string& value) {
  if (!ensureTable(tableName)) {
    return Status::Error("Failed to ensure table: %s", tableName.c_str());
  }
  
  std::string error;
  if (kvt_set(txId, tableName, key, value, error)) {
    return Status::OK();
  }
  return Status::Error("Failed to put key %s to table %s in transaction %lu: %s", 
                       key.c_str(), tableName.c_str(), txId, error.c_str());
}

Status KvtStore::remove(uint64_t txId, const std::string& tableName, const std::string& key) {
  if (!ensureTable(tableName)) {
    return Status::Error("Failed to ensure table: %s", tableName.c_str());
  }
  
  std::string error;
  if (kvt_del(txId, tableName, key, error)) {
    return Status::OK();
  }
  return Status::Error("Failed to remove key %s from table %s in transaction %lu: %s", 
                       key.c_str(), tableName.c_str(), txId, error.c_str());
}

Status KvtStore::scan(uint64_t txId, const std::string& tableName, 
                      const std::string& keyStart, const std::string& keyEnd,
                      size_t limit, std::vector<std::pair<std::string, std::string>>& results) {
  if (!ensureTable(tableName)) {
    return Status::Error("Failed to ensure table: %s", tableName.c_str());
  }
  
  std::string error;
  if (kvt_scan(txId, tableName, keyStart, keyEnd, limit, results, error)) {
    return Status::OK();
  }
  return Status::Error("Failed to scan table %s: %s", tableName.c_str(), error.c_str());
}

StatusOr<std::unique_ptr<KvtStoreCursor>> KvtStore::createScanCursor(
    const std::string& /* tableName */, const std::string& prefix) {
  // For simplicity, we'll implement cursor-based scanning using the scan function
  // This is a basic implementation - a more sophisticated version would maintain state
  return std::make_unique<KvtStoreCursor>(prefix);
}

StatusOr<std::pair<std::string, std::string>> KvtStore::scanNext(KvtStoreCursor* /* cursor */) {
  // This is a simplified implementation
  // A proper implementation would maintain cursor state and continue scanning
  return Status::Error("scanNext not fully implemented");
}

bool KvtStore::hasNext(KvtStoreCursor* /* cursor */) {
  // This is a simplified implementation
  return false;
}

void KvtStore::clear() {
  LOG(WARNING) << "KvtStore::clear() - This operation is not supported by KVT";
}

void KvtStore::dump() {
  LOG(INFO) << "KvtStore dump - Created tables: " << createdTables_.size();
  for (const auto& table : createdTables_) {
    LOG(INFO) << "  Table: " << table;
  }
}

}  // namespace storage
}  // namespace nebula