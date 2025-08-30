/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_KVTSTORE_H_
#define CLIENTS_STORAGE_KVTSTORE_H_

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_set>

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "kvt/kvt_inc.h"

namespace nebula {
namespace storage {

class KvtStoreCursor {
 public:
  KvtStoreCursor() = default;
  explicit KvtStoreCursor(const std::string& key) : currentKey_(key) {}

  const std::string& getCurrentKey() const { return currentKey_; }
  void setCurrentKey(const std::string& key) { currentKey_ = key; }
  bool isValid() const { return !currentKey_.empty(); }
  void invalidate() { currentKey_.clear(); }

 private:
  std::string currentKey_;
};

class KvtStore {
 public:
  static KvtStore* instance();

  // Basic KV operations
  StatusOr<std::string> get(const std::string& tableName, const std::string& key);
  Status put(const std::string& tableName, const std::string& key, const std::string& value);
  Status remove(const std::string& tableName, const std::string& key);
  bool exists(const std::string& tableName, const std::string& key);

  // Batch operations (emulated with individual operations)
  Status batchPut(const std::string& tableName, 
                  const std::vector<std::pair<std::string, std::string>>& kvs);
  Status batchRemove(const std::string& tableName, const std::vector<std::string>& keys);

  // Table management
  Status createTable(const std::string& tableName, const std::string& partitionMethod = "hash");

  // Transaction operations
  StatusOr<uint64_t> startTransaction();
  Status commitTransaction(uint64_t txId);
  Status rollbackTransaction(uint64_t txId);

  // Transactional operations
  StatusOr<std::string> get(uint64_t txId, const std::string& tableName, const std::string& key);
  Status put(uint64_t txId, const std::string& tableName, 
             const std::string& key, const std::string& value);
  Status remove(uint64_t txId, const std::string& tableName, const std::string& key);

  // Scan operations with cursor
  StatusOr<std::unique_ptr<KvtStoreCursor>> createScanCursor(
      const std::string& tableName, const std::string& prefix = "");
  StatusOr<std::pair<std::string, std::string>> scanNext(KvtStoreCursor* cursor);
  bool hasNext(KvtStoreCursor* cursor);

  // Scan with transaction
  Status scan(uint64_t txId, const std::string& tableName, 
              const std::string& keyStart, const std::string& keyEnd,
              size_t limit, std::vector<std::pair<std::string, std::string>>& results);

  // Utility operations
  void clear();

  // Debug operations
  void dump();

  KvtStore();
  ~KvtStore();

 private:
  static std::unique_ptr<KvtStore> instance_;
  static std::once_flag initFlag_;

  mutable std::mutex mutex_;
  std::unordered_set<std::string> createdTables_;
  bool kvtInitialized_;

  // Helper method to ensure table exists
  bool ensureTable(const std::string& tableName);
};

}  // namespace storage
}  // namespace nebula

#endif  // CLIENTS_STORAGE_KVTSTORE_H_