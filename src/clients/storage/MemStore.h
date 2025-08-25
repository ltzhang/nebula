/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_MEMSTORE_H_
#define CLIENTS_STORAGE_MEMSTORE_H_

#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "common/base/Base.h"
#include "common/base/StatusOr.h"

namespace nebula {
namespace storage {

class MemStoreCursor {
 public:
  MemStoreCursor() = default;
  explicit MemStoreCursor(const std::string& key) : currentKey_(key) {}

  const std::string& getCurrentKey() const { return currentKey_; }
  void setCurrentKey(const std::string& key) { currentKey_ = key; }
  bool isValid() const { return !currentKey_.empty(); }
  void invalidate() { currentKey_.clear(); }

 private:
  std::string currentKey_;
};

class MemStore {
 public:
  static MemStore* instance();

  // Basic KV operations
  StatusOr<std::string> get(const std::string& key);
  Status put(const std::string& key, const std::string& value);
  Status remove(const std::string& key);
  bool exists(const std::string& key);

  // Scan operations with cursor
  StatusOr<std::unique_ptr<MemStoreCursor>> createScanCursor(const std::string& prefix = "");
  StatusOr<std::pair<std::string, std::string>> scanNext(MemStoreCursor* cursor);
  bool hasNext(MemStoreCursor* cursor);

  // Batch operations
  Status batchPut(const std::vector<std::pair<std::string, std::string>>& kvs);
  Status batchRemove(const std::vector<std::string>& keys);

  // Utility operations
  size_t size();
  void clear();

  // Debug operations
  void dump();

  MemStore() = default;
  ~MemStore() = default;

 private:
  static std::unique_ptr<MemStore> instance_;
  static std::once_flag initFlag_;

  mutable std::mutex mutex_;
  std::map<std::string, std::string> data_;
};

}  // namespace storage
}  // namespace nebula

#endif  // CLIENTS_STORAGE_MEMSTORE_H_
