/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/MemStore.h"

#include <glog/logging.h>

#include <algorithm>
#include <iostream>

namespace nebula {
namespace storage {

std::unique_ptr<MemStore> MemStore::instance_ = nullptr;
std::once_flag MemStore::initFlag_;

MemStore* MemStore::instance() {
  std::call_once(initFlag_, []() { instance_ = std::unique_ptr<MemStore>(new MemStore()); });
  return instance_.get();
}

StatusOr<std::string> MemStore::get(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = data_.find(key);
  if (it == data_.end()) {
    return Status::Error("Key not found: %s", key.c_str());
  }
  return it->second;
}

Status MemStore::put(const std::string& key, const std::string& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  data_[key] = value;
  return Status::OK();
}

Status MemStore::remove(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = data_.find(key);
  if (it == data_.end()) {
    return Status::Error("Key not found: %s", key.c_str());
  }
  data_.erase(it);
  return Status::OK();
}

bool MemStore::exists(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  return data_.find(key) != data_.end();
}

StatusOr<std::unique_ptr<MemStoreCursor>> MemStore::createScanCursor(const std::string& prefix) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (data_.empty()) {
    return std::make_unique<MemStoreCursor>();
  }

  if (prefix.empty()) {
    // Start from the first key
    return std::make_unique<MemStoreCursor>(data_.begin()->first);
  } else {
    // Find the first key that starts with prefix
    auto it = data_.lower_bound(prefix);
    if (it != data_.end() && it->first.compare(0, prefix.length(), prefix) == 0) {
      return std::make_unique<MemStoreCursor>(it->first);
    }
    // No keys with this prefix
    return std::make_unique<MemStoreCursor>();
  }
}

StatusOr<std::pair<std::string, std::string>> MemStore::scanNext(MemStoreCursor* cursor) {
  if (cursor == nullptr) {
    return Status::Error("Cursor is null");
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!cursor->isValid()) {
    return Status::Error("Cursor is invalid");
  }

  auto it = data_.find(cursor->getCurrentKey());
  if (it == data_.end()) {
    cursor->invalidate();
    return Status::Error("Current cursor key not found");
  }

  std::pair<std::string, std::string> result = *it;
  
  // Move cursor to next key
  ++it;
  if (it != data_.end()) {
    cursor->setCurrentKey(it->first);
  } else {
    cursor->invalidate();
  }

  return result;
}

bool MemStore::hasNext(MemStoreCursor* cursor) {
  if (cursor == nullptr || !cursor->isValid()) {
    return false;
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  return data_.find(cursor->getCurrentKey()) != data_.end();
}

Status MemStore::batchPut(const std::vector<std::pair<std::string, std::string>>& kvs) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& kv : kvs) {
    data_[kv.first] = kv.second;
  }
  return Status::OK();
}

Status MemStore::batchRemove(const std::vector<std::string>& keys) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& key : keys) {
    data_.erase(key);
  }
  return Status::OK();
}

size_t MemStore::size() {
  std::lock_guard<std::mutex> lock(mutex_);
  return data_.size();
}

void MemStore::clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  data_.clear();
}

void MemStore::dump() {
  std::lock_guard<std::mutex> lock(mutex_);
  LOG(INFO) << "MemStore dump (" << data_.size() << " entries):";
  for (const auto& kv : data_) {
    LOG(INFO) << "  " << kv.first << " -> " << kv.second.substr(0, 100) 
              << (kv.second.length() > 100 ? "..." : "");
  }
}

}  // namespace storage
}  // namespace nebula