/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include "clients/storage/MemStore.h"
#include "clients/storage/MemStorageClient.h"

namespace nebula {
namespace storage {

class MemStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    MemStore::instance()->clear();
  }
  
  void TearDown() override {
    MemStore::instance()->clear();
  }
};

TEST_F(MemStoreTest, BasicOperations) {
  auto* store = MemStore::instance();
  
  // Test put and get
  ASSERT_TRUE(store->put("key1", "value1").ok());
  auto result = store->get("key1");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ("value1", result.value());
  
  // Test exists
  ASSERT_TRUE(store->exists("key1"));
  ASSERT_FALSE(store->exists("nonexistent"));
  
  // Test remove
  ASSERT_TRUE(store->remove("key1").ok());
  ASSERT_FALSE(store->exists("key1"));
  
  // Test batch operations
  std::vector<std::pair<std::string, std::string>> kvs = {
    {"batch1", "value1"},
    {"batch2", "value2"},
    {"batch3", "value3"}
  };
  ASSERT_TRUE(store->batchPut(kvs).ok());
  ASSERT_EQ(3, store->size());
  
  // Test batch remove
  std::vector<std::string> keys = {"batch1", "batch3"};
  ASSERT_TRUE(store->batchRemove(keys).ok());
  ASSERT_EQ(1, store->size());
  ASSERT_TRUE(store->exists("batch2"));
}

TEST_F(MemStoreTest, CursorScan) {
  auto* store = MemStore::instance();
  
  // Add some test data
  store->put("prefix:key1", "value1");
  store->put("prefix:key2", "value2");
  store->put("prefix:key3", "value3");
  store->put("other:key4", "value4");
  
  // Test scan with prefix
  auto cursorResult = store->createScanCursor("prefix:");
  ASSERT_TRUE(cursorResult.ok());
  auto cursor = std::move(cursorResult.value());
  
  int count = 0;
  while (store->hasNext(cursor.get())) {
    auto kvResult = store->scanNext(cursor.get());
    ASSERT_TRUE(kvResult.ok());
    auto kv = kvResult.value();
    ASSERT_TRUE(kv.first.find("prefix:") == 0);
    count++;
  }
  ASSERT_EQ(3, count);
  
  // Test scan all
  auto allCursor = store->createScanCursor();
  ASSERT_TRUE(allCursor.ok());
  
  count = 0;
  while (store->hasNext(allCursor.value().get())) {
    auto kvResult = store->scanNext(allCursor.value().get());
    ASSERT_TRUE(kvResult.ok());
    count++;
  }
  ASSERT_EQ(4, count);
}

TEST_F(MemStoreTest, MemStorageClientBasic) {
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
  MemStorageClient client(ioThreadPool, nullptr);
  
  // Test basic KV operations
  std::vector<KeyValue> kvs = {{"test_key", "test_value"}};
  auto putResult = client.put(1, std::move(kvs)).get();
  ASSERT_TRUE(putResult.succeeded());
  
  std::vector<std::string> keys = {"test_key"};
  auto getResult = client.get(1, std::move(keys)).get();
  ASSERT_TRUE(getResult.succeeded());
}

}  // namespace storage
}  // namespace nebula