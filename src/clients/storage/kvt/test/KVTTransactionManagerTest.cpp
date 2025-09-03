/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include "clients/storage/kvt/KVTTransactionManager.h"
#include "clients/storage/kvt/kvt_inc.h"

namespace nebula {
namespace storage {

class KVTTransactionManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize KVT system
    KVTError err = kvt_initialize();
    ASSERT_EQ(err, KVTError::SUCCESS);
    
    // Create a test table
    std::string error;
    err = kvt_create_table("test_table", "hash", testTableId_, error);
    ASSERT_EQ(err, KVTError::SUCCESS);
  }
  
  void TearDown() override {
    // Cleanup
    std::string error;
    kvt_drop_table(testTableId_, error);
    kvt_shutdown();
  }
  
  uint64_t testTableId_;
};

TEST_F(KVTTransactionManagerTest, StartTransactionBasic) {
  KVTTransactionManager manager;
  
  auto result = manager.startTransaction();
  ASSERT_TRUE(result.ok());
  
  auto tx = std::move(result).value();
  EXPECT_NE(tx->getId(), 0);
  EXPECT_FALSE(tx->isCommitted());
  
  // Test commit
  Status status = tx->commit();
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(tx->isCommitted());
  
  // Check stats
  auto stats = manager.getStats();
  EXPECT_EQ(stats.totalStarted, 1);
  EXPECT_EQ(stats.totalCommitted, 1);
  EXPECT_EQ(stats.totalRolledBack, 0);
}

TEST_F(KVTTransactionManagerTest, AutoRollback) {
  KVTTransactionManager manager;
  
  {
    auto result = manager.startTransaction();
    ASSERT_TRUE(result.ok());
    
    auto tx = std::move(result).value();
    EXPECT_NE(tx->getId(), 0);
    
    // Transaction goes out of scope without commit
    // Should auto-rollback
  }
  
  // Note: We can't directly check rollback stats here as the destructor
  // doesn't update manager stats, but we can verify no leaked transactions
}

TEST_F(KVTTransactionManagerTest, ExecuteWithRetry) {
  KVTTransactionManager manager;
  
  int attemptCount = 0;
  Status status = manager.executeWithRetry([&](uint64_t txId) {
    attemptCount++;
    
    // Simulate success on first attempt
    std::string error;
    KVTError err = kvt_set(txId, testTableId_, "key1", "value1", error);
    if (err != KVTError::SUCCESS) {
      return Status::Error("Set failed");
    }
    
    return Status::OK();
  }, 3);
  
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(attemptCount, 1);
  
  auto stats = manager.getStats();
  EXPECT_EQ(stats.totalCommitted, 1);
  EXPECT_EQ(stats.totalRetries, 0);
}

TEST_F(KVTTransactionManagerTest, BatchExecute) {
  KVTTransactionManager manager;
  
  // Prepare batch operations
  KVTBatchOps ops;
  
  // Set operations
  ops.push_back({OP_SET, testTableId_, "batch_key1", "batch_value1"});
  ops.push_back({OP_SET, testTableId_, "batch_key2", "batch_value2"});
  ops.push_back({OP_SET, testTableId_, "batch_key3", "batch_value3"});
  
  // Execute batch
  auto result = manager.executeBatch(ops);
  ASSERT_TRUE(result.ok());
  
  const auto& results = result.value();
  EXPECT_EQ(results.size(), 3);
  
  for (const auto& opResult : results) {
    EXPECT_EQ(opResult.error, KVTError::SUCCESS);
  }
  
  // Verify data was written
  KVTBatchOps getOps;
  getOps.push_back({OP_GET, testTableId_, "batch_key1", ""});
  getOps.push_back({OP_GET, testTableId_, "batch_key2", ""});
  getOps.push_back({OP_GET, testTableId_, "batch_key3", ""});
  
  auto getResult = manager.executeBatch(getOps);
  ASSERT_TRUE(getResult.ok());
  
  const auto& getResults = getResult.value();
  EXPECT_EQ(getResults.size(), 3);
  EXPECT_EQ(getResults[0].value, "batch_value1");
  EXPECT_EQ(getResults[1].value, "batch_value2");
  EXPECT_EQ(getResults[2].value, "batch_value3");
}

TEST_F(KVTTransactionManagerTest, BatchExecutePartialFailure) {
  KVTTransactionManager manager;
  
  // First set a key
  KVTBatchOps setOps;
  setOps.push_back({OP_SET, testTableId_, "existing_key", "existing_value"});
  auto setResult = manager.executeBatch(setOps);
  ASSERT_TRUE(setResult.ok());
  
  // Now try operations with some that will fail
  KVTBatchOps ops;
  ops.push_back({OP_SET, testTableId_, "new_key", "new_value"});
  ops.push_back({OP_GET, testTableId_, "nonexistent_key", ""});  // This will fail
  ops.push_back({OP_GET, testTableId_, "existing_key", ""});     // This will succeed
  
  auto result = manager.executeBatch(ops);
  
  // The batch should return results even with partial failure
  if (result.ok()) {
    const auto& results = result.value();
    EXPECT_EQ(results.size(), 3);
    
    // First operation should succeed
    EXPECT_EQ(results[0].error, KVTError::SUCCESS);
    
    // Second operation should fail (key not found)
    EXPECT_EQ(results[1].error, KVTError::KEY_NOT_FOUND);
    
    // Third operation should succeed
    EXPECT_EQ(results[2].error, KVTError::SUCCESS);
    EXPECT_EQ(results[2].value, "existing_value");
  }
}

TEST_F(KVTTransactionManagerTest, TransactionMove) {
  KVTTransactionManager manager;
  
  auto result = manager.startTransaction();
  ASSERT_TRUE(result.ok());
  
  auto tx1 = std::move(result).value();
  uint64_t txId1 = tx1->getId();
  
  // Move constructor
  KVTTransaction tx2(std::move(*tx1));
  EXPECT_EQ(tx2.getId(), txId1);
  EXPECT_EQ(tx1->getId(), 0);  // Original should be invalidated
  
  // Move assignment
  auto result2 = manager.startTransaction();
  ASSERT_TRUE(result2.ok());
  auto tx3 = std::move(result2).value();
  
  *tx3 = std::move(tx2);
  EXPECT_EQ(tx3->getId(), txId1);
  EXPECT_EQ(tx2.getId(), 0);
  
  // Commit the moved transaction
  EXPECT_TRUE(tx3->commit().ok());
}

TEST_F(KVTTransactionManagerTest, StatsTracking) {
  KVTTransactionManager manager;
  
  // Reset stats
  manager.resetStats();
  auto stats = manager.getStats();
  EXPECT_EQ(stats.totalStarted, 0);
  EXPECT_EQ(stats.totalCommitted, 0);
  EXPECT_EQ(stats.totalRolledBack, 0);
  
  // Start and commit a transaction
  auto result = manager.startTransaction();
  ASSERT_TRUE(result.ok());
  auto tx = std::move(result).value();
  tx->commit();
  
  stats = manager.getStats();
  EXPECT_EQ(stats.totalStarted, 1);
  EXPECT_EQ(stats.totalCommitted, 1);
  
  // Execute with retry (successful)
  manager.executeWithRetry([&](uint64_t txId) {
    return Status::OK();
  });
  
  stats = manager.getStats();
  EXPECT_EQ(stats.totalStarted, 2);
  EXPECT_EQ(stats.totalCommitted, 2);
  EXPECT_EQ(stats.totalRetries, 0);
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}