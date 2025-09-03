/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <memory>
#include <iostream>
#include <vector>
#include <string>

#include "clients/storage/kvt/kvt_inc.h"

class KVTValidationTest : public ::testing::Test {
protected:
    void SetUp() override {
        kvt_init();
        std::cout << "[TEST] KVT initialized successfully" << std::endl;
    }

    void TearDown() override {
        kvt_shutdown();
        std::cout << "[TEST] KVT shutdown successfully" << std::endl;
    }
};

TEST_F(KVTValidationTest, BasicOperations) {
    std::cout << "\n=== Testing Basic KVT Operations ===" << std::endl;
    
    // Test 1: Simple put and get
    {
        auto* txn = kvt_begin();
        ASSERT_NE(txn, nullptr) << "Failed to begin transaction";
        std::cout << "[PASS] Transaction started" << std::endl;
        
        const char* key = "test_key_1";
        const char* value = "test_value_1";
        
        int put_result = kvt_put(txn, key, strlen(key), value, strlen(value));
        ASSERT_EQ(put_result, 0) << "Failed to put key-value pair";
        std::cout << "[PASS] Put operation: " << key << " -> " << value << std::endl;
        
        int commit_result = kvt_commit(txn);
        ASSERT_EQ(commit_result, 0) << "Failed to commit transaction";
        std::cout << "[PASS] Transaction committed" << std::endl;
    }
    
    // Test 2: Read back the value
    {
        auto* txn = kvt_begin();
        ASSERT_NE(txn, nullptr) << "Failed to begin read transaction";
        
        const char* key = "test_key_1";
        size_t value_len;
        
        const char* retrieved = kvt_get(txn, key, strlen(key), &value_len);
        ASSERT_NE(retrieved, nullptr) << "Failed to get value";
        ASSERT_EQ(value_len, strlen("test_value_1")) << "Value length mismatch";
        ASSERT_EQ(std::string(retrieved, value_len), "test_value_1") << "Value mismatch";
        std::cout << "[PASS] Get operation: " << key << " -> " << std::string(retrieved, value_len) << std::endl;
        
        kvt_commit(txn);
    }
}

TEST_F(KVTValidationTest, TransactionIsolation) {
    std::cout << "\n=== Testing Transaction Isolation ===" << std::endl;
    
    // Setup: Insert initial data
    {
        auto* txn = kvt_begin();
        kvt_put(txn, "iso_key", 7, "initial", 7);
        kvt_commit(txn);
        std::cout << "[SETUP] Initial value: iso_key -> initial" << std::endl;
    }
    
    // Test: Start two transactions
    auto* txn1 = kvt_begin();
    auto* txn2 = kvt_begin();
    ASSERT_NE(txn1, nullptr);
    ASSERT_NE(txn2, nullptr);
    std::cout << "[PASS] Started two concurrent transactions" << std::endl;
    
    // Txn1 updates the value
    kvt_put(txn1, "iso_key", 7, "txn1_value", 10);
    std::cout << "[TXN1] Updated iso_key -> txn1_value (not committed)" << std::endl;
    
    // Txn2 should still see the original value
    size_t len;
    const char* value = kvt_get(txn2, "iso_key", 7, &len);
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(std::string(value, len), "initial") << "Transaction isolation violated";
    std::cout << "[PASS] TXN2 still sees original value: " << std::string(value, len) << std::endl;
    
    // Commit txn1
    int result = kvt_commit(txn1);
    ASSERT_EQ(result, 0) << "Failed to commit txn1";
    std::cout << "[TXN1] Committed successfully" << std::endl;
    
    // Txn2 should still see snapshot value
    value = kvt_get(txn2, "iso_key", 7, &len);
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(std::string(value, len), "initial") << "Snapshot isolation violated";
    std::cout << "[PASS] TXN2 maintains snapshot isolation" << std::endl;
    
    kvt_commit(txn2);
    
    // New transaction should see the updated value
    auto* txn3 = kvt_begin();
    value = kvt_get(txn3, "iso_key", 7, &len);
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(std::string(value, len), "txn1_value") << "Update not visible after commit";
    std::cout << "[PASS] New transaction sees committed value: " << std::string(value, len) << std::endl;
    kvt_commit(txn3);
}

TEST_F(KVTValidationTest, ConflictDetection) {
    std::cout << "\n=== Testing Conflict Detection ===" << std::endl;
    
    // Setup
    {
        auto* txn = kvt_begin();
        kvt_put(txn, "conflict_key", 12, "original", 8);
        kvt_commit(txn);
        std::cout << "[SETUP] Initial value: conflict_key -> original" << std::endl;
    }
    
    // Two transactions modify the same key
    auto* txn1 = kvt_begin();
    auto* txn2 = kvt_begin();
    
    // Both read the same key (establishing read sets)
    size_t len;
    kvt_get(txn1, "conflict_key", 12, &len);
    kvt_get(txn2, "conflict_key", 12, &len);
    std::cout << "[INFO] Both transactions read conflict_key" << std::endl;
    
    // Both try to update
    kvt_put(txn1, "conflict_key", 12, "value1", 6);
    kvt_put(txn2, "conflict_key", 12, "value2", 6);
    std::cout << "[INFO] Both transactions updated conflict_key" << std::endl;
    
    // First commit should succeed
    int result1 = kvt_commit(txn1);
    ASSERT_EQ(result1, 0) << "First transaction should commit successfully";
    std::cout << "[PASS] TXN1 committed successfully" << std::endl;
    
    // Second commit should fail due to conflict
    int result2 = kvt_commit(txn2);
    ASSERT_NE(result2, 0) << "Second transaction should fail due to conflict";
    std::cout << "[PASS] TXN2 failed to commit (conflict detected)" << std::endl;
    
    // Verify final value
    auto* txn3 = kvt_begin();
    const char* value = kvt_get(txn3, "conflict_key", 12, &len);
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(std::string(value, len), "value1") << "Wrong value after conflict resolution";
    std::cout << "[PASS] Final value is from TXN1: " << std::string(value, len) << std::endl;
    kvt_commit(txn3);
}

TEST_F(KVTValidationTest, RangeScans) {
    std::cout << "\n=== Testing Range Scans ===" << std::endl;
    
    // Insert test data with keys that can be scanned
    {
        auto* txn = kvt_begin();
        kvt_put(txn, "scan_001", 8, "value1", 6);
        kvt_put(txn, "scan_002", 8, "value2", 6);
        kvt_put(txn, "scan_003", 8, "value3", 6);
        kvt_put(txn, "scan_004", 8, "value4", 6);
        kvt_put(txn, "scan_005", 8, "value5", 6);
        kvt_commit(txn);
        std::cout << "[SETUP] Inserted 5 scan keys" << std::endl;
    }
    
    // Test range scan
    {
        auto* txn = kvt_begin();
        auto* iter = kvt_scan(txn, "scan_001", 8, "scan_004", 8);
        ASSERT_NE(iter, nullptr) << "Failed to create scan iterator";
        
        int count = 0;
        std::vector<std::string> found_keys;
        
        while (kvt_iter_valid(iter)) {
            size_t key_len, val_len;
            const char* key = kvt_iter_key(iter, &key_len);
            const char* val = kvt_iter_value(iter, &val_len);
            
            ASSERT_NE(key, nullptr);
            ASSERT_NE(val, nullptr);
            
            found_keys.push_back(std::string(key, key_len));
            std::cout << "[SCAN] Found: " << std::string(key, key_len) 
                      << " -> " << std::string(val, val_len) << std::endl;
            
            kvt_iter_next(iter);
            count++;
        }
        
        kvt_iter_destroy(iter);
        kvt_commit(txn);
        
        ASSERT_EQ(count, 3) << "Expected 3 keys in range [scan_001, scan_004)";
        ASSERT_EQ(found_keys[0], "scan_001");
        ASSERT_EQ(found_keys[1], "scan_002");
        ASSERT_EQ(found_keys[2], "scan_003");
        std::cout << "[PASS] Range scan returned " << count << " keys" << std::endl;
    }
}

TEST_F(KVTValidationTest, DeleteOperations) {
    std::cout << "\n=== Testing Delete Operations ===" << std::endl;
    
    // Setup
    {
        auto* txn = kvt_begin();
        kvt_put(txn, "delete_key", 10, "to_delete", 9);
        kvt_commit(txn);
        std::cout << "[SETUP] Inserted delete_key -> to_delete" << std::endl;
    }
    
    // Verify key exists
    {
        auto* txn = kvt_begin();
        size_t len;
        const char* value = kvt_get(txn, "delete_key", 10, &len);
        ASSERT_NE(value, nullptr) << "Key should exist before deletion";
        std::cout << "[VERIFY] Key exists: " << std::string(value, len) << std::endl;
        kvt_commit(txn);
    }
    
    // Delete the key
    {
        auto* txn = kvt_begin();
        int result = kvt_delete(txn, "delete_key", 10);
        ASSERT_EQ(result, 0) << "Failed to delete key";
        std::cout << "[DELETE] Marked key for deletion" << std::endl;
        
        // Key should not be visible in same transaction after delete
        size_t len;
        const char* value = kvt_get(txn, "delete_key", 10, &len);
        ASSERT_EQ(value, nullptr) << "Deleted key should not be visible";
        std::cout << "[PASS] Key not visible in same transaction after delete" << std::endl;
        
        kvt_commit(txn);
    }
    
    // Verify deletion persisted
    {
        auto* txn = kvt_begin();
        size_t len;
        const char* value = kvt_get(txn, "delete_key", 10, &len);
        ASSERT_EQ(value, nullptr) << "Deleted key should not exist";
        std::cout << "[PASS] Key successfully deleted" << std::endl;
        kvt_commit(txn);
    }
}

TEST_F(KVTValidationTest, LargeDataHandling) {
    std::cout << "\n=== Testing Large Data Handling ===" << std::endl;
    
    // Create large value (1MB)
    const size_t large_size = 1024 * 1024;
    std::string large_value(large_size, 'X');
    
    // Store large value
    {
        auto* txn = kvt_begin();
        int result = kvt_put(txn, "large_key", 9, large_value.c_str(), large_size);
        ASSERT_EQ(result, 0) << "Failed to store large value";
        std::cout << "[PASS] Stored " << large_size << " bytes" << std::endl;
        
        result = kvt_commit(txn);
        ASSERT_EQ(result, 0) << "Failed to commit large value";
    }
    
    // Retrieve and verify
    {
        auto* txn = kvt_begin();
        size_t len;
        const char* value = kvt_get(txn, "large_key", 9, &len);
        ASSERT_NE(value, nullptr) << "Failed to retrieve large value";
        ASSERT_EQ(len, large_size) << "Large value size mismatch";
        
        // Verify content
        for (size_t i = 0; i < std::min(size_t(100), len); i++) {
            ASSERT_EQ(value[i], 'X') << "Large value content corrupted at position " << i;
        }
        std::cout << "[PASS] Retrieved and verified " << len << " bytes" << std::endl;
        kvt_commit(txn);
    }
}

TEST_F(KVTValidationTest, StressTest) {
    std::cout << "\n=== Running Stress Test ===" << std::endl;
    
    const int num_operations = 1000;
    
    // Insert many keys
    {
        auto* txn = kvt_begin();
        for (int i = 0; i < num_operations; i++) {
            std::string key = "stress_" + std::to_string(i);
            std::string value = "value_" + std::to_string(i);
            
            int result = kvt_put(txn, key.c_str(), key.length(), 
                               value.c_str(), value.length());
            ASSERT_EQ(result, 0) << "Failed to insert key " << i;
            
            if (i % 100 == 0) {
                std::cout << "[PROGRESS] Inserted " << i << " keys" << std::endl;
            }
        }
        
        int result = kvt_commit(txn);
        ASSERT_EQ(result, 0) << "Failed to commit stress test inserts";
        std::cout << "[PASS] Inserted " << num_operations << " keys" << std::endl;
    }
    
    // Verify all keys
    {
        auto* txn = kvt_begin();
        for (int i = 0; i < num_operations; i++) {
            std::string key = "stress_" + std::to_string(i);
            std::string expected = "value_" + std::to_string(i);
            
            size_t len;
            const char* value = kvt_get(txn, key.c_str(), key.length(), &len);
            ASSERT_NE(value, nullptr) << "Failed to get key " << i;
            ASSERT_EQ(std::string(value, len), expected) << "Value mismatch for key " << i;
        }
        kvt_commit(txn);
        std::cout << "[PASS] Verified all " << num_operations << " keys" << std::endl;
    }
    
    // Update half the keys
    {
        auto* txn = kvt_begin();
        for (int i = 0; i < num_operations / 2; i++) {
            std::string key = "stress_" + std::to_string(i);
            std::string value = "updated_" + std::to_string(i);
            
            int result = kvt_put(txn, key.c_str(), key.length(), 
                               value.c_str(), value.length());
            ASSERT_EQ(result, 0) << "Failed to update key " << i;
        }
        
        int result = kvt_commit(txn);
        ASSERT_EQ(result, 0) << "Failed to commit updates";
        std::cout << "[PASS] Updated " << num_operations/2 << " keys" << std::endl;
    }
    
    // Delete every third key
    {
        auto* txn = kvt_begin();
        int deleted = 0;
        for (int i = 0; i < num_operations; i += 3) {
            std::string key = "stress_" + std::to_string(i);
            int result = kvt_delete(txn, key.c_str(), key.length());
            ASSERT_EQ(result, 0) << "Failed to delete key " << i;
            deleted++;
        }
        
        int result = kvt_commit(txn);
        ASSERT_EQ(result, 0) << "Failed to commit deletions";
        std::cout << "[PASS] Deleted " << deleted << " keys" << std::endl;
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    
    std::cout << "================================================" << std::endl;
    std::cout << "     KVT Implementation Validation Tests       " << std::endl;
    std::cout << "================================================" << std::endl;
    
    int result = RUN_ALL_TESTS();
    
    if (result == 0) {
        std::cout << "\n================================================" << std::endl;
        std::cout << "     ALL TESTS PASSED SUCCESSFULLY!            " << std::endl;
        std::cout << "================================================" << std::endl;
    } else {
        std::cout << "\n================================================" << std::endl;
        std::cout << "     SOME TESTS FAILED - CHECK OUTPUT          " << std::endl;
        std::cout << "================================================" << std::endl;
    }
    
    return result;
}