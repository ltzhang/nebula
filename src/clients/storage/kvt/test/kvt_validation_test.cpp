/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <memory>
#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <cstring>
#include <algorithm>

#include "../kvt_inc.h"

#define TEST_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "[FAIL] " << message << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            exit(1); \
        } \
    } while(0)

class KVTValidationTest {
public:
    void SetUp() {
        KVTError err = kvt_initialize();
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to initialize KVT");
        std::cout << "[TEST] KVT initialized successfully" << std::endl;
    }

    void TearDown() {
        kvt_shutdown();
        std::cout << "[TEST] KVT shutdown successfully" << std::endl;
    }
};

void TestBasicOperations() {
    std::cout << "\n=== Testing Basic KVT Operations ===" << std::endl;
    
    // Create a table first
    std::string error_msg;
    uint64_t table_id;
    KVTError err = kvt_create_table("test_table", "hash", table_id, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to create table: " + error_msg);
    std::cout << "[PASS] Table created with ID: " << table_id << std::endl;
    
    // Test 1: Simple put and get
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to begin transaction: " + error_msg);
        std::cout << "[PASS] Transaction started with ID: " << tx_id << std::endl;
        
        std::string key = "test_key_1";
        std::string value = "test_value_1";
        
        err = kvt_set(tx_id, table_id, key, value, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to put key-value pair: " + error_msg);
        std::cout << "[PASS] Put operation: " << key << " -> " << value << std::endl;
        
        err = kvt_commit_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit transaction: " + error_msg);
        std::cout << "[PASS] Transaction committed" << std::endl;
    }
    
    // Test 2: Read back the value
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to begin read transaction: " + error_msg);
        
        std::string key = "test_key_1";
        std::string retrieved_value;
        
        err = kvt_get(tx_id, table_id, key, retrieved_value, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to get value: " + error_msg);
        TEST_ASSERT(retrieved_value == "test_value_1", "Value mismatch");
        std::cout << "[PASS] Get operation: " << key << " -> " << retrieved_value << std::endl;
        
        kvt_commit_transaction(tx_id, error_msg);
    }
}

void TestTransactionIsolation() {
    std::cout << "\n=== Testing Transaction Isolation ===" << std::endl;
    
    // Create table
    std::string error_msg;
    uint64_t table_id;
    KVTError err = kvt_create_table("iso_table", "hash", table_id, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to create table: " + error_msg);
    
    // Setup: Insert initial data
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        err = kvt_set(tx_id, table_id, "iso_key", "initial", error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to set initial value");
        
        err = kvt_commit_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit");
        std::cout << "[SETUP] Initial value: iso_key -> initial" << std::endl;
    }
    
    // Test: Start two transactions
    uint64_t txn1, txn2;
    err = kvt_start_transaction(txn1, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start txn1");
    
    err = kvt_start_transaction(txn2, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start txn2");
    std::cout << "[PASS] Started two concurrent transactions" << std::endl;
    
    // Txn1 updates the value
    err = kvt_set(txn1, table_id, "iso_key", "txn1_value", error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to update in txn1");
    std::cout << "[TXN1] Updated iso_key -> txn1_value (not committed)" << std::endl;
    
    // Txn2 should still see the original value
    std::string value;
    err = kvt_get(txn2, table_id, "iso_key", value, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to get in txn2");
    TEST_ASSERT(value == "initial", "Transaction isolation violated");
    std::cout << "[PASS] TXN2 still sees original value: " << value << std::endl;
    
    // Commit txn1
    err = kvt_commit_transaction(txn1, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit txn1");
    std::cout << "[TXN1] Committed successfully" << std::endl;
    
    // Txn2 should still see snapshot value
    err = kvt_get(txn2, table_id, "iso_key", value, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to get in txn2 after commit");
    TEST_ASSERT(value == "initial", "Snapshot isolation violated");
    std::cout << "[PASS] TXN2 maintains snapshot isolation" << std::endl;
    
    kvt_commit_transaction(txn2, error_msg);
    
    // New transaction should see the updated value
    uint64_t txn3;
    err = kvt_start_transaction(txn3, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start txn3");
    
    err = kvt_get(txn3, table_id, "iso_key", value, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to get in txn3");
    TEST_ASSERT(value == "txn1_value", "Update not visible after commit");
    std::cout << "[PASS] New transaction sees committed value: " << value << std::endl;
    kvt_commit_transaction(txn3, error_msg);
}

void TestConflictDetection() {
    std::cout << "\n=== Testing Conflict Detection ===" << std::endl;
    
    // Create table
    std::string error_msg;
    uint64_t table_id;
    KVTError err = kvt_create_table("conflict_table", "hash", table_id, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to create table: " + error_msg);
    
    // Setup
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        err = kvt_set(tx_id, table_id, "conflict_key", "original", error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to set initial value");
        
        err = kvt_commit_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit");
        std::cout << "[SETUP] Initial value: conflict_key -> original" << std::endl;
    }
    
    // Two transactions modify the same key
    uint64_t txn1, txn2;
    err = kvt_start_transaction(txn1, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start txn1");
    
    err = kvt_start_transaction(txn2, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start txn2");
    
    // Both read the same key (establishing read sets)
    std::string value;
    err = kvt_get(txn1, table_id, "conflict_key", value, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to get in txn1");
    
    err = kvt_get(txn2, table_id, "conflict_key", value, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to get in txn2");
    std::cout << "[INFO] Both transactions read conflict_key" << std::endl;
    
    // Both try to update
    err = kvt_set(txn1, table_id, "conflict_key", "value1", error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to update in txn1");
    
    err = kvt_set(txn2, table_id, "conflict_key", "value2", error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to update in txn2");
    std::cout << "[INFO] Both transactions updated conflict_key" << std::endl;
    
    // First commit should succeed
    err = kvt_commit_transaction(txn1, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "First transaction should commit successfully");
    std::cout << "[PASS] TXN1 committed successfully" << std::endl;
    
    // Second commit should fail due to conflict
    err = kvt_commit_transaction(txn2, error_msg);
    TEST_ASSERT(err == KVTError::TRANSACTION_HAS_STALE_DATA, "Second transaction should fail due to conflict");
    std::cout << "[PASS] TXN2 failed to commit (conflict detected)" << std::endl;
    
    // Verify final value
    uint64_t txn3;
    err = kvt_start_transaction(txn3, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start txn3");
    
    err = kvt_get(txn3, table_id, "conflict_key", value, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to get final value");
    TEST_ASSERT(value == "value1", "Wrong value after conflict resolution");
    std::cout << "[PASS] Final value is from TXN1: " << value << std::endl;
    kvt_commit_transaction(txn3, error_msg);
}

void TestRangeScans() {
    std::cout << "\n=== Testing Range Scans ===" << std::endl;
    
    // Create a range-partitioned table for scans
    std::string error_msg;
    uint64_t table_id;
    KVTError err = kvt_create_table("scan_table", "range", table_id, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to create range table: " + error_msg);
    
    // Insert test data with keys that can be scanned
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        err = kvt_set(tx_id, table_id, "scan_001", "value1", error_msg);
        err = kvt_set(tx_id, table_id, "scan_002", "value2", error_msg);
        err = kvt_set(tx_id, table_id, "scan_003", "value3", error_msg);
        err = kvt_set(tx_id, table_id, "scan_004", "value4", error_msg);
        err = kvt_set(tx_id, table_id, "scan_005", "value5", error_msg);
        
        err = kvt_commit_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit");
        std::cout << "[SETUP] Inserted 5 scan keys" << std::endl;
    }
    
    // Test range scan
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        std::vector<std::pair<std::string, std::string>> results;
        err = kvt_scan(tx_id, table_id, "scan_001", "scan_004", 10, results, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to perform scan: " + error_msg);
        
        // The scan is inclusive at start, exclusive at end [scan_001, scan_004)
        TEST_ASSERT(results.size() == 3, "Expected 3 keys in range [scan_001, scan_004)");
        TEST_ASSERT(results[0].first == "scan_001", "First key mismatch");
        TEST_ASSERT(results[1].first == "scan_002", "Second key mismatch");
        TEST_ASSERT(results[2].first == "scan_003", "Third key mismatch");
        
        for (const auto& [key, val] : results) {
            std::cout << "[SCAN] Found: " << key << " -> " << val << std::endl;
        }
        
        std::cout << "[PASS] Range scan returned " << results.size() << " keys" << std::endl;
        kvt_commit_transaction(tx_id, error_msg);
    }
}

void TestDeleteOperations() {
    std::cout << "\n=== Testing Delete Operations ===" << std::endl;
    
    // Create table
    std::string error_msg;
    uint64_t table_id;
    KVTError err = kvt_create_table("delete_table", "hash", table_id, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to create table: " + error_msg);
    
    // Setup
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        err = kvt_set(tx_id, table_id, "delete_key", "to_delete", error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to set value");
        
        err = kvt_commit_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit");
        std::cout << "[SETUP] Inserted delete_key -> to_delete" << std::endl;
    }
    
    // Verify key exists
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        std::string value;
        err = kvt_get(tx_id, table_id, "delete_key", value, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Key should exist before deletion");
        std::cout << "[VERIFY] Key exists: " << value << std::endl;
        kvt_commit_transaction(tx_id, error_msg);
    }
    
    // Delete the key
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        err = kvt_del(tx_id, table_id, "delete_key", error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to delete key");
        std::cout << "[DELETE] Marked key for deletion" << std::endl;
        
        // Key should not be visible in same transaction after delete
        std::string value;
        err = kvt_get(tx_id, table_id, "delete_key", value, error_msg);
        TEST_ASSERT(err == KVTError::KEY_IS_DELETED || err == KVTError::KEY_NOT_FOUND, "Deleted key should not be visible");
        std::cout << "[PASS] Key not visible in same transaction after delete" << std::endl;
        
        err = kvt_commit_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit deletion");
    }
    
    // Verify deletion persisted
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        std::string value;
        err = kvt_get(tx_id, table_id, "delete_key", value, error_msg);
        TEST_ASSERT(err == KVTError::KEY_NOT_FOUND, "Deleted key should not exist");
        std::cout << "[PASS] Key successfully deleted" << std::endl;
        kvt_commit_transaction(tx_id, error_msg);
    }
}

void TestLargeDataHandling() {
    std::cout << "\n=== Testing Large Data Handling ===" << std::endl;
    
    // Create table
    std::string error_msg;
    uint64_t table_id;
    KVTError err = kvt_create_table("large_table", "hash", table_id, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to create table: " + error_msg);
    
    // Create large value (1MB)
    const size_t large_size = 1024 * 1024;
    std::string large_value(large_size, 'X');
    
    // Store large value
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        err = kvt_set(tx_id, table_id, "large_key", large_value, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to store large value");
        std::cout << "[PASS] Stored " << large_size << " bytes" << std::endl;
        
        err = kvt_commit_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit large value");
    }
    
    // Retrieve and verify
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        std::string value;
        err = kvt_get(tx_id, table_id, "large_key", value, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to retrieve large value");
        TEST_ASSERT(value.size() == large_size, "Large value size mismatch");
        
        // Verify content
        for (size_t i = 0; i < std::min(size_t(100), value.size()); i++) {
            TEST_ASSERT(value[i] == 'X', "Large value content corrupted at position " + std::to_string(i));
        }
        std::cout << "[PASS] Retrieved and verified " << value.size() << " bytes" << std::endl;
        kvt_commit_transaction(tx_id, error_msg);
    }
}

void TestStressTest() {
    std::cout << "\n=== Running Stress Test ===" << std::endl;
    
    // Create table
    std::string error_msg;
    uint64_t table_id;
    KVTError err = kvt_create_table("stress_table", "hash", table_id, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to create table: " + error_msg);
    
    const int num_keys = 1000;
    
    // Insert many keys
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        for (int i = 0; i < num_keys; i++) {
            std::string key = "stress_key_" + std::to_string(i);
            std::string value = "stress_value_" + std::to_string(i);
            
            err = kvt_set(tx_id, table_id, key, value, error_msg);
            TEST_ASSERT(err == KVTError::SUCCESS, "Failed to insert key " + key);
            
            if (i % 100 == 0) {
                std::cout << "[PROGRESS] Inserted " << i << " keys" << std::endl;
            }
        }
        
        err = kvt_commit_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit stress test inserts");
        std::cout << "[PASS] Inserted " << num_keys << " keys" << std::endl;
    }
    
    // Verify all keys
    {
        uint64_t tx_id;
        err = kvt_start_transaction(tx_id, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        for (int i = 0; i < num_keys; i++) {
            std::string key = "stress_key_" + std::to_string(i);
            std::string expected_value = "stress_value_" + std::to_string(i);
            std::string value;
            
            err = kvt_get(tx_id, table_id, key, value, error_msg);
            TEST_ASSERT(err == KVTError::SUCCESS, "Failed to get key " + key);
            TEST_ASSERT(value == expected_value, "Value mismatch for key " + key);
            
            if (i % 100 == 0) {
                std::cout << "[PROGRESS] Verified " << i << " keys" << std::endl;
            }
        }
        
        kvt_commit_transaction(tx_id, error_msg);
        std::cout << "[PASS] Verified all " << num_keys << " keys" << std::endl;
    }
}

// Main test runner
int main(int argc, char** argv) {
    KVTValidationTest test;
    
    std::cout << "=== KVT Validation Test Suite ===" << std::endl;
    std::cout << "Initializing KVT system..." << std::endl;
    
    test.SetUp();
    
    try {
        TestBasicOperations();
        TestTransactionIsolation();
        TestConflictDetection();
        TestRangeScans();
        TestDeleteOperations();
        TestLargeDataHandling();
        TestStressTest();
        
        std::cout << "\n=== ALL TESTS PASSED ===" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "\n[ERROR] Test failed with exception: " << e.what() << std::endl;
        test.TearDown();
        return 1;
    }
    
    test.TearDown();
    return 0;
}