/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <memory>
#include <iostream>
#include <set>
#include <map>
#include <vector>
#include <algorithm>
#include <random>
#include <cstring>

#include "../kvt_inc.h"

#define TEST_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "[FAIL] " << message << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            exit(1); \
        } \
    } while(0)

class KVTComprehensiveTest {
public:
    uint64_t vertexTableId_ = 0;
    uint64_t edgeTableId_ = 0;
    
    // Helper data structures for validation
    std::set<std::string> existingVertices_;
    std::map<std::string, std::set<std::string>> outgoingEdges_;  // src -> set of dst
    std::map<std::string, std::set<std::string>> incomingEdges_;  // dst -> set of src
    void SetUp() {
        // Initialize KVT
        KVTError err = kvt_initialize();
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to initialize KVT");
        std::cout << "[SETUP] KVT initialized" << std::endl;
        
        // Create tables for vertices and edges
        std::string error_msg;
        err = kvt_create_table("vertices", "hash", vertexTableId_, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to create vertex table: " + error_msg);
        
        err = kvt_create_table("edges", "range", edgeTableId_, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to create edge table: " + error_msg);
        
        std::cout << "[SETUP] Tables created - Vertex: " << vertexTableId_ << ", Edge: " << edgeTableId_ << std::endl;
    }
    
    void TearDown() {
        kvt_shutdown();
        std::cout << "[TEARDOWN] Cleanup completed" << std::endl;
    }
    
    std::string createVertexKey(const std::string& vertexId) {
        return "v:" + vertexId;
    }
    
    std::string createEdgeKey(const std::string& srcId, const std::string& dstId) {
        return "e:" + srcId + ":" + dstId;
    }
    
    std::string createReverseEdgeKey(const std::string& dstId, const std::string& srcId) {
        return "re:" + dstId + ":" + srcId;
    }
    
    void addVertex(const std::string& vertexId, const std::string& properties) {
        std::string error_msg;
        uint64_t txId;
        KVTError err = kvt_start_transaction(txId, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        std::string key = createVertexKey(vertexId);
        err = kvt_set(txId, vertexTableId_, key, properties, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to add vertex " + vertexId);
        
        err = kvt_commit_transaction(txId, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit vertex addition");
        
        existingVertices_.insert(vertexId);
        std::cout << "[VERTEX] Added: " << vertexId << std::endl;
    }
    
    void addEdge(const std::string& srcId, const std::string& dstId, const std::string& properties) {
        std::string error_msg;
        uint64_t txId;
        KVTError err = kvt_start_transaction(txId, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
        
        // Add forward edge
        std::string edgeKey = createEdgeKey(srcId, dstId);
        err = kvt_set(txId, edgeTableId_, edgeKey, properties, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to add edge");
        
        // Add reverse edge for efficient incoming edge queries
        std::string reverseKey = createReverseEdgeKey(dstId, srcId);
        err = kvt_set(txId, edgeTableId_, reverseKey, properties, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to add reverse edge");
        
        err = kvt_commit_transaction(txId, error_msg);
        TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit edge addition");
        
        outgoingEdges_[srcId].insert(dstId);
        incomingEdges_[dstId].insert(srcId);
        std::cout << "[EDGE] Added: " << srcId << " -> " << dstId << std::endl;
    }
    
    void validateGraphIntegrity() {
        std::cout << "\n[VALIDATE] Checking graph integrity..." << std::endl;
        
        // Validate all vertices exist
        for (const auto& vertexId : existingVertices_) {
            std::string error_msg;
            uint64_t txId;
            KVTError err = kvt_start_transaction(txId, error_msg);
            TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
            
            std::string key = createVertexKey(vertexId);
            std::string value;
            err = kvt_get(txId, vertexTableId_, key, value, error_msg);
            TEST_ASSERT(err == KVTError::SUCCESS, "Vertex " + vertexId + " should exist");
            
            kvt_commit_transaction(txId, error_msg);
        }
        std::cout << "[VALIDATE] All " << existingVertices_.size() << " vertices verified" << std::endl;
        
        // Validate all edges exist
        int edgeCount = 0;
        for (const auto& [src, dsts] : outgoingEdges_) {
            for (const auto& dst : dsts) {
                std::string error_msg;
                uint64_t txId;
                KVTError err = kvt_start_transaction(txId, error_msg);
                TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
                
                // Check forward edge
                std::string edgeKey = createEdgeKey(src, dst);
                std::string value;
                err = kvt_get(txId, edgeTableId_, edgeKey, value, error_msg);
                TEST_ASSERT(err == KVTError::SUCCESS, "Edge " + src + "->" + dst + " should exist");
                
                // Check reverse edge
                std::string reverseKey = createReverseEdgeKey(dst, src);
                err = kvt_get(txId, edgeTableId_, reverseKey, value, error_msg);
                TEST_ASSERT(err == KVTError::SUCCESS, "Reverse edge " + dst + "<-" + src + " should exist");
                
                kvt_commit_transaction(txId, error_msg);
                edgeCount++;
            }
        }
        std::cout << "[VALIDATE] All " << edgeCount << " edges (forward and reverse) verified" << std::endl;
    }
};

// Test basic graph operations
void TestBasicGraphOperations() {
    std::cout << "\n=== Test 1: Basic Graph Operations ===" << std::endl;
    
    KVTComprehensiveTest test;
    test.SetUp();
    
    // Add vertices
    test.addVertex("user1", "name:Alice,age:30");
    test.addVertex("user2", "name:Bob,age:25");
    test.addVertex("user3", "name:Charlie,age:35");
    
    // Add edges
    test.addEdge("user1", "user2", "type:friend,since:2020");
    test.addEdge("user2", "user3", "type:friend,since:2021");
    test.addEdge("user1", "user3", "type:colleague,since:2019");
    
    // Validate
    test.validateGraphIntegrity();
    
    test.TearDown();
    std::cout << "[PASS] Basic graph operations test completed" << std::endl;
}

// Test concurrent transactions
void TestConcurrentTransactions() {
    std::cout << "\n=== Test 2: Concurrent Transactions ===" << std::endl;
    
    KVTComprehensiveTest test;
    test.SetUp();
    
    // Add initial vertices
    test.addVertex("v1", "data:initial");
    test.addVertex("v2", "data:initial");
    
    std::string error_msg;
    
    // Start two concurrent transactions
    uint64_t tx1, tx2;
    KVTError err = kvt_start_transaction(tx1, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start tx1");
    
    err = kvt_start_transaction(tx2, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start tx2");
    
    // Both transactions read the same key first (to establish read set for OCC)
    std::string v1Key = test.createVertexKey("v1");
    std::string value1, value2;
    err = kvt_get(tx1, test.vertexTableId_, v1Key, value1, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "tx1 read failed");
    
    err = kvt_get(tx2, test.vertexTableId_, v1Key, value2, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "tx2 read failed");
    
    // Both transactions try to update the same vertex
    err = kvt_set(tx1, test.vertexTableId_, v1Key, "data:tx1_update", error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "tx1 update failed");
    
    err = kvt_set(tx2, test.vertexTableId_, v1Key, "data:tx2_update", error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "tx2 update failed");
    
    // Commit tx1
    err = kvt_commit_transaction(tx1, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "tx1 commit should succeed");
    std::cout << "[TX1] Committed successfully" << std::endl;
    
    // Try to commit tx2 - should fail due to conflict
    err = kvt_commit_transaction(tx2, error_msg);
    TEST_ASSERT(err == KVTError::TRANSACTION_HAS_STALE_DATA, "tx2 should fail due to conflict");
    std::cout << "[TX2] Failed to commit (expected conflict)" << std::endl;
    
    // Verify final state
    uint64_t tx3;
    err = kvt_start_transaction(tx3, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start tx3");
    
    std::string value;
    err = kvt_get(tx3, test.vertexTableId_, v1Key, value, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to get v1");
    TEST_ASSERT(value == "data:tx1_update", "v1 should have tx1's update");
    
    kvt_commit_transaction(tx3, error_msg);
    
    test.TearDown();
    std::cout << "[PASS] Concurrent transactions test completed" << std::endl;
}

// Test range scans on edges
void TestEdgeRangeScans() {
    std::cout << "\n=== Test 3: Edge Range Scans ===" << std::endl;
    
    KVTComprehensiveTest test;
    test.SetUp();
    
    // Create a star topology: center connects to multiple nodes
    test.addVertex("center", "type:hub");
    for (int i = 1; i <= 5; i++) {
        std::string nodeId = "node" + std::to_string(i);
        test.addVertex(nodeId, "type:leaf,id:" + std::to_string(i));
        test.addEdge("center", nodeId, "weight:" + std::to_string(i));
    }
    
    // Scan outgoing edges from center
    std::string error_msg;
    uint64_t txId;
    KVTError err = kvt_start_transaction(txId, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
    
    std::string scanStart = "e:center:";
    std::string scanEnd = "e:center;";  // ';' is after ':' in ASCII, will exclude all "e:center:" prefixed keys
    
    std::vector<std::pair<std::string, std::string>> results;
    err = kvt_scan(txId, test.edgeTableId_, scanStart, scanEnd, 100, results, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to scan edges");
    
    std::cout << "[SCAN] Found " << results.size() << " outgoing edges from 'center'" << std::endl;
    TEST_ASSERT(results.size() == 5, "Should find 5 outgoing edges");
    
    for (const auto& [key, value] : results) {
        std::cout << "[EDGE] " << key << " -> " << value << std::endl;
    }
    
    // Scan incoming edges to node3
    results.clear();
    scanStart = "re:node3:";
    scanEnd = "re:node3;";
    
    err = kvt_scan(txId, test.edgeTableId_, scanStart, scanEnd, 100, results, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to scan reverse edges");
    
    std::cout << "[SCAN] Found " << results.size() << " incoming edges to 'node3'" << std::endl;
    TEST_ASSERT(results.size() == 1, "Should find 1 incoming edge");
    
    kvt_commit_transaction(txId, error_msg);
    
    test.TearDown();
    std::cout << "[PASS] Edge range scans test completed" << std::endl;
}

// Test batch operations
void TestBatchOperations() {
    std::cout << "\n=== Test 4: Batch Operations ===" << std::endl;
    
    KVTComprehensiveTest test;
    test.SetUp();
    
    std::string error_msg;
    uint64_t txId;
    KVTError err = kvt_start_transaction(txId, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
    
    // Prepare batch operations
    KVTBatchOps ops;
    KVTBatchResults results;
    
    // Add multiple vertices in batch
    for (int i = 0; i < 10; i++) {
        KVTOp op;
        op.op = OP_SET;
        op.table_id = test.vertexTableId_;
        op.key = test.createVertexKey("batch_v" + std::to_string(i));
        op.value = "batch_data:" + std::to_string(i);
        ops.push_back(op);
    }
    
    // Add multiple edges in batch
    for (int i = 0; i < 9; i++) {
        KVTOp op;
        op.op = OP_SET;
        op.table_id = test.edgeTableId_;
        op.key = test.createEdgeKey("batch_v" + std::to_string(i), "batch_v" + std::to_string(i + 1));
        op.value = "edge_data:" + std::to_string(i);
        ops.push_back(op);
    }
    
    // Execute batch
    err = kvt_batch_execute(txId, ops, results, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Batch execution failed: " + error_msg);
    
    // Verify all operations succeeded
    int successCount = 0;
    for (const auto& result : results) {
        if (result.error == KVTError::SUCCESS) {
            successCount++;
        }
    }
    
    std::cout << "[BATCH] " << successCount << "/" << ops.size() << " operations succeeded" << std::endl;
    TEST_ASSERT(successCount == ops.size(), "Some batch operations failed");
    
    err = kvt_commit_transaction(txId, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit batch operations");
    
    // Verify data was written
    err = kvt_start_transaction(txId, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start verification transaction");
    
    std::string value;
    std::string key = test.createVertexKey("batch_v5");
    err = kvt_get(txId, test.vertexTableId_, key, value, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to get batch_v5");
    TEST_ASSERT(value == "batch_data:5", "batch_v5 data mismatch");
    
    kvt_commit_transaction(txId, error_msg);
    
    test.TearDown();
    std::cout << "[PASS] Batch operations test completed" << std::endl;
}

// Test vertex deletion with cascade
void TestVertexDeletionCascade() {
    std::cout << "\n=== Test 5: Vertex Deletion Cascade ===" << std::endl;
    
    KVTComprehensiveTest test;
    test.SetUp();
    
    // Create a small graph
    test.addVertex("a", "data:a");
    test.addVertex("b", "data:b");
    test.addVertex("c", "data:c");
    test.addEdge("a", "b", "edge:ab");
    test.addEdge("b", "c", "edge:bc");
    test.addEdge("a", "c", "edge:ac");
    
    // Delete vertex 'b' and its edges
    std::string error_msg;
    uint64_t txId;
    KVTError err = kvt_start_transaction(txId, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start transaction");
    
    // Delete vertex
    std::string vertexKey = test.createVertexKey("b");
    err = kvt_del(txId, test.vertexTableId_, vertexKey, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to delete vertex b");
    
    // Delete outgoing edges from b
    std::string edgeKey = test.createEdgeKey("b", "c");
    err = kvt_del(txId, test.edgeTableId_, edgeKey, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to delete edge b->c");
    
    std::string reverseKey = test.createReverseEdgeKey("c", "b");
    err = kvt_del(txId, test.edgeTableId_, reverseKey, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to delete reverse edge");
    
    // Delete incoming edges to b
    edgeKey = test.createEdgeKey("a", "b");
    err = kvt_del(txId, test.edgeTableId_, edgeKey, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to delete edge a->b");
    
    reverseKey = test.createReverseEdgeKey("b", "a");
    err = kvt_del(txId, test.edgeTableId_, reverseKey, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to delete reverse edge");
    
    err = kvt_commit_transaction(txId, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to commit deletion");
    
    // Verify deletion
    err = kvt_start_transaction(txId, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Failed to start verification transaction");
    
    std::string value;
    err = kvt_get(txId, test.vertexTableId_, vertexKey, value, error_msg);
    TEST_ASSERT(err == KVTError::KEY_NOT_FOUND, "Vertex b should be deleted");
    
    // Verify edge a->c still exists
    edgeKey = test.createEdgeKey("a", "c");
    err = kvt_get(txId, test.edgeTableId_, edgeKey, value, error_msg);
    TEST_ASSERT(err == KVTError::SUCCESS, "Edge a->c should still exist");
    
    kvt_commit_transaction(txId, error_msg);
    
    test.TearDown();
    std::cout << "[PASS] Vertex deletion cascade test completed" << std::endl;
}

// Stress test with many operations
void TestStressOperations() {
    std::cout << "\n=== Test 6: Stress Test ===" << std::endl;
    
    KVTComprehensiveTest test;
    test.SetUp();
    
    const int numVertices = 100;
    const int numEdges = 500;
    
    std::cout << "[STRESS] Creating " << numVertices << " vertices..." << std::endl;
    
    // Add many vertices
    for (int i = 0; i < numVertices; i++) {
        std::string id = "v" + std::to_string(i);
        test.addVertex(id, "data:" + std::to_string(i));
        
        if (i % 20 == 0) {
            std::cout << "[PROGRESS] Created " << i << " vertices" << std::endl;
        }
    }
    
    std::cout << "[STRESS] Creating " << numEdges << " edges..." << std::endl;
    
    // Add random edges
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, numVertices - 1);
    
    for (int i = 0; i < numEdges; i++) {
        int src = dis(gen);
        int dst = dis(gen);
        if (src != dst) {
            std::string srcId = "v" + std::to_string(src);
            std::string dstId = "v" + std::to_string(dst);
            test.addEdge(srcId, dstId, "edge:" + std::to_string(i));
        }
        
        if (i % 50 == 0) {
            std::cout << "[PROGRESS] Created " << i << " edges" << std::endl;
        }
    }
    
    // Validate final state
    test.validateGraphIntegrity();
    
    test.TearDown();
    std::cout << "[PASS] Stress test completed" << std::endl;
}

int main(int argc, char** argv) {
    std::cout << "=== KVT Comprehensive Test Suite ===" << std::endl;
    
    try {
        TestBasicGraphOperations();
        TestConcurrentTransactions();
        TestEdgeRangeScans();
        TestBatchOperations();
        TestVertexDeletionCascade();
        TestStressOperations();
        
        std::cout << "\n=== ALL COMPREHENSIVE TESTS PASSED ===" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "\n[ERROR] Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}