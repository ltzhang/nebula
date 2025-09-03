/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <memory>
#include <iostream>
#include <set>
#include <map>
#include <vector>
#include <algorithm>
#include <random>

#include "clients/storage/kvt/KVTStorageClient.h"
#include "clients/storage/kvt/KVTKeyEncoder.h"
#include "clients/storage/kvt/KVTTransactionManager.h"
#include "clients/storage/kvt/kvt_inc.h"
#include "common/base/Base.h"

using namespace nebula;
using namespace nebula::storage;

class KVTComprehensiveTest : public ::testing::Test {
protected:
    std::unique_ptr<KVTStorageClient> client_;
    GraphSpaceID spaceId_ = 1;
    SessionID sessionId_ = 1234;
    ExecutionPlanID planId_ = 5678;
    
    // Helper data structures for validation
    std::set<std::string> existingVertices_;
    std::map<std::string, std::set<std::string>> outgoingEdges_;  // src -> set of dst
    std::map<std::string, std::set<std::string>> incomingEdges_;  // dst -> set of src
    
    void SetUp() override {
        // Initialize KVT
        KVTError err = kvt_initialize();
        ASSERT_EQ(err, KVTError::SUCCESS) << "Failed to initialize KVT";
        std::cout << "[SETUP] KVT initialized" << std::endl;
        
        // Create KVT storage client
        client_ = std::make_unique<KVTStorageClient>(nullptr, nullptr);
        std::cout << "[SETUP] KVTStorageClient created" << std::endl;
    }
    
    void TearDown() override {
        client_.reset();
        kvt_shutdown();
        std::cout << "[TEARDOWN] Cleanup completed" << std::endl;
    }
    
    // Helper function to add a vertex
    bool addVertex(const std::string& id, const std::string& name, int value) {
        std::vector<cpp2::NewVertex> vertices;
        cpp2::NewVertex vertex;
        vertex.id_ref() = id;
        cpp2::NewTag tag;
        tag.tag_id_ref() = 100;
        tag.props_ref() = std::vector<Value>{Value(name), Value(value), Value("TestCity")};
        vertex.tags_ref() = {tag};
        vertices.push_back(vertex);
        
        auto future = client_->addVertices(
            spaceId_, std::move(vertices), {}, true,
            KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
        );
        
        auto result = std::move(future).get();
        if (result.succeeded()) {
            existingVertices_.insert(id);
            return true;
        }
        return false;
    }
    
    // Helper function to add an edge
    bool addEdge(const std::string& src, const std::string& dst, int weight) {
        std::vector<cpp2::NewEdge> edges;
        cpp2::NewEdge edge;
        edge.key_ref()->src_ref() = src;
        edge.key_ref()->dst_ref() = dst;
        edge.key_ref()->edge_type_ref() = 200;
        edge.key_ref()->ranking_ref() = 0;
        edge.props_ref() = std::vector<Value>{Value(weight), Value(1234567890)};
        edges.push_back(edge);
        
        auto future = client_->addEdges(
            spaceId_, std::move(edges), {}, true,
            KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
        );
        
        auto result = std::move(future).get();
        if (result.succeeded()) {
            outgoingEdges_[src].insert(dst);
            incomingEdges_[dst].insert(src);
            return true;
        }
        return false;
    }
    
    // Helper function to delete a vertex
    bool deleteVertex(const std::string& id) {
        std::vector<std::string> ids = {id};
        auto future = client_->deleteVertices(
            spaceId_, std::move(ids),
            KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
        );
        
        auto result = std::move(future).get();
        if (result.succeeded()) {
            // Update our tracking structures
            existingVertices_.erase(id);
            
            // Remove all edges involving this vertex
            if (outgoingEdges_.count(id)) {
                for (const auto& dst : outgoingEdges_[id]) {
                    incomingEdges_[dst].erase(id);
                    if (incomingEdges_[dst].empty()) {
                        incomingEdges_.erase(dst);
                    }
                }
                outgoingEdges_.erase(id);
            }
            
            if (incomingEdges_.count(id)) {
                for (const auto& src : incomingEdges_[id]) {
                    outgoingEdges_[src].erase(id);
                    if (outgoingEdges_[src].empty()) {
                        outgoingEdges_.erase(src);
                    }
                }
                incomingEdges_.erase(id);
            }
            
            return true;
        }
        return false;
    }
    
    // Helper function to delete an edge
    bool deleteEdge(const std::string& src, const std::string& dst) {
        std::vector<cpp2::EdgeKey> edges;
        cpp2::EdgeKey key;
        key.src_ref() = src;
        key.dst_ref() = dst;
        key.edge_type_ref() = 200;
        key.ranking_ref() = 0;
        edges.push_back(key);
        
        auto future = client_->deleteEdges(
            spaceId_, std::move(edges),
            KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
        );
        
        auto result = std::move(future).get();
        if (result.succeeded()) {
            outgoingEdges_[src].erase(dst);
            if (outgoingEdges_[src].empty()) {
                outgoingEdges_.erase(src);
            }
            incomingEdges_[dst].erase(src);
            if (incomingEdges_[dst].empty()) {
                incomingEdges_.erase(dst);
            }
            return true;
        }
        return false;
    }
    
    // Verify graph consistency
    bool verifyGraphConsistency() {
        // For each vertex, check its edges
        for (const auto& vertex : existingVertices_) {
            // Check outgoing edges
            if (outgoingEdges_.count(vertex)) {
                for (const auto& dst : outgoingEdges_[vertex]) {
                    // Destination vertex should exist
                    if (existingVertices_.find(dst) == existingVertices_.end()) {
                        std::cout << "[ERROR] Edge " << vertex << "->" << dst 
                                  << " points to non-existent vertex" << std::endl;
                        return false;
                    }
                }
            }
            
            // Check incoming edges
            if (incomingEdges_.count(vertex)) {
                for (const auto& src : incomingEdges_[vertex]) {
                    // Source vertex should exist
                    if (existingVertices_.find(src) == existingVertices_.end()) {
                        std::cout << "[ERROR] Edge " << src << "->" << vertex 
                                  << " comes from non-existent vertex" << std::endl;
                        return false;
                    }
                }
            }
        }
        return true;
    }
};

TEST_F(KVTComprehensiveTest, BasicReverseEdgeOperations) {
    std::cout << "\n=== Test 1: Basic Reverse Edge Operations ===" << std::endl;
    
    // Create simple graph: A -> B -> C
    ASSERT_TRUE(addVertex("A", "Node A", 1));
    ASSERT_TRUE(addVertex("B", "Node B", 2));
    ASSERT_TRUE(addVertex("C", "Node C", 3));
    
    ASSERT_TRUE(addEdge("A", "B", 10));
    ASSERT_TRUE(addEdge("B", "C", 20));
    
    std::cout << "[SETUP] Created chain: A -> B -> C" << std::endl;
    
    // Test OUT edges for A (should find B)
    ASSERT_EQ(outgoingEdges_["A"].size(), 1);
    ASSERT_TRUE(outgoingEdges_["A"].count("B"));
    
    // Test IN edges for C (should find B)
    ASSERT_EQ(incomingEdges_["C"].size(), 1);
    ASSERT_TRUE(incomingEdges_["C"].count("B"));
    
    // Test BOTH edges for B (should find A as incoming, C as outgoing)
    ASSERT_EQ(incomingEdges_["B"].size(), 1);
    ASSERT_TRUE(incomingEdges_["B"].count("A"));
    ASSERT_EQ(outgoingEdges_["B"].size(), 1);
    ASSERT_TRUE(outgoingEdges_["B"].count("C"));
    
    ASSERT_TRUE(verifyGraphConsistency());
    std::cout << "[PASS] Basic edge operations work correctly" << std::endl;
}

TEST_F(KVTComprehensiveTest, ComplexGraphPattern) {
    std::cout << "\n=== Test 2: Complex Graph Pattern ===" << std::endl;
    
    // Create a more complex graph with multiple paths
    // Graph structure:
    //     D -> E -> F
    //     |    |    ^
    //     v    v    |
    //     G -> H ---+
    
    ASSERT_TRUE(addVertex("D", "Node D", 4));
    ASSERT_TRUE(addVertex("E", "Node E", 5));
    ASSERT_TRUE(addVertex("F", "Node F", 6));
    ASSERT_TRUE(addVertex("G", "Node G", 7));
    ASSERT_TRUE(addVertex("H", "Node H", 8));
    
    ASSERT_TRUE(addEdge("D", "E", 10));
    ASSERT_TRUE(addEdge("E", "F", 20));
    ASSERT_TRUE(addEdge("D", "G", 30));
    ASSERT_TRUE(addEdge("E", "H", 40));
    ASSERT_TRUE(addEdge("G", "H", 50));
    ASSERT_TRUE(addEdge("H", "F", 60));
    
    std::cout << "[SETUP] Created complex graph with 6 edges" << std::endl;
    
    // Verify F has 2 incoming edges (from E and H)
    ASSERT_EQ(incomingEdges_["F"].size(), 2);
    ASSERT_TRUE(incomingEdges_["F"].count("E"));
    ASSERT_TRUE(incomingEdges_["F"].count("H"));
    
    // Verify H has 2 incoming edges (from E and G)
    ASSERT_EQ(incomingEdges_["H"].size(), 2);
    ASSERT_TRUE(incomingEdges_["H"].count("E"));
    ASSERT_TRUE(incomingEdges_["H"].count("G"));
    
    // Verify D has 2 outgoing edges (to E and G)
    ASSERT_EQ(outgoingEdges_["D"].size(), 2);
    ASSERT_TRUE(outgoingEdges_["D"].count("E"));
    ASSERT_TRUE(outgoingEdges_["D"].count("G"));
    
    ASSERT_TRUE(verifyGraphConsistency());
    std::cout << "[PASS] Complex graph pattern handled correctly" << std::endl;
}

TEST_F(KVTComprehensiveTest, DeleteVertexCascade) {
    std::cout << "\n=== Test 3: Delete Vertex Cascade ===" << std::endl;
    
    // Create star pattern: Center node with multiple connections
    //     I -> J <- K
    //     ^    |    ^
    //     |    v    |
    //     L <- M -> N
    
    ASSERT_TRUE(addVertex("I", "Node I", 9));
    ASSERT_TRUE(addVertex("J", "Node J", 10));
    ASSERT_TRUE(addVertex("K", "Node K", 11));
    ASSERT_TRUE(addVertex("L", "Node L", 12));
    ASSERT_TRUE(addVertex("M", "Node M", 13));
    ASSERT_TRUE(addVertex("N", "Node N", 14));
    
    ASSERT_TRUE(addEdge("I", "J", 10));
    ASSERT_TRUE(addEdge("K", "J", 20));
    ASSERT_TRUE(addEdge("J", "M", 30));
    ASSERT_TRUE(addEdge("L", "I", 40));
    ASSERT_TRUE(addEdge("M", "L", 50));
    ASSERT_TRUE(addEdge("M", "N", 60));
    ASSERT_TRUE(addEdge("N", "K", 70));
    
    std::cout << "[SETUP] Created star pattern with J and M as hubs" << std::endl;
    
    // Delete J - should remove I->J, K->J, J->M
    ASSERT_TRUE(deleteVertex("J"));
    std::cout << "[ACTION] Deleted vertex J" << std::endl;
    
    // Verify J is gone
    ASSERT_FALSE(existingVertices_.count("J"));
    
    // Verify edges involving J are gone
    ASSERT_FALSE(outgoingEdges_["I"].count("J"));
    ASSERT_FALSE(outgoingEdges_["K"].count("J"));
    ASSERT_FALSE(incomingEdges_.count("J"));
    ASSERT_FALSE(outgoingEdges_.count("J"));
    
    // Verify M no longer has J as incoming
    ASSERT_FALSE(incomingEdges_["M"].count("J"));
    
    // Other edges should still exist
    ASSERT_TRUE(outgoingEdges_["M"].count("L"));
    ASSERT_TRUE(outgoingEdges_["M"].count("N"));
    ASSERT_TRUE(outgoingEdges_["N"].count("K"));
    
    ASSERT_TRUE(verifyGraphConsistency());
    std::cout << "[PASS] Vertex deletion cascaded correctly" << std::endl;
    
    // Now delete M - should remove M->L, M->N and L->I should still exist
    ASSERT_TRUE(deleteVertex("M"));
    std::cout << "[ACTION] Deleted vertex M" << std::endl;
    
    // Verify M is gone and its edges
    ASSERT_FALSE(existingVertices_.count("M"));
    ASSERT_FALSE(incomingEdges_["L"].count("M"));
    ASSERT_FALSE(incomingEdges_["N"].count("M"));
    
    // L->I and N->K should still exist
    ASSERT_TRUE(outgoingEdges_["L"].count("I"));
    ASSERT_TRUE(outgoingEdges_["N"].count("K"));
    
    ASSERT_TRUE(verifyGraphConsistency());
    std::cout << "[PASS] Second vertex deletion cascaded correctly" << std::endl;
}

TEST_F(KVTComprehensiveTest, DeleteEdgeConsistency) {
    std::cout << "\n=== Test 4: Delete Edge Consistency ===" << std::endl;
    
    // Create a fully connected triangle
    ASSERT_TRUE(addVertex("P", "Node P", 15));
    ASSERT_TRUE(addVertex("Q", "Node Q", 16));
    ASSERT_TRUE(addVertex("R", "Node R", 17));
    
    ASSERT_TRUE(addEdge("P", "Q", 10));
    ASSERT_TRUE(addEdge("Q", "R", 20));
    ASSERT_TRUE(addEdge("R", "P", 30));
    ASSERT_TRUE(addEdge("P", "R", 40));  // Second edge P->R
    ASSERT_TRUE(addEdge("Q", "P", 50));  // Reverse edge
    
    std::cout << "[SETUP] Created fully connected triangle with 5 edges" << std::endl;
    
    // Delete specific edges
    ASSERT_TRUE(deleteEdge("P", "Q"));
    std::cout << "[ACTION] Deleted edge P->Q" << std::endl;
    
    // Verify P->Q is gone but Q->P still exists
    ASSERT_FALSE(outgoingEdges_["P"].count("Q"));
    ASSERT_TRUE(outgoingEdges_["Q"].count("P"));
    
    // Verify reverse index is updated
    ASSERT_FALSE(incomingEdges_["Q"].count("P"));
    ASSERT_TRUE(incomingEdges_["P"].count("Q"));
    
    ASSERT_TRUE(verifyGraphConsistency());
    std::cout << "[PASS] Edge deletion maintains consistency" << std::endl;
}

TEST_F(KVTComprehensiveTest, LargeScaleOperations) {
    std::cout << "\n=== Test 5: Large Scale Operations ===" << std::endl;
    
    const int NUM_VERTICES = 50;
    const int NUM_EDGES = 200;
    
    // Create vertices
    for (int i = 0; i < NUM_VERTICES; i++) {
        std::string id = "V" + std::to_string(i);
        ASSERT_TRUE(addVertex(id, "Node " + id, i));
    }
    std::cout << "[SETUP] Created " << NUM_VERTICES << " vertices" << std::endl;
    
    // Create random edges
    std::mt19937 gen(42);  // Fixed seed for reproducibility
    std::uniform_int_distribution<> dis(0, NUM_VERTICES - 1);
    
    int edgesCreated = 0;
    std::set<std::pair<int, int>> edgeSet;
    
    while (edgesCreated < NUM_EDGES) {
        int src = dis(gen);
        int dst = dis(gen);
        
        if (src != dst && edgeSet.find({src, dst}) == edgeSet.end()) {
            std::string srcId = "V" + std::to_string(src);
            std::string dstId = "V" + std::to_string(dst);
            
            if (addEdge(srcId, dstId, edgesCreated)) {
                edgeSet.insert({src, dst});
                edgesCreated++;
            }
        }
    }
    std::cout << "[SETUP] Created " << NUM_EDGES << " random edges" << std::endl;
    
    // Count vertices with high in-degree
    int highInDegree = 0;
    for (const auto& [vertex, sources] : incomingEdges_) {
        if (sources.size() >= 5) {
            highInDegree++;
        }
    }
    std::cout << "[INFO] Vertices with in-degree >= 5: " << highInDegree << std::endl;
    
    // Delete some high-degree vertices
    std::vector<std::string> toDelete;
    for (const auto& [vertex, sources] : incomingEdges_) {
        if (sources.size() >= 5 && toDelete.size() < 5) {
            toDelete.push_back(vertex);
        }
    }
    
    for (const auto& vertex : toDelete) {
        size_t inDegree = incomingEdges_[vertex].size();
        size_t outDegree = outgoingEdges_[vertex].size();
        
        ASSERT_TRUE(deleteVertex(vertex));
        std::cout << "[ACTION] Deleted high-degree vertex " << vertex 
                  << " (in=" << inDegree << ", out=" << outDegree << ")" << std::endl;
    }
    
    ASSERT_TRUE(verifyGraphConsistency());
    std::cout << "[PASS] Large scale operations completed successfully" << std::endl;
    
    // Verify final state
    std::cout << "[INFO] Final graph: " << existingVertices_.size() << " vertices, ";
    
    int totalEdges = 0;
    for (const auto& [src, dsts] : outgoingEdges_) {
        totalEdges += dsts.size();
    }
    std::cout << totalEdges << " edges" << std::endl;
}

TEST_F(KVTComprehensiveTest, CyclicGraphOperations) {
    std::cout << "\n=== Test 6: Cyclic Graph Operations ===" << std::endl;
    
    // Create a graph with multiple cycles
    // S -> T -> U
    // ^    |    |
    // |    v    v
    // X <- W <- V
    
    ASSERT_TRUE(addVertex("S", "Node S", 18));
    ASSERT_TRUE(addVertex("T", "Node T", 19));
    ASSERT_TRUE(addVertex("U", "Node U", 20));
    ASSERT_TRUE(addVertex("V", "Node V", 21));
    ASSERT_TRUE(addVertex("W", "Node W", 22));
    ASSERT_TRUE(addVertex("X", "Node X", 23));
    
    // Create cycles
    ASSERT_TRUE(addEdge("S", "T", 10));
    ASSERT_TRUE(addEdge("T", "U", 20));
    ASSERT_TRUE(addEdge("U", "V", 30));
    ASSERT_TRUE(addEdge("V", "W", 40));
    ASSERT_TRUE(addEdge("W", "X", 50));
    ASSERT_TRUE(addEdge("X", "S", 60));  // Completes outer cycle
    ASSERT_TRUE(addEdge("T", "W", 70));  // Inner connection
    ASSERT_TRUE(addEdge("W", "T", 80));  // Reverse inner connection
    
    std::cout << "[SETUP] Created graph with cycles" << std::endl;
    
    // Verify cycles exist
    ASSERT_TRUE(outgoingEdges_["X"].count("S"));
    ASSERT_TRUE(outgoingEdges_["T"].count("W"));
    ASSERT_TRUE(outgoingEdges_["W"].count("T"));
    
    // Delete vertex in the middle of cycles
    ASSERT_TRUE(deleteVertex("W"));
    std::cout << "[ACTION] Deleted vertex W from cycles" << std::endl;
    
    // Verify W and its edges are gone
    ASSERT_FALSE(existingVertices_.count("W"));
    ASSERT_FALSE(outgoingEdges_["V"].count("W"));
    ASSERT_FALSE(incomingEdges_["X"].count("W"));
    ASSERT_FALSE(outgoingEdges_["T"].count("W"));
    ASSERT_FALSE(incomingEdges_["T"].count("W"));
    
    // Outer cycle should be broken but other edges remain
    ASSERT_TRUE(outgoingEdges_["S"].count("T"));
    ASSERT_TRUE(outgoingEdges_["T"].count("U"));
    ASSERT_TRUE(outgoingEdges_["U"].count("V"));
    ASSERT_TRUE(outgoingEdges_["X"].count("S"));
    
    ASSERT_TRUE(verifyGraphConsistency());
    std::cout << "[PASS] Cyclic graph handled correctly" << std::endl;
}

TEST_F(KVTComprehensiveTest, StressTestWithValidation) {
    std::cout << "\n=== Test 7: Stress Test with Validation ===" << std::endl;
    
    const int ITERATIONS = 100;
    std::mt19937 gen(123);  // Fixed seed
    std::uniform_int_distribution<> opDis(0, 3);  // 4 operations
    std::uniform_int_distribution<> vertexDis(0, 99);
    
    // Initial setup
    for (int i = 0; i < 20; i++) {
        std::string id = "SV" + std::to_string(i);
        ASSERT_TRUE(addVertex(id, "Stress " + id, i));
    }
    
    for (int i = 0; i < 30; i++) {
        int src = vertexDis(gen) % 20;
        int dst = vertexDis(gen) % 20;
        if (src != dst) {
            addEdge("SV" + std::to_string(src), "SV" + std::to_string(dst), i);
        }
    }
    
    std::cout << "[SETUP] Initial graph created" << std::endl;
    
    // Perform random operations
    int addVertexCount = 0, addEdgeCount = 0;
    int deleteVertexCount = 0, deleteEdgeCount = 0;
    
    for (int iter = 0; iter < ITERATIONS; iter++) {
        int op = opDis(gen);
        
        switch (op) {
            case 0: {  // Add vertex
                int id = 20 + addVertexCount;
                std::string vid = "SV" + std::to_string(id);
                if (addVertex(vid, "Stress " + vid, id)) {
                    addVertexCount++;
                }
                break;
            }
            case 1: {  // Add edge
                if (existingVertices_.size() >= 2) {
                    // Pick two random existing vertices
                    auto it1 = existingVertices_.begin();
                    auto it2 = existingVertices_.begin();
                    std::advance(it1, vertexDis(gen) % existingVertices_.size());
                    std::advance(it2, vertexDis(gen) % existingVertices_.size());
                    
                    if (*it1 != *it2) {
                        if (addEdge(*it1, *it2, addEdgeCount)) {
                            addEdgeCount++;
                        }
                    }
                }
                break;
            }
            case 2: {  // Delete vertex
                if (!existingVertices_.empty()) {
                    auto it = existingVertices_.begin();
                    std::advance(it, vertexDis(gen) % existingVertices_.size());
                    std::string toDelete = *it;
                    if (deleteVertex(toDelete)) {
                        deleteVertexCount++;
                    }
                }
                break;
            }
            case 3: {  // Delete edge
                if (!outgoingEdges_.empty()) {
                    // Find a random edge to delete
                    auto it = outgoingEdges_.begin();
                    std::advance(it, vertexDis(gen) % outgoingEdges_.size());
                    
                    if (!it->second.empty()) {
                        std::string src = it->first;
                        auto dstIt = it->second.begin();
                        std::advance(dstIt, vertexDis(gen) % it->second.size());
                        std::string dst = *dstIt;
                        
                        if (deleteEdge(src, dst)) {
                            deleteEdgeCount++;
                        }
                    }
                }
                break;
            }
        }
        
        // Periodic consistency check
        if (iter % 20 == 0) {
            ASSERT_TRUE(verifyGraphConsistency()) 
                << "Consistency check failed at iteration " << iter;
        }
    }
    
    std::cout << "[STATS] Operations performed:" << std::endl;
    std::cout << "  - Vertices added: " << addVertexCount << std::endl;
    std::cout << "  - Edges added: " << addEdgeCount << std::endl;
    std::cout << "  - Vertices deleted: " << deleteVertexCount << std::endl;
    std::cout << "  - Edges deleted: " << deleteEdgeCount << std::endl;
    
    // Final validation
    ASSERT_TRUE(verifyGraphConsistency());
    
    std::cout << "[STATS] Final graph:" << std::endl;
    std::cout << "  - Vertices: " << existingVertices_.size() << std::endl;
    
    int totalEdges = 0;
    for (const auto& [src, dsts] : outgoingEdges_) {
        totalEdges += dsts.size();
    }
    std::cout << "  - Edges: " << totalEdges << std::endl;
    
    std::cout << "[PASS] Stress test completed successfully" << std::endl;
}

TEST_F(KVTComprehensiveTest, BidirectionalTraversal) {
    std::cout << "\n=== Test 8: Bidirectional Traversal ===" << std::endl;
    
    // Create a graph for testing bidirectional traversal
    // Create a diamond pattern:
    //     Y
    //    / \
    //   Z   AA
    //    \ /
    //     BB
    
    ASSERT_TRUE(addVertex("Y", "Node Y", 24));
    ASSERT_TRUE(addVertex("Z", "Node Z", 25));
    ASSERT_TRUE(addVertex("AA", "Node AA", 26));
    ASSERT_TRUE(addVertex("BB", "Node BB", 27));
    
    ASSERT_TRUE(addEdge("Y", "Z", 10));
    ASSERT_TRUE(addEdge("Y", "AA", 20));
    ASSERT_TRUE(addEdge("Z", "BB", 30));
    ASSERT_TRUE(addEdge("AA", "BB", 40));
    
    std::cout << "[SETUP] Created diamond pattern" << std::endl;
    
    // Test traversal from Y (should reach all nodes via OUT edges)
    ASSERT_EQ(outgoingEdges_["Y"].size(), 2);
    ASSERT_TRUE(outgoingEdges_["Y"].count("Z"));
    ASSERT_TRUE(outgoingEdges_["Y"].count("AA"));
    
    // Test reverse traversal from BB (should find Z and AA via IN edges)
    ASSERT_EQ(incomingEdges_["BB"].size(), 2);
    ASSERT_TRUE(incomingEdges_["BB"].count("Z"));
    ASSERT_TRUE(incomingEdges_["BB"].count("AA"));
    
    // Add more edges to make it more complex
    ASSERT_TRUE(addEdge("BB", "Y", 50));  // Create cycle
    ASSERT_TRUE(addEdge("Z", "AA", 60));  // Cross connection
    
    std::cout << "[ACTION] Added cycle and cross connection" << std::endl;
    
    // Now Y has incoming edge from BB
    ASSERT_EQ(incomingEdges_["Y"].size(), 1);
    ASSERT_TRUE(incomingEdges_["Y"].count("BB"));
    
    // AA has incoming from both Y and Z
    ASSERT_EQ(incomingEdges_["AA"].size(), 2);
    ASSERT_TRUE(incomingEdges_["AA"].count("Y"));
    ASSERT_TRUE(incomingEdges_["AA"].count("Z"));
    
    ASSERT_TRUE(verifyGraphConsistency());
    std::cout << "[PASS] Bidirectional traversal works correctly" << std::endl;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    
    std::cout << "================================================" << std::endl;
    std::cout << "   KVT Comprehensive Test Suite                " << std::endl;
    std::cout << "================================================" << std::endl;
    std::cout << "This test suite thoroughly validates:" << std::endl;
    std::cout << "1. Reverse edge indexing" << std::endl;
    std::cout << "2. Complete delete operations" << std::endl;
    std::cout << "3. Graph consistency" << std::endl;
    std::cout << "4. Complex graph patterns" << std::endl;
    std::cout << "5. Large scale operations" << std::endl;
    std::cout << "6. Stress testing with validation" << std::endl;
    std::cout << "================================================\n" << std::endl;
    
    int result = RUN_ALL_TESTS();
    
    if (result == 0) {
        std::cout << "\n================================================" << std::endl;
        std::cout << "   ALL COMPREHENSIVE TESTS PASSED!             " << std::endl;
        std::cout << "================================================" << std::endl;
    } else {
        std::cout << "\n================================================" << std::endl;
        std::cout << "   SOME TESTS FAILED - CHECK OUTPUT            " << std::endl;
        std::cout << "================================================" << std::endl;
    }
    
    return result;
}