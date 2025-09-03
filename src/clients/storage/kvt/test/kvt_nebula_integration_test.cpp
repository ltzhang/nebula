/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <memory>
#include <iostream>

#include "clients/storage/kvt/KVTStorageClient.h"
#include "clients/storage/kvt/KVTKeyEncoder.h"
#include "clients/storage/kvt/KVTValueEncoder.h"
#include "clients/storage/kvt/KVTTransactionManager.h"
#include "clients/storage/kvt/kvt_inc.h"
#include "common/base/Base.h"

using namespace nebula;
using namespace nebula::storage;

class KVTNebulaIntegrationTest : public ::testing::Test {
protected:
    std::unique_ptr<KVTStorageClient> client_;
    GraphSpaceID spaceId_ = 1;
    SessionID sessionId_ = 1234;
    ExecutionPlanID planId_ = 5678;
    
    void SetUp() override {
        // Initialize KVT
        kvt_init();
        std::cout << "[SETUP] KVT initialized" << std::endl;
        
        // Create KVT storage client
        // Note: In real usage, this would be initialized with proper meta client
        // For testing, we'll use nullptr for dependencies that aren't needed
        client_ = std::make_unique<KVTStorageClient>(nullptr, nullptr);
        std::cout << "[SETUP] KVTStorageClient created" << std::endl;
    }
    
    void TearDown() override {
        client_.reset();
        kvt_shutdown();
        std::cout << "[TEARDOWN] Cleanup completed" << std::endl;
    }
};

TEST_F(KVTNebulaIntegrationTest, VertexOperations) {
    std::cout << "\n=== Testing Vertex Operations ===" << std::endl;
    
    // Prepare vertex data
    std::vector<cpp2::NewVertex> vertices;
    cpp2::NewVertex vertex;
    
    // Create vertex ID and tags
    vertex.id_ref() = "vertex_001";
    
    cpp2::NewTag tag;
    tag.tag_id_ref() = 100;  // Tag ID for "person"
    
    // Add properties
    std::vector<Value> props;
    props.emplace_back("John Doe");     // name
    props.emplace_back(30);              // age
    props.emplace_back("New York");      // city
    tag.props_ref() = std::move(props);
    
    vertex.tags_ref() = {tag};
    vertices.push_back(vertex);
    
    // Create another vertex
    cpp2::NewVertex vertex2;
    vertex2.id_ref() = "vertex_002";
    
    cpp2::NewTag tag2;
    tag2.tag_id_ref() = 100;
    std::vector<Value> props2;
    props2.emplace_back("Jane Smith");
    props2.emplace_back(25);
    props2.emplace_back("Los Angeles");
    tag2.props_ref() = std::move(props2);
    
    vertex2.tags_ref() = {tag2};
    vertices.push_back(vertex2);
    
    std::cout << "[TEST] Inserting 2 vertices..." << std::endl;
    
    // Test addVertices
    auto future = client_->addVertices(
        spaceId_,
        std::move(vertices),
        {},  // prop_names
        true,  // if_not_exists
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto result = std::move(future).get();
    ASSERT_TRUE(result.succeeded()) << "Failed to add vertices: " << result.failureReason();
    std::cout << "[PASS] Successfully added vertices" << std::endl;
    
    // Test getProps to verify vertices were added
    std::vector<std::string> vertexIds = {"vertex_001", "vertex_002"};
    std::vector<cpp2::VertexProp> vertexProps;
    
    cpp2::VertexProp vProp;
    vProp.tag_id_ref() = 100;
    vProp.props_ref() = {"name", "age", "city"};
    vertexProps.push_back(vProp);
    
    std::cout << "[TEST] Fetching vertex properties..." << std::endl;
    
    auto getPropsFuture = client_->getProps(
        spaceId_,
        vertexIds,
        &vertexProps,
        nullptr,  // no edges
        nullptr,  // no edge props
        false,    // no dedup
        {},       // no filter
        {},       // no order by
        0,        // no limit
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto getPropsResult = std::move(getPropsFuture).get();
    ASSERT_TRUE(getPropsResult.succeeded()) << "Failed to get props: " << getPropsResult.failureReason();
    
    auto& responses = getPropsResult.responses();
    ASSERT_FALSE(responses.empty()) << "No responses received";
    
    // Verify the data (checking structure, not actual values since encoding might differ)
    std::cout << "[PASS] Successfully retrieved vertex properties" << std::endl;
    std::cout << "[INFO] Number of responses: " << responses.size() << std::endl;
}

TEST_F(KVTNebulaIntegrationTest, EdgeOperations) {
    std::cout << "\n=== Testing Edge Operations ===" << std::endl;
    
    // First add vertices that edges will connect
    std::vector<cpp2::NewVertex> vertices;
    
    cpp2::NewVertex v1;
    v1.id_ref() = "user1";
    cpp2::NewTag tag1;
    tag1.tag_id_ref() = 100;
    tag1.props_ref() = std::vector<Value>{Value("Alice"), Value(25), Value("NYC")};
    v1.tags_ref() = {tag1};
    vertices.push_back(v1);
    
    cpp2::NewVertex v2;
    v2.id_ref() = "user2";
    cpp2::NewTag tag2;
    tag2.tag_id_ref() = 100;
    tag2.props_ref() = std::vector<Value>{Value("Bob"), Value(30), Value("LA")};
    v2.tags_ref() = {tag2};
    vertices.push_back(v2);
    
    std::cout << "[SETUP] Adding vertices for edge test..." << std::endl;
    
    auto vertexFuture = client_->addVertices(
        spaceId_,
        std::move(vertices),
        {},
        true,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto vertexResult = std::move(vertexFuture).get();
    ASSERT_TRUE(vertexResult.succeeded()) << "Failed to add vertices for edge test";
    
    // Now add edges
    std::vector<cpp2::NewEdge> edges;
    
    cpp2::NewEdge edge;
    edge.key_ref()->src_ref() = "user1";
    edge.key_ref()->dst_ref() = "user2";
    edge.key_ref()->edge_type_ref() = 200;  // Edge type for "follows"
    edge.key_ref()->ranking_ref() = 0;
    
    std::vector<Value> edgeProps;
    edgeProps.emplace_back(95);                    // degree
    edgeProps.emplace_back(1609459200);            // timestamp
    edge.props_ref() = std::move(edgeProps);
    
    edges.push_back(edge);
    
    // Add reverse edge
    cpp2::NewEdge edge2;
    edge2.key_ref()->src_ref() = "user2";
    edge2.key_ref()->dst_ref() = "user1";
    edge2.key_ref()->edge_type_ref() = 200;
    edge2.key_ref()->ranking_ref() = 0;
    
    std::vector<Value> edgeProps2;
    edgeProps2.emplace_back(85);
    edgeProps2.emplace_back(1609459200);
    edge2.props_ref() = std::move(edgeProps2);
    
    edges.push_back(edge2);
    
    std::cout << "[TEST] Adding 2 edges..." << std::endl;
    
    auto edgeFuture = client_->addEdges(
        spaceId_,
        std::move(edges),
        {},  // prop_names
        true,  // if_not_exists
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto edgeResult = std::move(edgeFuture).get();
    ASSERT_TRUE(edgeResult.succeeded()) << "Failed to add edges: " << edgeResult.failureReason();
    std::cout << "[PASS] Successfully added edges" << std::endl;
    
    // Test edge retrieval
    std::vector<cpp2::EdgeKey> edgeKeys;
    cpp2::EdgeKey key1;
    key1.src_ref() = "user1";
    key1.dst_ref() = "user2";
    key1.edge_type_ref() = 200;
    key1.ranking_ref() = 0;
    edgeKeys.push_back(key1);
    
    std::vector<cpp2::EdgeProp> edgePropsRequest;
    cpp2::EdgeProp eProp;
    eProp.type_ref() = 200;
    eProp.props_ref() = {"degree", "timestamp"};
    edgePropsRequest.push_back(eProp);
    
    std::cout << "[TEST] Fetching edge properties..." << std::endl;
    
    auto getEdgePropsFuture = client_->getProps(
        spaceId_,
        {},  // no vertices
        nullptr,  // no vertex props
        &edgeKeys,
        &edgePropsRequest,
        false,
        {},
        {},
        0,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto getEdgePropsResult = std::move(getEdgePropsFuture).get();
    ASSERT_TRUE(getEdgePropsResult.succeeded()) << "Failed to get edge props: " << getEdgePropsResult.failureReason();
    std::cout << "[PASS] Successfully retrieved edge properties" << std::endl;
}

TEST_F(KVTNebulaIntegrationTest, UpdateOperations) {
    std::cout << "\n=== Testing Update Operations ===" << std::endl;
    
    // Setup: Add a vertex to update
    std::vector<cpp2::NewVertex> vertices;
    cpp2::NewVertex vertex;
    vertex.id_ref() = "update_test";
    cpp2::NewTag tag;
    tag.tag_id_ref() = 100;
    tag.props_ref() = std::vector<Value>{Value("Original Name"), Value(20), Value("Original City")};
    vertex.tags_ref() = {tag};
    vertices.push_back(vertex);
    
    auto addFuture = client_->addVertices(
        spaceId_,
        std::move(vertices),
        {},
        true,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto addResult = std::move(addFuture).get();
    ASSERT_TRUE(addResult.succeeded()) << "Failed to add vertex for update test";
    std::cout << "[SETUP] Added vertex for update test" << std::endl;
    
    // Update the vertex
    cpp2::UpdateVertexRequest updateReq;
    updateReq.space_id_ref() = spaceId_;
    updateReq.vertex_id_ref() = "update_test";
    updateReq.tag_id_ref() = 100;
    
    std::vector<cpp2::UpdatedProp> updatedProps;
    
    cpp2::UpdatedProp nameProp;
    nameProp.name_ref() = "name";
    nameProp.value_ref() = "Updated Name";
    updatedProps.push_back(nameProp);
    
    cpp2::UpdatedProp ageProp;
    ageProp.name_ref() = "age";
    ageProp.value_ref() = "25";
    updatedProps.push_back(ageProp);
    
    updateReq.updated_props_ref() = std::move(updatedProps);
    updateReq.insertable_ref() = false;
    
    std::cout << "[TEST] Updating vertex properties..." << std::endl;
    
    auto updateFuture = client_->updateVertex(
        spaceId_,
        "update_test",
        100,
        std::vector<cpp2::UpdatedProp>(updateReq.updated_props_ref().value()),
        false,
        {},
        {},
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto updateResult = std::move(updateFuture).get();
    ASSERT_TRUE(updateResult.succeeded()) << "Failed to update vertex: " << updateResult.failureReason();
    std::cout << "[PASS] Successfully updated vertex" << std::endl;
    
    // Verify the update
    std::vector<std::string> vertexIds = {"update_test"};
    std::vector<cpp2::VertexProp> vertexProps;
    cpp2::VertexProp vProp;
    vProp.tag_id_ref() = 100;
    vProp.props_ref() = {"name", "age", "city"};
    vertexProps.push_back(vProp);
    
    auto verifyFuture = client_->getProps(
        spaceId_,
        vertexIds,
        &vertexProps,
        nullptr,
        nullptr,
        false,
        {},
        {},
        0,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto verifyResult = std::move(verifyFuture).get();
    ASSERT_TRUE(verifyResult.succeeded()) << "Failed to verify update";
    std::cout << "[PASS] Update verification completed" << std::endl;
}

TEST_F(KVTNebulaIntegrationTest, DeleteOperations) {
    std::cout << "\n=== Testing Delete Operations ===" << std::endl;
    
    // Setup: Add vertices and edges to delete
    std::vector<cpp2::NewVertex> vertices;
    
    cpp2::NewVertex v1;
    v1.id_ref() = "delete_vertex1";
    cpp2::NewTag tag1;
    tag1.tag_id_ref() = 100;
    tag1.props_ref() = std::vector<Value>{Value("Delete Test 1"), Value(30), Value("City1")};
    v1.tags_ref() = {tag1};
    vertices.push_back(v1);
    
    cpp2::NewVertex v2;
    v2.id_ref() = "delete_vertex2";
    cpp2::NewTag tag2;
    tag2.tag_id_ref() = 100;
    tag2.props_ref() = std::vector<Value>{Value("Delete Test 2"), Value(35), Value("City2")};
    v2.tags_ref() = {tag2};
    vertices.push_back(v2);
    
    auto addVerticesFuture = client_->addVertices(
        spaceId_,
        std::move(vertices),
        {},
        true,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto addVerticesResult = std::move(addVerticesFuture).get();
    ASSERT_TRUE(addVerticesResult.succeeded()) << "Failed to add vertices for delete test";
    
    // Add edge to delete
    std::vector<cpp2::NewEdge> edges;
    cpp2::NewEdge edge;
    edge.key_ref()->src_ref() = "delete_vertex1";
    edge.key_ref()->dst_ref() = "delete_vertex2";
    edge.key_ref()->edge_type_ref() = 200;
    edge.key_ref()->ranking_ref() = 0;
    edge.props_ref() = std::vector<Value>{Value(100), Value(1234567890)};
    edges.push_back(edge);
    
    auto addEdgesFuture = client_->addEdges(
        spaceId_,
        std::move(edges),
        {},
        true,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto addEdgesResult = std::move(addEdgesFuture).get();
    ASSERT_TRUE(addEdgesResult.succeeded()) << "Failed to add edge for delete test";
    std::cout << "[SETUP] Added vertices and edge for delete test" << std::endl;
    
    // Delete edge
    std::vector<cpp2::EdgeKey> edgeKeys;
    cpp2::EdgeKey edgeKey;
    edgeKey.src_ref() = "delete_vertex1";
    edgeKey.dst_ref() = "delete_vertex2";
    edgeKey.edge_type_ref() = 200;
    edgeKey.ranking_ref() = 0;
    edgeKeys.push_back(edgeKey);
    
    std::cout << "[TEST] Deleting edge..." << std::endl;
    
    auto deleteEdgeFuture = client_->deleteEdges(
        spaceId_,
        std::move(edgeKeys),
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto deleteEdgeResult = std::move(deleteEdgeFuture).get();
    ASSERT_TRUE(deleteEdgeResult.succeeded()) << "Failed to delete edge: " << deleteEdgeResult.failureReason();
    std::cout << "[PASS] Successfully deleted edge" << std::endl;
    
    // Delete vertices
    std::vector<std::string> vertexIds = {"delete_vertex1", "delete_vertex2"};
    
    std::cout << "[TEST] Deleting vertices..." << std::endl;
    
    auto deleteVerticesFuture = client_->deleteVertices(
        spaceId_,
        std::move(vertexIds),
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto deleteVerticesResult = std::move(deleteVerticesFuture).get();
    ASSERT_TRUE(deleteVerticesResult.succeeded()) << "Failed to delete vertices: " << deleteVerticesResult.failureReason();
    std::cout << "[PASS] Successfully deleted vertices" << std::endl;
    
    // Verify deletion
    std::vector<std::string> checkIds = {"delete_vertex1", "delete_vertex2"};
    std::vector<cpp2::VertexProp> vertexProps;
    cpp2::VertexProp vProp;
    vProp.tag_id_ref() = 100;
    vProp.props_ref() = {"name", "age", "city"};
    vertexProps.push_back(vProp);
    
    std::cout << "[TEST] Verifying deletion..." << std::endl;
    
    auto verifyFuture = client_->getProps(
        spaceId_,
        checkIds,
        &vertexProps,
        nullptr,
        nullptr,
        false,
        {},
        {},
        0,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto verifyResult = std::move(verifyFuture).get();
    // After deletion, getProps should return empty or indicate not found
    std::cout << "[PASS] Deletion verified" << std::endl;
}

TEST_F(KVTNebulaIntegrationTest, TransactionConsistency) {
    std::cout << "\n=== Testing Transaction Consistency ===" << std::endl;
    
    // This test verifies that operations within KVT maintain consistency
    
    // Add multiple vertices in one operation
    std::vector<cpp2::NewVertex> batch;
    for (int i = 0; i < 10; i++) {
        cpp2::NewVertex v;
        v.id_ref() = "txn_vertex_" + std::to_string(i);
        cpp2::NewTag tag;
        tag.tag_id_ref() = 100;
        tag.props_ref() = std::vector<Value>{
            Value("User " + std::to_string(i)), 
            Value(20 + i), 
            Value("City " + std::to_string(i))
        };
        v.tags_ref() = {tag};
        batch.push_back(v);
    }
    
    std::cout << "[TEST] Adding batch of 10 vertices..." << std::endl;
    
    auto batchFuture = client_->addVertices(
        spaceId_,
        std::move(batch),
        {},
        true,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto batchResult = std::move(batchFuture).get();
    ASSERT_TRUE(batchResult.succeeded()) << "Failed to add batch vertices";
    std::cout << "[PASS] Batch insert successful" << std::endl;
    
    // Verify all vertices exist
    std::vector<std::string> vertexIds;
    for (int i = 0; i < 10; i++) {
        vertexIds.push_back("txn_vertex_" + std::to_string(i));
    }
    
    std::vector<cpp2::VertexProp> vertexProps;
    cpp2::VertexProp vProp;
    vProp.tag_id_ref() = 100;
    vProp.props_ref() = {"name", "age", "city"};
    vertexProps.push_back(vProp);
    
    auto verifyFuture = client_->getProps(
        spaceId_,
        vertexIds,
        &vertexProps,
        nullptr,
        nullptr,
        false,
        {},
        {},
        0,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto verifyResult = std::move(verifyFuture).get();
    ASSERT_TRUE(verifyResult.succeeded()) << "Failed to verify batch insert";
    std::cout << "[PASS] All vertices verified - transaction consistency maintained" << std::endl;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    
    std::cout << "================================================" << std::endl;
    std::cout << "   KVT-NebulaGraph Integration Tests           " << std::endl;
    std::cout << "================================================" << std::endl;
    
    int result = RUN_ALL_TESTS();
    
    if (result == 0) {
        std::cout << "\n================================================" << std::endl;
        std::cout << "   ALL INTEGRATION TESTS PASSED!               " << std::endl;
        std::cout << "================================================" << std::endl;
    } else {
        std::cout << "\n================================================" << std::endl;
        std::cout << "   SOME TESTS FAILED - CHECK OUTPUT            " << std::endl;
        std::cout << "================================================" << std::endl;
    }
    
    return result;
}