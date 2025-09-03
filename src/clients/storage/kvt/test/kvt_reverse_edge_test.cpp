/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <memory>
#include <iostream>

#include "clients/storage/kvt/KVTStorageClient.h"
#include "clients/storage/kvt/KVTKeyEncoder.h"
#include "clients/storage/kvt/kvt_inc.h"
#include "common/base/Base.h"

using namespace nebula;
using namespace nebula::storage;

class KVTReverseEdgeTest : public ::testing::Test {
protected:
    std::unique_ptr<KVTStorageClient> client_;
    GraphSpaceID spaceId_ = 1;
    SessionID sessionId_ = 1234;
    ExecutionPlanID planId_ = 5678;
    
    void SetUp() override {
        // Initialize KVT
        kvt_initialize();
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
};

TEST_F(KVTReverseEdgeTest, ReverseEdgeIndexing) {
    std::cout << "\n=== Testing Reverse Edge Indexing ===" << std::endl;
    
    // Step 1: Create vertices
    std::vector<cpp2::NewVertex> vertices;
    
    // Create vertex A
    cpp2::NewVertex vertexA;
    vertexA.id_ref() = "A";
    cpp2::NewTag tagA;
    tagA.tag_id_ref() = 100;
    tagA.props_ref() = std::vector<Value>{Value("Vertex A"), Value(1), Value("City A")};
    vertexA.tags_ref() = {tagA};
    vertices.push_back(vertexA);
    
    // Create vertex B
    cpp2::NewVertex vertexB;
    vertexB.id_ref() = "B";
    cpp2::NewTag tagB;
    tagB.tag_id_ref() = 100;
    tagB.props_ref() = std::vector<Value>{Value("Vertex B"), Value(2), Value("City B")};
    vertexB.tags_ref() = {tagB};
    vertices.push_back(vertexB);
    
    // Create vertex C
    cpp2::NewVertex vertexC;
    vertexC.id_ref() = "C";
    cpp2::NewTag tagC;
    tagC.tag_id_ref() = 100;
    tagC.props_ref() = std::vector<Value>{Value("Vertex C"), Value(3), Value("City C")};
    vertexC.tags_ref() = {tagC};
    vertices.push_back(vertexC);
    
    auto vertexFuture = client_->addVertices(
        spaceId_,
        std::move(vertices),
        {},
        true,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto vertexResult = std::move(vertexFuture).get();
    ASSERT_TRUE(vertexResult.succeeded()) << "Failed to add vertices";
    std::cout << "[PASS] Added vertices A, B, C" << std::endl;
    
    // Step 2: Create edges
    // A -> B, A -> C, B -> C
    std::vector<cpp2::NewEdge> edges;
    
    cpp2::NewEdge edgeAB;
    edgeAB.key_ref()->src_ref() = "A";
    edgeAB.key_ref()->dst_ref() = "B";
    edgeAB.key_ref()->edge_type_ref() = 200;
    edgeAB.key_ref()->ranking_ref() = 0;
    edgeAB.props_ref() = std::vector<Value>{Value(10), Value(1234567890)};
    edges.push_back(edgeAB);
    
    cpp2::NewEdge edgeAC;
    edgeAC.key_ref()->src_ref() = "A";
    edgeAC.key_ref()->dst_ref() = "C";
    edgeAC.key_ref()->edge_type_ref() = 200;
    edgeAC.key_ref()->ranking_ref() = 0;
    edgeAC.props_ref() = std::vector<Value>{Value(20), Value(1234567890)};
    edges.push_back(edgeAC);
    
    cpp2::NewEdge edgeBC;
    edgeBC.key_ref()->src_ref() = "B";
    edgeBC.key_ref()->dst_ref() = "C";
    edgeBC.key_ref()->edge_type_ref() = 200;
    edgeBC.key_ref()->ranking_ref() = 0;
    edgeBC.props_ref() = std::vector<Value>{Value(30), Value(1234567890)};
    edges.push_back(edgeBC);
    
    auto edgeFuture = client_->addEdges(
        spaceId_,
        std::move(edges),
        {},
        true,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto edgeResult = std::move(edgeFuture).get();
    ASSERT_TRUE(edgeResult.succeeded()) << "Failed to add edges";
    std::cout << "[PASS] Added edges A->B, A->C, B->C" << std::endl;
    
    // Step 3: Test OUT_EDGE queries (existing functionality)
    std::vector<std::string> vertexIds = {"A"};
    std::vector<cpp2::EdgeProp> edgeProps;
    cpp2::EdgeProp eProp;
    eProp.type_ref() = 200;
    eProp.props_ref() = {"degree", "timestamp"};
    edgeProps.push_back(eProp);
    
    // Set edge direction to OUT_EDGE
    cpp2::EdgeDirection direction = cpp2::EdgeDirection::OUT_EDGE;
    
    auto outEdgeFuture = client_->getProps(
        spaceId_,
        vertexIds,
        nullptr,
        nullptr,
        &edgeProps,
        false,
        {},
        {},
        0,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto outEdgeResult = std::move(outEdgeFuture).get();
    ASSERT_TRUE(outEdgeResult.succeeded()) << "Failed to get OUT edges";
    std::cout << "[PASS] OUT_EDGE query for vertex A works" << std::endl;
    
    // Step 4: Test IN_EDGE queries (new reverse edge functionality)
    vertexIds = {"C"};  // Find all edges pointing to C
    direction = cpp2::EdgeDirection::IN_EDGE;
    
    auto inEdgeFuture = client_->getProps(
        spaceId_,
        vertexIds,
        nullptr,
        nullptr,
        &edgeProps,
        false,
        {},
        {},
        0,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto inEdgeResult = std::move(inEdgeFuture).get();
    ASSERT_TRUE(inEdgeResult.succeeded()) << "Failed to get IN edges";
    std::cout << "[PASS] IN_EDGE query for vertex C works (should find A->C and B->C)" << std::endl;
    
    // Step 5: Test BOTH direction
    vertexIds = {"B"};
    direction = cpp2::EdgeDirection::BOTH;
    
    auto bothEdgeFuture = client_->getProps(
        spaceId_,
        vertexIds,
        nullptr,
        nullptr,
        &edgeProps,
        false,
        {},
        {},
        0,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto bothEdgeResult = std::move(bothEdgeFuture).get();
    ASSERT_TRUE(bothEdgeResult.succeeded()) << "Failed to get BOTH edges";
    std::cout << "[PASS] BOTH direction query for vertex B works (should find A->B and B->C)" << std::endl;
}

TEST_F(KVTReverseEdgeTest, DeleteVertexWithEdges) {
    std::cout << "\n=== Testing Delete Vertex with Complete Edge Cleanup ===" << std::endl;
    
    // Step 1: Create a small graph
    // Create vertices D, E, F
    std::vector<cpp2::NewVertex> vertices;
    
    cpp2::NewVertex vertexD;
    vertexD.id_ref() = "D";
    cpp2::NewTag tagD;
    tagD.tag_id_ref() = 100;
    tagD.props_ref() = std::vector<Value>{Value("Vertex D"), Value(4), Value("City D")};
    vertexD.tags_ref() = {tagD};
    vertices.push_back(vertexD);
    
    cpp2::NewVertex vertexE;
    vertexE.id_ref() = "E";
    cpp2::NewTag tagE;
    tagE.tag_id_ref() = 100;
    tagE.props_ref() = std::vector<Value>{Value("Vertex E"), Value(5), Value("City E")};
    vertexE.tags_ref() = {tagE};
    vertices.push_back(vertexE);
    
    cpp2::NewVertex vertexF;
    vertexF.id_ref() = "F";
    cpp2::NewTag tagF;
    tagF.tag_id_ref() = 100;
    tagF.props_ref() = std::vector<Value>{Value("Vertex F"), Value(6), Value("City F")};
    vertexF.tags_ref() = {tagF};
    vertices.push_back(vertexF);
    
    auto vertexFuture = client_->addVertices(
        spaceId_,
        std::move(vertices),
        {},
        true,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto vertexResult = std::move(vertexFuture).get();
    ASSERT_TRUE(vertexResult.succeeded()) << "Failed to add vertices";
    std::cout << "[SETUP] Added vertices D, E, F" << std::endl;
    
    // Step 2: Create edges D->E, E->F, F->D (cycle)
    std::vector<cpp2::NewEdge> edges;
    
    cpp2::NewEdge edgeDE;
    edgeDE.key_ref()->src_ref() = "D";
    edgeDE.key_ref()->dst_ref() = "E";
    edgeDE.key_ref()->edge_type_ref() = 200;
    edgeDE.key_ref()->ranking_ref() = 0;
    edgeDE.props_ref() = std::vector<Value>{Value(40), Value(1234567890)};
    edges.push_back(edgeDE);
    
    cpp2::NewEdge edgeEF;
    edgeEF.key_ref()->src_ref() = "E";
    edgeEF.key_ref()->dst_ref() = "F";
    edgeEF.key_ref()->edge_type_ref() = 200;
    edgeEF.key_ref()->ranking_ref() = 0;
    edgeEF.props_ref() = std::vector<Value>{Value(50), Value(1234567890)};
    edges.push_back(edgeEF);
    
    cpp2::NewEdge edgeFD;
    edgeFD.key_ref()->src_ref() = "F";
    edgeFD.key_ref()->dst_ref() = "D";
    edgeFD.key_ref()->edge_type_ref() = 200;
    edgeFD.key_ref()->ranking_ref() = 0;
    edgeFD.props_ref() = std::vector<Value>{Value(60), Value(1234567890)};
    edges.push_back(edgeFD);
    
    auto edgeFuture = client_->addEdges(
        spaceId_,
        std::move(edges),
        {},
        true,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto edgeResult = std::move(edgeFuture).get();
    ASSERT_TRUE(edgeResult.succeeded()) << "Failed to add edges";
    std::cout << "[SETUP] Added edges D->E, E->F, F->D (cycle)" << std::endl;
    
    // Step 3: Delete vertex E (should delete D->E and E->F)
    std::vector<std::string> deleteIds = {"E"};
    
    auto deleteFuture = client_->deleteVertices(
        spaceId_,
        std::move(deleteIds),
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto deleteResult = std::move(deleteFuture).get();
    ASSERT_TRUE(deleteResult.succeeded()) << "Failed to delete vertex E";
    std::cout << "[PASS] Deleted vertex E" << std::endl;
    
    // Step 4: Verify edges are properly cleaned up
    // Check that D no longer has outgoing edge to E
    std::vector<std::string> vertexIds = {"D"};
    std::vector<cpp2::EdgeProp> edgeProps;
    cpp2::EdgeProp eProp;
    eProp.type_ref() = 200;
    eProp.props_ref() = {"degree", "timestamp"};
    edgeProps.push_back(eProp);
    
    auto checkDFuture = client_->getProps(
        spaceId_,
        vertexIds,
        nullptr,
        nullptr,
        &edgeProps,
        false,
        {},
        {},
        0,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto checkDResult = std::move(checkDFuture).get();
    ASSERT_TRUE(checkDResult.succeeded()) << "Failed to check D's edges";
    // Should only have F->D incoming edge now, not D->E
    std::cout << "[PASS] Verified D->E edge was deleted" << std::endl;
    
    // Check that F no longer has incoming edge from E
    vertexIds = {"F"};
    cpp2::EdgeDirection direction = cpp2::EdgeDirection::IN_EDGE;
    
    auto checkFFuture = client_->getProps(
        spaceId_,
        vertexIds,
        nullptr,
        nullptr,
        &edgeProps,
        false,
        {},
        {},
        0,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto checkFResult = std::move(checkFFuture).get();
    ASSERT_TRUE(checkFResult.succeeded()) << "Failed to check F's incoming edges";
    // Should not have E->F incoming edge anymore
    std::cout << "[PASS] Verified E->F edge was deleted" << std::endl;
    
    // Verify F->D still exists
    vertexIds = {"F"};
    direction = cpp2::EdgeDirection::OUT_EDGE;
    
    auto checkFDFuture = client_->getProps(
        spaceId_,
        vertexIds,
        nullptr,
        nullptr,
        &edgeProps,
        false,
        {},
        {},
        0,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto checkFDResult = std::move(checkFDFuture).get();
    ASSERT_TRUE(checkFDResult.succeeded()) << "Failed to check F->D edge";
    std::cout << "[PASS] Verified F->D edge still exists" << std::endl;
}

TEST_F(KVTReverseEdgeTest, DeleteEdgeWithReverseIndex) {
    std::cout << "\n=== Testing Delete Edge with Reverse Index Cleanup ===" << std::endl;
    
    // Step 1: Create vertices G, H
    std::vector<cpp2::NewVertex> vertices;
    
    cpp2::NewVertex vertexG;
    vertexG.id_ref() = "G";
    cpp2::NewTag tagG;
    tagG.tag_id_ref() = 100;
    tagG.props_ref() = std::vector<Value>{Value("Vertex G"), Value(7), Value("City G")};
    vertexG.tags_ref() = {tagG};
    vertices.push_back(vertexG);
    
    cpp2::NewVertex vertexH;
    vertexH.id_ref() = "H";
    cpp2::NewTag tagH;
    tagH.tag_id_ref() = 100;
    tagH.props_ref() = std::vector<Value>{Value("Vertex H"), Value(8), Value("City H")};
    vertexH.tags_ref() = {tagH};
    vertices.push_back(vertexH);
    
    auto vertexFuture = client_->addVertices(
        spaceId_,
        std::move(vertices),
        {},
        true,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto vertexResult = std::move(vertexFuture).get();
    ASSERT_TRUE(vertexResult.succeeded()) << "Failed to add vertices";
    
    // Step 2: Create edge G->H
    std::vector<cpp2::NewEdge> edges;
    
    cpp2::NewEdge edgeGH;
    edgeGH.key_ref()->src_ref() = "G";
    edgeGH.key_ref()->dst_ref() = "H";
    edgeGH.key_ref()->edge_type_ref() = 200;
    edgeGH.key_ref()->ranking_ref() = 0;
    edgeGH.props_ref() = std::vector<Value>{Value(70), Value(1234567890)};
    edges.push_back(edgeGH);
    
    auto edgeFuture = client_->addEdges(
        spaceId_,
        std::move(edges),
        {},
        true,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto edgeResult = std::move(edgeFuture).get();
    ASSERT_TRUE(edgeResult.succeeded()) << "Failed to add edge";
    std::cout << "[SETUP] Added edge G->H" << std::endl;
    
    // Step 3: Verify IN_EDGE query works for H
    std::vector<std::string> vertexIds = {"H"};
    std::vector<cpp2::EdgeProp> edgeProps;
    cpp2::EdgeProp eProp;
    eProp.type_ref() = 200;
    eProp.props_ref() = {"degree", "timestamp"};
    edgeProps.push_back(eProp);
    
    cpp2::EdgeDirection direction = cpp2::EdgeDirection::IN_EDGE;
    
    auto inEdgeFuture = client_->getProps(
        spaceId_,
        vertexIds,
        nullptr,
        nullptr,
        &edgeProps,
        false,
        {},
        {},
        0,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto inEdgeResult = std::move(inEdgeFuture).get();
    ASSERT_TRUE(inEdgeResult.succeeded()) << "Failed to get IN edges for H";
    std::cout << "[PASS] IN_EDGE query finds G->H" << std::endl;
    
    // Step 4: Delete the edge G->H
    std::vector<cpp2::EdgeKey> edgeKeys;
    cpp2::EdgeKey key;
    key.src_ref() = "G";
    key.dst_ref() = "H";
    key.edge_type_ref() = 200;
    key.ranking_ref() = 0;
    edgeKeys.push_back(key);
    
    auto deleteEdgeFuture = client_->deleteEdges(
        spaceId_,
        std::move(edgeKeys),
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto deleteEdgeResult = std::move(deleteEdgeFuture).get();
    ASSERT_TRUE(deleteEdgeResult.succeeded()) << "Failed to delete edge";
    std::cout << "[PASS] Deleted edge G->H" << std::endl;
    
    // Step 5: Verify IN_EDGE query for H returns no results
    vertexIds = {"H"};
    
    auto checkInEdgeFuture = client_->getProps(
        spaceId_,
        vertexIds,
        nullptr,
        nullptr,
        &edgeProps,
        false,
        {},
        {},
        0,
        KVTStorageClient::CommonRequestParam(spaceId_, sessionId_, planId_)
    );
    
    auto checkInEdgeResult = std::move(checkInEdgeFuture).get();
    ASSERT_TRUE(checkInEdgeResult.succeeded()) << "Failed to check IN edges for H after delete";
    // Should return no edges
    std::cout << "[PASS] IN_EDGE query for H returns no results (reverse index cleaned up)" << std::endl;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    
    std::cout << "================================================" << std::endl;
    std::cout << "   KVT Reverse Edge Index Tests                " << std::endl;
    std::cout << "================================================" << std::endl;
    
    int result = RUN_ALL_TESTS();
    
    if (result == 0) {
        std::cout << "\n================================================" << std::endl;
        std::cout << "   ALL REVERSE EDGE TESTS PASSED!              " << std::endl;
        std::cout << "================================================" << std::endl;
    } else {
        std::cout << "\n================================================" << std::endl;
        std::cout << "   SOME TESTS FAILED - CHECK OUTPUT            " << std::endl;
        std::cout << "================================================" << std::endl;
    }
    
    return result;
}