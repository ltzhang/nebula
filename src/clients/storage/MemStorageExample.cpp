/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/MemStorageClient.h"
#include "clients/storage/MemStore.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include <glog/logging.h>

namespace nebula {
namespace storage {

/**
 * Example demonstrating how to use MemStorageClient instead of regular StorageClient
 * This example shows how the graph layer can work with in-memory storage
 */
class MemStorageExample {
 public:
  void runExample() {
    LOG(INFO) << "Starting MemStorage example...";
    
    // Initialize MemStorageClient (similar to how StorageClient is initialized)
    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    MemStorageClient memClient(ioThreadPool, nullptr);
    
    // Example space and session info
    GraphSpaceID spaceId = 1;
    SessionID sessionId = 12345;
    ExecutionPlanID planId = 67890;
    
    MemStorageClient::CommonRequestParam param(spaceId, sessionId, planId);
    
    // Example 1: Add vertices
    LOG(INFO) << "Adding vertices...";
    std::vector<cpp2::NewVertex> vertices;
    
    // Create a vertex with VID "player100" and tag "player"
    cpp2::NewVertex vertex;
    vertex.set_id(Value("player100"));
    
    std::vector<cpp2::NewTag> tags;
    cpp2::NewTag tag;
    tag.set_tag_id(1); // player tag
    std::vector<Value> props = {Value("Tim Duncan"), Value(42)};
    tag.set_props(props);
    tags.push_back(tag);
    vertex.set_tags(tags);
    
    vertices.push_back(vertex);
    
    std::unordered_map<TagID, std::vector<std::string>> propNames;
    propNames[1] = {"name", "age"};
    
    auto addVertexFuture = memClient.addVertices(param, std::move(vertices), propNames, false, false);
    auto addVertexResp = std::move(addVertexFuture).get();
    LOG(INFO) << "Add vertex result: " << (addVertexResp.succeeded() ? "SUCCESS" : "FAILED");
    
    // Example 2: Add edges
    LOG(INFO) << "Adding edges...";
    std::vector<cpp2::NewEdge> edges;
    
    cpp2::NewEdge edge;
    cpp2::EdgeKey edgeKey;
    edgeKey.set_src(Value("player100"));
    edgeKey.set_dst(Value("team204"));
    edgeKey.set_edge_type(101); // serve edge type
    edgeKey.set_ranking(1997);
    edge.set_key(edgeKey);
    
    std::vector<Value> edgeProps = {Value("1997-2016")};
    edge.set_props(edgeProps);
    edges.push_back(edge);
    
    std::vector<std::string> edgePropNames = {"years"};
    
    auto addEdgeFuture = memClient.addEdges(param, std::move(edges), edgePropNames, false, false);
    auto addEdgeResp = std::move(addEdgeFuture).get();
    LOG(INFO) << "Add edge result: " << (addEdgeResp.succeeded() ? "SUCCESS" : "FAILED");
    
    // Example 3: Query vertices
    LOG(INFO) << "Querying vertices...";
    DataSet inputDataSet({{"vid"}});
    inputDataSet.emplace_back(Row({Value("player100")}));
    
    std::vector<cpp2::VertexProp> vertexProps;
    cpp2::VertexProp vertexProp;
    vertexProp.set_tag(1); // player tag
    vertexProp.set_props({"name", "age"});
    vertexProps.push_back(vertexProp);
    
    auto getPropsFuture = memClient.getProps(param, inputDataSet, &vertexProps, nullptr, nullptr);
    auto getPropsResp = std::move(getPropsFuture).get();
    LOG(INFO) << "Get props result: " << (getPropsResp.succeeded() ? "SUCCESS" : "FAILED");
    
    // Example 4: Query neighbors
    LOG(INFO) << "Querying neighbors...";
    std::vector<Value> vids = {Value("player100")};
    std::vector<EdgeType> edgeTypes = {101}; // serve edge type
    std::vector<std::string> colNames = {"_vid", "_edge"};
    
    auto getNeighborsFuture = memClient.getNeighbors(param, colNames, vids, edgeTypes, 
                                                   cpp2::EdgeDirection::OUT_EDGE,
                                                   nullptr, nullptr, nullptr, nullptr);
    auto getNeighborsResp = std::move(getNeighborsFuture).get();
    LOG(INFO) << "Get neighbors result: " << (getNeighborsResp.succeeded() ? "SUCCESS" : "FAILED");
    
    // Example 5: Direct MemStore access
    LOG(INFO) << "Direct MemStore operations...";
    auto* memStore = MemStore::instance();
    LOG(INFO) << "Current MemStore size: " << memStore->size();
    
    // Dump all data for debugging
    memStore->dump();
    
    LOG(INFO) << "MemStorage example completed!";
  }
};

}  // namespace storage
}  // namespace nebula

// Standalone example main function
int main() {
  google::InitGoogleLogging("memstore_example");
  FLAGS_logtostderr = true;
  FLAGS_minloglevel = 0;
  
  nebula::storage::MemStorageExample example;
  example.runExample();
  
  return 0;
}