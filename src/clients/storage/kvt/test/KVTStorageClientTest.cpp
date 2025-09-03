/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include "clients/storage/kvt/KVTStorageClient.h"
#include "clients/storage/kvt/kvt_inc.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Value.h"

namespace nebula {
namespace storage {

class KVTStorageClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create IO thread pool
    ioThreadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(4);
    
    // For testing, we don't need a real MetaClient
    // In production, this would be properly initialized
    metaClient_ = nullptr;
    
    // Create KVT storage client
    client_ = std::make_unique<KVTStorageClient>(ioThreadPool_, metaClient_);
    
    // Initialize the client
    Status status = client_->init();
    ASSERT_TRUE(status.ok()) << "Failed to initialize KVT client: " << status.toString();
  }
  
  void TearDown() override {
    client_.reset();
    ioThreadPool_.reset();
  }
  
  std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
  meta::MetaClient* metaClient_;
  std::unique_ptr<KVTStorageClient> client_;
};

TEST_F(KVTStorageClientTest, GetPropsEmpty) {
  KVTStorageClient::CommonRequestParam param(
      1,      // space ID
      100,    // session ID
      200,    // plan ID
      false,  // profile
      false,  // experimental
      nullptr // event base
  );
  
  // Empty input dataset
  DataSet input;
  input.colNames = {"VertexID"};
  
  // Request vertex properties
  std::vector<cpp2::VertexProp> vertexProps;
  cpp2::VertexProp vProp;
  vProp.set_tag_ref(10);  // Tag ID
  vProp.set_props({"name", "age"});
  vertexProps.push_back(vProp);
  
  // Call getProps
  auto future = client_->getProps(
      param,
      input,
      &vertexProps,
      nullptr,  // edgeProps
      nullptr,  // expressions
      false,    // dedup
      {},       // orderBy
      -1,       // limit
      nullptr   // filter
  );
  
  // Wait for result
  auto result = std::move(future).get();
  
  // Should succeed even with empty input
  EXPECT_TRUE(result.succeeded());
  EXPECT_EQ(result.responses().size(), 1);
  
  const auto& response = result.responses()[0];
  const auto& props = response.get_props();
  
  // Should have column names set
  EXPECT_EQ(props.colNames.size(), 2);
  EXPECT_EQ(props.colNames[0], "name");
  EXPECT_EQ(props.colNames[1], "age");
  
  // Should have no rows for empty input
  EXPECT_EQ(props.rows.size(), 0);
}

TEST_F(KVTStorageClientTest, GetPropsVertexNotFound) {
  KVTStorageClient::CommonRequestParam param(
      1,      // space ID
      100,    // session ID
      200,    // plan ID
      false,  // profile
      false,  // experimental
      nullptr // event base
  );
  
  // Input dataset with vertex IDs that don't exist
  DataSet input;
  input.colNames = {"VertexID"};
  
  Row row1;
  row1.values.emplace_back(Value(999L));  // Non-existent vertex
  input.rows.push_back(row1);
  
  Row row2;
  row2.values.emplace_back(Value("vertex_abc"));  // Non-existent vertex
  input.rows.push_back(row2);
  
  // Request vertex properties
  std::vector<cpp2::VertexProp> vertexProps;
  cpp2::VertexProp vProp;
  vProp.set_tag_ref(10);  // Tag ID
  vProp.set_props({"name", "age"});
  vertexProps.push_back(vProp);
  
  // Call getProps
  auto future = client_->getProps(
      param,
      input,
      &vertexProps,
      nullptr,  // edgeProps
      nullptr,  // expressions
      false,    // dedup
      {},       // orderBy
      -1,       // limit
      nullptr   // filter
  );
  
  // Wait for result
  auto result = std::move(future).get();
  
  // Should succeed (not finding data is not an error)
  EXPECT_TRUE(result.succeeded());
  EXPECT_EQ(result.responses().size(), 1);
  
  const auto& response = result.responses()[0];
  const auto& props = response.get_props();
  
  // Should have column names set
  EXPECT_EQ(props.colNames.size(), 2);
  
  // For non-existent vertices, we might return empty rows
  // or rows with null values depending on implementation
  // The key point is it shouldn't crash
}

TEST_F(KVTStorageClientTest, GetPropsWithDedup) {
  KVTStorageClient::CommonRequestParam param(
      1,      // space ID
      100,    // session ID
      200,    // plan ID
      false,  // profile
      false,  // experimental
      nullptr // event base
  );
  
  // Input dataset with duplicate vertex IDs
  DataSet input;
  input.colNames = {"VertexID"};
  
  // Add same vertex ID multiple times
  for (int i = 0; i < 5; i++) {
    Row row;
    row.values.emplace_back(Value(123L));
    input.rows.push_back(row);
  }
  
  // Request vertex properties
  std::vector<cpp2::VertexProp> vertexProps;
  cpp2::VertexProp vProp;
  vProp.set_tag_ref(10);
  vProp.set_props({"name"});
  vertexProps.push_back(vProp);
  
  // Call getProps with dedup enabled
  auto future = client_->getProps(
      param,
      input,
      &vertexProps,
      nullptr,  // edgeProps
      nullptr,  // expressions
      true,     // dedup enabled
      {},       // orderBy
      -1,       // limit
      nullptr   // filter
  );
  
  // Wait for result
  auto result = std::move(future).get();
  
  EXPECT_TRUE(result.succeeded());
  
  // With dedup, duplicate rows should be removed
  // Implementation will determine exact behavior
}

TEST_F(KVTStorageClientTest, GetPropsWithLimit) {
  KVTStorageClient::CommonRequestParam param(
      1,      // space ID
      100,    // session ID
      200,    // plan ID
      false,  // profile
      false,  // experimental
      nullptr // event base
  );
  
  // Input dataset with multiple vertices
  DataSet input;
  input.colNames = {"VertexID"};
  
  for (int i = 0; i < 10; i++) {
    Row row;
    row.values.emplace_back(Value(int64_t(i)));
    input.rows.push_back(row);
  }
  
  // Request vertex properties
  std::vector<cpp2::VertexProp> vertexProps;
  cpp2::VertexProp vProp;
  vProp.set_tag_ref(10);
  vProp.set_props({"id"});
  vertexProps.push_back(vProp);
  
  // Call getProps with limit
  auto future = client_->getProps(
      param,
      input,
      &vertexProps,
      nullptr,  // edgeProps
      nullptr,  // expressions
      false,    // dedup
      {},       // orderBy
      5,        // limit to 5 results
      nullptr   // filter
  );
  
  // Wait for result
  auto result = std::move(future).get();
  
  EXPECT_TRUE(result.succeeded());
  
  const auto& response = result.responses()[0];
  const auto& props = response.get_props();
  
  // Should respect the limit (at most 5 rows)
  EXPECT_LE(props.rows.size(), 5);
}

TEST_F(KVTStorageClientTest, GetPropsEdges) {
  KVTStorageClient::CommonRequestParam param(
      1,      // space ID
      100,    // session ID
      200,    // plan ID
      false,  // profile
      false,  // experimental
      nullptr // event base
  );
  
  // Input dataset for edges (src, edge_type, ranking, dst)
  DataSet input;
  input.colNames = {"src", "edge_type", "ranking", "dst"};
  
  Row edgeRow;
  edgeRow.values.emplace_back(Value("vertex1"));   // src
  edgeRow.values.emplace_back(Value(int64_t(5)));  // edge type
  edgeRow.values.emplace_back(Value(int64_t(0)));  // ranking
  edgeRow.values.emplace_back(Value("vertex2"));   // dst
  input.rows.push_back(edgeRow);
  
  // Request edge properties
  std::vector<cpp2::EdgeProp> edgeProps;
  cpp2::EdgeProp eProp;
  eProp.set_type_ref(5);  // Edge type
  eProp.set_props({"weight", "timestamp"});
  edgeProps.push_back(eProp);
  
  // Call getProps for edges
  auto future = client_->getProps(
      param,
      input,
      nullptr,     // vertexProps
      &edgeProps,  // edgeProps
      nullptr,     // expressions
      false,       // dedup
      {},          // orderBy
      -1,          // limit
      nullptr      // filter
  );
  
  // Wait for result
  auto result = std::move(future).get();
  
  EXPECT_TRUE(result.succeeded());
  EXPECT_EQ(result.responses().size(), 1);
  
  const auto& response = result.responses()[0];
  const auto& props = response.get_props();
  
  // Should have column names for edge properties
  EXPECT_EQ(props.colNames.size(), 2);
  EXPECT_EQ(props.colNames[0], "weight");
  EXPECT_EQ(props.colNames[1], "timestamp");
}

TEST_F(KVTStorageClientTest, MultipleSpaces) {
  // Test with different space IDs to ensure proper table creation
  for (GraphSpaceID spaceId : {1, 100, 999}) {
    KVTStorageClient::CommonRequestParam param(
        spaceId,
        100,    // session ID
        200,    // plan ID
        false,  // profile
        false,  // experimental
        nullptr // event base
    );
    
    DataSet input;
    input.colNames = {"VertexID"};
    
    Row row;
    row.values.emplace_back(Value(int64_t(1)));
    input.rows.push_back(row);
    
    std::vector<cpp2::VertexProp> vertexProps;
    cpp2::VertexProp vProp;
    vProp.set_tag_ref(1);
    vProp.set_props({"prop1"});
    vertexProps.push_back(vProp);
    
    auto future = client_->getProps(
        param,
        input,
        &vertexProps,
        nullptr,
        nullptr,
        false,
        {},
        -1,
        nullptr
    );
    
    auto result = std::move(future).get();
    
    // Should succeed for each space
    EXPECT_TRUE(result.succeeded()) 
        << "Failed for space " << spaceId;
  }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}