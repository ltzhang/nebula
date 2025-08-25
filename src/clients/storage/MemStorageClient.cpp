/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/MemStorageClient.h"

#include <glog/logging.h>
#include <folly/json.h>

#include "common/datatypes/Value.h"
#include "common/utils/Utils.h"

namespace nebula {
namespace storage {

cpp2::RequestCommon MemStorageClient::CommonRequestParam::toReqCommon() const {
  cpp2::RequestCommon req;
  req.set_space_id(space);
  req.set_session_id(session);
  req.set_plan_id(plan);
  req.set_profile(profile);
  return req;
}

std::string MemStorageClient::generateVertexKey(GraphSpaceID space, const Value& vid, TagID tag) {
  return folly::sformat("v:{}:{}:{}", space, vid.toString(), tag);
}

std::string MemStorageClient::generateEdgeKey(GraphSpaceID space, const Value& src, 
                                            EdgeType type, int64_t rank, const Value& dst) {
  return folly::sformat("e:{}:{}:{}:{}:{}", space, src.toString(), type, rank, dst.toString());
}

template<typename T>
MemStorageRpcRespFuture<T> MemStorageClient::makeSuccessResponse(T response) {
  StorageRpcResponse<T> rpcResp(1);
  rpcResp.addResponse(std::move(response));
  rpcResp.markSinglePartSuccess();
  return folly::makeFuture<StorageRpcResponse<T>>(std::move(rpcResp));
}

template<typename T>
MemStorageRpcRespFuture<T> MemStorageClient::makeErrorResponse(const std::string& error) {
  StorageRpcResponse<T> rpcResp(1);
  // Add failed response - implementation would depend on the specific error handling
  LOG(ERROR) << "MemStorageClient error: " << error;
  return folly::makeFuture<StorageRpcResponse<T>>(std::move(rpcResp));
}

MemStorageRpcRespFuture<cpp2::ExecResponse> MemStorageClient::addVertices(
    const CommonRequestParam& param,
    std::vector<cpp2::NewVertex> vertices,
    std::unordered_map<TagID, std::vector<std::string>> propNames,
    bool ifNotExists,
    bool ignoreExistedIndex) {
  
  LOG(INFO) << "MemStorageClient::addVertices - Adding " << vertices.size() << " vertices";
  
  cpp2::ExecResponse response;
  std::vector<std::pair<std::string, std::string>> kvPairs;
  
  for (const auto& vertex : vertices) {
    const Value& vid = vertex.get_id();
    for (const auto& tag : vertex.get_tags()) {
      TagID tagId = tag.get_tag_id();
      std::string key = generateVertexKey(param.space, vid, tagId);
      
      // Serialize the tag properties to JSON for simplicity
      folly::dynamic propObj = folly::dynamic::object;
      const auto& props = tag.get_props();
      for (size_t i = 0; i < props.size() && i < propNames[tagId].size(); ++i) {
        // Simple string serialization - in reality you'd want proper value serialization
        propObj[propNames[tagId][i]] = props[i].toString();
      }
      
      std::string value = folly::toJson(propObj);
      kvPairs.emplace_back(key, value);
    }
  }
  
  auto status = memStore_->batchPut(kvPairs);
  if (!status.ok()) {
    return makeErrorResponse<cpp2::ExecResponse>(status.toString());
  }
  
  cpp2::ResponseCommon respCommon;
  respCommon.set_latency_in_us(0); // For simplicity
  response.set_result(respCommon);
  
  return makeSuccessResponse(std::move(response));
}

MemStorageRpcRespFuture<cpp2::ExecResponse> MemStorageClient::addEdges(
    const CommonRequestParam& param,
    std::vector<cpp2::NewEdge> edges,
    std::vector<std::string> propNames,
    bool ifNotExists,
    bool ignoreExistedIndex) {
  
  LOG(INFO) << "MemStorageClient::addEdges - Adding " << edges.size() << " edges";
  
  cpp2::ExecResponse response;
  std::vector<std::pair<std::string, std::string>> kvPairs;
  
  for (const auto& edge : edges) {
    const auto& key = edge.get_key();
    std::string edgeKey = generateEdgeKey(param.space, key.get_src(), key.get_edge_type(),
                                        key.get_ranking(), key.get_dst());
    
    // Serialize edge properties to JSON
    folly::dynamic propObj = folly::dynamic::object;
    const auto& props = edge.get_props();
    for (size_t i = 0; i < props.size() && i < propNames.size(); ++i) {
      propObj[propNames[i]] = props[i].toString();
    }
    
    std::string value = folly::toJson(propObj);
    kvPairs.emplace_back(edgeKey, value);
  }
  
  auto status = memStore_->batchPut(kvPairs);
  if (!status.ok()) {
    return makeErrorResponse<cpp2::ExecResponse>(status.toString());
  }
  
  cpp2::ResponseCommon respCommon;
  respCommon.set_latency_in_us(0);
  response.set_result(respCommon);
  
  return makeSuccessResponse(std::move(response));
}

MemStorageRpcRespFuture<cpp2::GetPropResponse> MemStorageClient::getProps(
    const CommonRequestParam& param,
    const DataSet& input,
    const std::vector<cpp2::VertexProp>* vertexProps,
    const std::vector<cpp2::EdgeProp>* edgeProps,
    const std::vector<cpp2::Expr>* expressions,
    bool dedup,
    const std::vector<cpp2::OrderBy>& orderBy,
    int64_t limit,
    const Expression* filter) {
  
  LOG(INFO) << "MemStorageClient::getProps - Getting properties for " 
            << input.rows.size() << " items";
  
  cpp2::GetPropResponse response;
  DataSet resultDataSet;
  
  // Simplified implementation - just return the input for now
  // In a real implementation, you would:
  // 1. Parse the input DataSet to extract vertex/edge keys
  // 2. Query the MemStore for each key
  // 3. Build the response DataSet with the requested properties
  
  for (const auto& row : input.rows) {
    // For vertices
    if (vertexProps != nullptr && !vertexProps->empty()) {
      for (const auto& vertexProp : *vertexProps) {
        TagID tagId = vertexProp.get_tag();
        if (!row.values.empty()) {
          const Value& vid = row.values[0]; // Assuming first column is VID
          std::string key = generateVertexKey(param.space, vid, tagId);
          
          auto result = memStore_->get(key);
          if (result.ok()) {
            // Parse JSON and add to result dataset
            // Simplified - just add the raw value for now
            Row resultRow;
            resultRow.values.push_back(vid);
            resultRow.values.push_back(Value(result.value()));
            resultDataSet.rows.push_back(std::move(resultRow));
          }
        }
      }
    }
    
    // For edges - similar logic
    if (edgeProps != nullptr && !edgeProps->empty()) {
      // Implementation would extract src, dst, type, rank from input
      // and query MemStore accordingly
    }
  }
  
  response.set_props(std::move(resultDataSet));
  
  cpp2::ResponseCommon respCommon;
  respCommon.set_latency_in_us(0);
  response.set_result(respCommon);
  
  return makeSuccessResponse(std::move(response));
}

MemStorageRpcRespFuture<cpp2::GetNeighborsResponse> MemStorageClient::getNeighbors(
    const CommonRequestParam& param,
    std::vector<std::string> colNames,
    const std::vector<Value>& vids,
    const std::vector<EdgeType>& edgeTypes,
    cpp2::EdgeDirection edgeDirection,
    const std::vector<cpp2::StatProp>* statProps,
    const std::vector<cpp2::VertexProp>* vertexProps,
    const std::vector<cpp2::EdgeProp>* edgeProps,
    const std::vector<cpp2::Expr>* expressions,
    bool dedup,
    bool random,
    const std::vector<cpp2::OrderBy>& orderBy,
    int64_t limit,
    const Expression* filter,
    const Expression* tagFilter) {
  
  LOG(INFO) << "MemStorageClient::getNeighbors - Getting neighbors for " 
            << vids.size() << " vertices";
  
  cpp2::GetNeighborsResponse response;
  DataSet resultDataSet;
  
  // Simplified implementation
  // In a real implementation, you would:
  // 1. Scan through all edges in MemStore with prefix "e:{space}:"
  // 2. Filter edges by source VID and edge types
  // 3. Build the neighbor response
  
  auto cursor = memStore_->createScanCursor(folly::sformat("e:{}:", param.space));
  if (cursor.ok() && cursor.value()->isValid()) {
    while (memStore_->hasNext(cursor.value().get())) {
      auto kvResult = memStore_->scanNext(cursor.value().get());
      if (kvResult.ok()) {
        // Parse edge key and check if it matches our criteria
        // This is a simplified version
        Row row;
        row.values.push_back(Value(kvResult.value().first));
        row.values.push_back(Value(kvResult.value().second));
        resultDataSet.rows.push_back(std::move(row));
        
        if (resultDataSet.rows.size() >= static_cast<size_t>(limit)) {
          break;
        }
      }
    }
  }
  
  response.set_vertices(std::move(resultDataSet));
  
  cpp2::ResponseCommon respCommon;
  respCommon.set_latency_in_us(0);
  response.set_result(respCommon);
  
  return makeSuccessResponse(std::move(response));
}

// Implement other methods with similar patterns...
MemStorageRpcRespFuture<cpp2::ExecResponse> MemStorageClient::deleteVertices(
    const CommonRequestParam& param,
    std::vector<Value> ids) {
  LOG(INFO) << "MemStorageClient::deleteVertices - Deleting " << ids.size() << " vertices";
  
  // Simplified - would need to find all vertex keys for each VID
  cpp2::ExecResponse response;
  cpp2::ResponseCommon respCommon;
  respCommon.set_latency_in_us(0);
  response.set_result(respCommon);
  
  return makeSuccessResponse(std::move(response));
}

MemStorageRpcRespFuture<cpp2::ExecResponse> MemStorageClient::deleteEdges(
    const CommonRequestParam& param,
    std::vector<storage::cpp2::EdgeKey> edges) {
  LOG(INFO) << "MemStorageClient::deleteEdges - Deleting " << edges.size() << " edges";
  
  std::vector<std::string> keys;
  for (const auto& edge : edges) {
    std::string key = generateEdgeKey(param.space, edge.get_src(), edge.get_edge_type(),
                                    edge.get_ranking(), edge.get_dst());
    keys.push_back(key);
  }
  
  auto status = memStore_->batchRemove(keys);
  
  cpp2::ExecResponse response;
  cpp2::ResponseCommon respCommon;
  respCommon.set_latency_in_us(0);
  response.set_result(respCommon);
  
  return makeSuccessResponse(std::move(response));
}

// KV operations
folly::SemiFuture<StorageRpcResponse<cpp2::KVGetResponse>> MemStorageClient::get(
    GraphSpaceID space,
    std::vector<std::string>&& keys,
    bool returnPartly,
    folly::EventBase* evb) {
  
  cpp2::KVGetResponse response;
  std::vector<std::string> values;
  
  for (const auto& key : keys) {
    auto result = memStore_->get(key);
    if (result.ok()) {
      values.push_back(result.value());
    } else {
      values.push_back(""); // Empty value for not found
    }
  }
  
  response.set_key_values(std::move(values));
  
  return makeSuccessResponse(std::move(response));
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> MemStorageClient::put(
    GraphSpaceID space,
    std::vector<KeyValue> kvs,
    folly::EventBase* evb) {
  
  std::vector<std::pair<std::string, std::string>> memKvs;
  for (const auto& kv : kvs) {
    memKvs.emplace_back(kv.key, kv.value);
  }
  
  auto status = memStore_->batchPut(memKvs);
  
  cpp2::ExecResponse response;
  cpp2::ResponseCommon respCommon;
  respCommon.set_latency_in_us(0);
  response.set_result(respCommon);
  
  return makeSuccessResponse(std::move(response));
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> MemStorageClient::remove(
    GraphSpaceID space,
    std::vector<std::string> keys,
    folly::EventBase* evb) {
  
  auto status = memStore_->batchRemove(keys);
  
  cpp2::ExecResponse response;
  cpp2::ResponseCommon respCommon;
  respCommon.set_latency_in_us(0);
  response.set_result(respCommon);
  
  return makeSuccessResponse(std::move(response));
}

// Stub implementations for remaining methods
MemStorageRpcRespFuture<cpp2::ExecResponse> MemStorageClient::deleteTags(
    const CommonRequestParam& param,
    std::vector<cpp2::DelTags> delTags) {
  cpp2::ExecResponse response;
  cpp2::ResponseCommon respCommon;
  response.set_result(respCommon);
  return makeSuccessResponse(std::move(response));
}

folly::Future<StatusOr<storage::cpp2::UpdateResponse>> MemStorageClient::updateVertex(
    const CommonRequestParam& param,
    Value vertexId,
    TagID tagId,
    std::vector<cpp2::UpdatedProp> updatedProps,
    bool insertable,
    std::vector<std::string> returnProps,
    std::string condition) {
  storage::cpp2::UpdateResponse response;
  return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(std::move(response));
}

folly::Future<StatusOr<storage::cpp2::UpdateResponse>> MemStorageClient::updateEdge(
    const CommonRequestParam& param,
    storage::cpp2::EdgeKey edgeKey,
    std::vector<cpp2::UpdatedProp> updatedProps,
    bool insertable,
    std::vector<std::string> returnProps,
    std::string condition) {
  storage::cpp2::UpdateResponse response;
  return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(std::move(response));
}

MemStorageRpcRespFuture<cpp2::GetDstBySrcResponse> MemStorageClient::getDstBySrc(
    const CommonRequestParam& param,
    const std::vector<Value>& vertices,
    const std::vector<EdgeType>& edgeTypes) {
  cpp2::GetDstBySrcResponse response;
  return makeSuccessResponse(std::move(response));
}

MemStorageRpcRespFuture<cpp2::ScanResponse> MemStorageClient::scanVertex(
    const CommonRequestParam& param,
    const std::vector<cpp2::VertexProp>& vertexProp,
    int64_t limit,
    const Expression* filter) {
  cpp2::ScanResponse response;
  return makeSuccessResponse(std::move(response));
}

MemStorageRpcRespFuture<cpp2::ScanResponse> MemStorageClient::scanEdge(
    const CommonRequestParam& param,
    const std::vector<cpp2::EdgeProp>& edgeProp,
    int64_t limit,
    const Expression* filter) {
  cpp2::ScanResponse response;
  return makeSuccessResponse(std::move(response));
}

}  // namespace storage
}  // namespace nebula