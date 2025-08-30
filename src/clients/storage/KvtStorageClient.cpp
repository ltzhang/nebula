/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/KvtStorageClient.h"

#include <glog/logging.h>
#include <folly/json.h>

#include "common/datatypes/Value.h"
#include "common/utils/Utils.h"

namespace nebula {
namespace storage {

cpp2::RequestCommon KvtStorageClient::CommonRequestParam::toReqCommon() const {
  cpp2::RequestCommon req;
  req.session_id_ref() = session;
  req.plan_id_ref() = plan;
  req.profile_detail_ref() = profile;
  return req;
}


std::string KvtStorageClient::getTableName(GraphSpaceID space, const std::string& type) {
  return folly::sformat("space_{}_{}", space, type);
}

std::string KvtStorageClient::generateVertexKey(GraphSpaceID space, const Value& vid, TagID tag) {
  return folly::sformat("v:{}:{}:{}", space, vid.toString(), tag);
}

std::string KvtStorageClient::generateEdgeKey(GraphSpaceID space, const Value& src,
                                            EdgeType type, int64_t rank, const Value& dst) {
  return folly::sformat("e:{}:{}:{}:{}:{}", space, src.toString(), type, rank, dst.toString());
}

template<typename T>
KvtStorageRpcRespFuture<T> KvtStorageClient::makeSuccessResponse(T response) {
  StorageRpcResponse<T> rpcResp(1);
  rpcResp.addResponse(std::move(response));
  return folly::makeFuture<StorageRpcResponse<T>>(std::move(rpcResp));
}

template<typename T>
KvtStorageRpcRespFuture<T> KvtStorageClient::makeErrorResponse(const std::string& error) {
  StorageRpcResponse<T> rpcResp(1);
  LOG(ERROR) << "KvtStorageClient error: " << error;
  return folly::makeFuture<StorageRpcResponse<T>>(std::move(rpcResp));
}

KvtStorageRpcRespFuture<cpp2::ExecResponse> KvtStorageClient::addVertices(
    const CommonRequestParam& param,
    std::vector<cpp2::NewVertex> vertices,
    std::unordered_map<TagID, std::vector<std::string>> propNames,
    bool /* ifNotExists */,
    bool /* ignoreExistedIndex */) {

  LOG(INFO) << "KvtStorageClient::addVertices - Adding " << vertices.size() << " vertices";

  std::string tableName = getTableName(param.space, "vertices");
  
  auto txResult = kvtStore_->startTransaction();
  if (!txResult.ok()) {
    return makeErrorResponse<cpp2::ExecResponse>(txResult.status().toString());
  }
  uint64_t txId = txResult.value();

  for (const auto& vertex : vertices) {
    const Value& vid = vertex.get_id();
    for (const auto& tag : vertex.get_tags()) {
      TagID tagId = tag.get_tag_id();
      std::string key = generateVertexKey(param.space, vid, tagId);

      // Serialize the tag properties to JSON for simplicity
      folly::dynamic propObj = folly::dynamic::object;
      const auto& props = tag.get_props();
      for (size_t i = 0; i < props.size() && i < propNames[tagId].size(); ++i) {
        propObj[propNames[tagId][i]] = props[i].toString();
      }

      std::string value = folly::toJson(propObj);
      
      auto status = kvtStore_->put(txId, tableName, key, value);
      if (!status.ok()) {
        kvtStore_->rollbackTransaction(txId);
        return makeErrorResponse<cpp2::ExecResponse>("Failed to set vertex: " + status.toString());
      }
    }
  }

  auto commitStatus = kvtStore_->commitTransaction(txId);
  if (!commitStatus.ok()) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to commit transaction: " + commitStatus.toString());
  }

  cpp2::ExecResponse response;
  cpp2::ResponseCommon respCommon;
  respCommon.latency_in_us_ref() = 0;
  response.result_ref() = respCommon;

  return makeSuccessResponse(std::move(response));
}

KvtStorageRpcRespFuture<cpp2::ExecResponse> KvtStorageClient::addEdges(
    const CommonRequestParam& param,
    std::vector<cpp2::NewEdge> edges,
    std::vector<std::string> propNames,
    bool /* ifNotExists */,
    bool /* ignoreExistedIndex */) {

  LOG(INFO) << "KvtStorageClient::addEdges - Adding " << edges.size() << " edges";

  std::string tableName = getTableName(param.space, "edges");
  if (!ensureTable(tableName)) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to ensure table: " + tableName);
  }

  std::string error;
  uint64_t txId = kvt_start_transaction(error);
  if (txId == 0) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to start transaction: " + error);
  }

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
    
    if (!kvt_set(txId, tableName, edgeKey, value, error)) {
      kvt_rollback_transaction(txId, error);
      return makeErrorResponse<cpp2::ExecResponse>("Failed to set edge: " + error);
    }
  }

  if (!kvt_commit_transaction(txId, error)) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to commit transaction: " + error);
  }

  cpp2::ExecResponse response;
  cpp2::ResponseCommon respCommon;
  respCommon.latency_in_us_ref() = 0;
  response.result_ref() = respCommon;

  return makeSuccessResponse(std::move(response));
}

KvtStorageRpcRespFuture<cpp2::ExecResponse> KvtStorageClient::deleteVertices(
    const CommonRequestParam& param,
    std::vector<Value> ids) {

  LOG(INFO) << "KvtStorageClient::deleteVertices - Deleting " << ids.size() << " vertices";

  std::string tableName = getTableName(param.space, "vertices");
  
  std::string error;
  uint64_t txId = kvt_start_transaction(error);
  if (txId == 0) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to start transaction: " + error);
  }

  // Since we don't know the tag IDs, we'll need to scan for vertex keys matching the VID pattern
  // This is a simplified implementation - in practice, you might want to maintain an index
  for (const auto& vid : ids) {
    std::string keyPrefix = folly::sformat("v:{}:{}:", param.space, vid.toString());
    
    // Scan to find all vertex keys with this VID
    std::vector<std::pair<std::string, std::string>> results;
    std::string scanStart = keyPrefix;
    std::string scanEnd = keyPrefix + "~";  // Use ~ as it's after : in ASCII
    
    if (kvt_scan(txId, tableName, scanStart, scanEnd, 1000, results, error)) {
      for (const auto& [key, value] : results) {
        if (!kvt_del(txId, tableName, key, error)) {
          kvt_rollback_transaction(txId, error);
          return makeErrorResponse<cpp2::ExecResponse>("Failed to delete vertex key: " + error);
        }
      }
    }
  }

  if (!kvt_commit_transaction(txId, error)) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to commit transaction: " + error);
  }

  cpp2::ExecResponse response;
  cpp2::ResponseCommon respCommon;
  respCommon.latency_in_us_ref() = 0;
  response.result_ref() = respCommon;

  return makeSuccessResponse(std::move(response));
}

KvtStorageRpcRespFuture<cpp2::ExecResponse> KvtStorageClient::deleteEdges(
    const CommonRequestParam& param,
    std::vector<storage::cpp2::EdgeKey> edges) {

  LOG(INFO) << "KvtStorageClient::deleteEdges - Deleting " << edges.size() << " edges";

  std::string tableName = getTableName(param.space, "edges");
  
  std::string error;
  uint64_t txId = kvt_start_transaction(error);
  if (txId == 0) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to start transaction: " + error);
  }

  for (const auto& edgeKey : edges) {
    std::string key = generateEdgeKey(param.space, edgeKey.get_src(), edgeKey.get_edge_type(),
                                    edgeKey.get_ranking(), edgeKey.get_dst());
    
    if (!kvt_del(txId, tableName, key, error)) {
      kvt_rollback_transaction(txId, error);
      return makeErrorResponse<cpp2::ExecResponse>("Failed to delete edge: " + error);
    }
  }

  if (!kvt_commit_transaction(txId, error)) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to commit transaction: " + error);
  }

  cpp2::ExecResponse response;
  cpp2::ResponseCommon respCommon;
  respCommon.latency_in_us_ref() = 0;
  response.result_ref() = respCommon;

  return makeSuccessResponse(std::move(response));
}

// KV operations implementation
folly::SemiFuture<StorageRpcResponse<cpp2::KVGetResponse>> KvtStorageClient::get(
    GraphSpaceID space,
    std::vector<std::string>&& keys,
    bool /* returnPartly */,
    folly::EventBase* /* evb */) {

  LOG(INFO) << "KvtStorageClient::get - Getting " << keys.size() << " keys";

  std::string tableName = getTableName(space, "kv");
  if (!ensureTable(tableName)) {
    return makeErrorResponse<cpp2::KVGetResponse>("Failed to ensure table: " + tableName);
  }

  cpp2::KVGetResponse response;
  std::string error;

  for (const auto& key : keys) {
    std::string value;
    if (kvt_get(0, tableName, key, value, error)) {  // Use auto-commit for simple reads
      cpp2::KeyValue kv;
      kv.key_ref() = key;
      kv.value_ref() = value;
      response.key_values_ref()->emplace_back(std::move(kv));
    }
    // Note: Missing keys are not added to the response, which is typical behavior
  }

  cpp2::ResponseCommon respCommon;
  respCommon.latency_in_us_ref() = 0;
  response.result_ref() = respCommon;

  return makeSuccessResponse(std::move(response));
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> KvtStorageClient::put(
    GraphSpaceID space,
    std::vector<KeyValue> kvs,
    folly::EventBase* /* evb */) {

  LOG(INFO) << "KvtStorageClient::put - Putting " << kvs.size() << " key-value pairs";

  std::string tableName = getTableName(space, "kv");
  if (!ensureTable(tableName)) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to ensure table: " + tableName);
  }

  std::string error;
  uint64_t txId = kvt_start_transaction(error);
  if (txId == 0) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to start transaction: " + error);
  }

  for (const auto& kv : kvs) {
    if (!kvt_set(txId, tableName, kv.key, kv.value, error)) {
      kvt_rollback_transaction(txId, error);
      return makeErrorResponse<cpp2::ExecResponse>("Failed to set key-value: " + error);
    }
  }

  if (!kvt_commit_transaction(txId, error)) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to commit transaction: " + error);
  }

  cpp2::ExecResponse response;
  cpp2::ResponseCommon respCommon;
  respCommon.latency_in_us_ref() = 0;
  response.result_ref() = respCommon;

  return makeSuccessResponse(std::move(response));
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> KvtStorageClient::remove(
    GraphSpaceID space,
    std::vector<std::string> keys,
    folly::EventBase* /* evb */) {

  LOG(INFO) << "KvtStorageClient::remove - Removing " << keys.size() << " keys";

  std::string tableName = getTableName(space, "kv");
  
  std::string error;
  uint64_t txId = kvt_start_transaction(error);
  if (txId == 0) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to start transaction: " + error);
  }

  for (const auto& key : keys) {
    if (!kvt_del(txId, tableName, key, error)) {
      kvt_rollback_transaction(txId, error);
      return makeErrorResponse<cpp2::ExecResponse>("Failed to delete key: " + error);
    }
  }

  if (!kvt_commit_transaction(txId, error)) {
    return makeErrorResponse<cpp2::ExecResponse>("Failed to commit transaction: " + error);
  }

  cpp2::ExecResponse response;
  cpp2::ResponseCommon respCommon;
  respCommon.latency_in_us_ref() = 0;
  response.result_ref() = respCommon;

  return makeSuccessResponse(std::move(response));
}

// Stub implementations for complex operations - these would require more detailed implementation
// based on the specific requirements of NebulaGraph's query engine

KvtStorageRpcRespFuture<cpp2::ExecResponse> KvtStorageClient::deleteTags(
    const CommonRequestParam& /* param */,
    std::vector<cpp2::DelTags> /* delTags */) {
  LOG(WARNING) << "KvtStorageClient::deleteTags - Not implemented";
  return makeErrorResponse<cpp2::ExecResponse>("deleteTags not implemented");
}

folly::Future<StatusOr<storage::cpp2::UpdateResponse>> KvtStorageClient::updateVertex(
    const CommonRequestParam& /* param */,
    Value /* vertexId */,
    TagID /* tagId */,
    std::vector<cpp2::UpdatedProp> /* updatedProps */,
    bool /* insertable */,
    std::vector<std::string> /* returnProps */,
    std::string /* condition */) {
  LOG(WARNING) << "KvtStorageClient::updateVertex - Not implemented";
  return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(
      Status::Error("updateVertex not implemented"));
}

folly::Future<StatusOr<storage::cpp2::UpdateResponse>> KvtStorageClient::updateEdge(
    const CommonRequestParam& /* param */,
    storage::cpp2::EdgeKey /* edgeKey */,
    std::vector<cpp2::UpdatedProp> /* updatedProps */,
    bool /* insertable */,
    std::vector<std::string> /* returnProps */,
    std::string /* condition */) {
  LOG(WARNING) << "KvtStorageClient::updateEdge - Not implemented";
  return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(
      Status::Error("updateEdge not implemented"));
}

KvtStorageRpcRespFuture<cpp2::GetNeighborsResponse> KvtStorageClient::getNeighbors(
    const CommonRequestParam& /* param */,
    std::vector<std::string> /* colNames */,
    const std::vector<Value>& /* vids */,
    const std::vector<EdgeType>& /* edgeTypes */,
    cpp2::EdgeDirection /* edgeDirection */,
    const std::vector<cpp2::StatProp>* /* statProps */,
    const std::vector<cpp2::VertexProp>* /* vertexProps */,
    const std::vector<cpp2::EdgeProp>* /* edgeProps */,
    const std::vector<cpp2::Expr>* /* expressions */,
    bool /* dedup */,
    bool /* random */,
    const std::vector<cpp2::OrderBy>& /* orderBy */,
    int64_t /* limit */,
    const Expression* /* filter */,
    const Expression* /* tagFilter */) {
  LOG(WARNING) << "KvtStorageClient::getNeighbors - Not implemented";
  return makeErrorResponse<cpp2::GetNeighborsResponse>("getNeighbors not implemented");
}

KvtStorageRpcRespFuture<cpp2::GetDstBySrcResponse> KvtStorageClient::getDstBySrc(
    const CommonRequestParam& /* param */,
    const std::vector<Value>& /* vertices */,
    const std::vector<EdgeType>& /* edgeTypes */) {
  LOG(WARNING) << "KvtStorageClient::getDstBySrc - Not implemented";
  return makeErrorResponse<cpp2::GetDstBySrcResponse>("getDstBySrc not implemented");
}

KvtStorageRpcRespFuture<cpp2::GetPropResponse> KvtStorageClient::getProps(
    const CommonRequestParam& /* param */,
    const DataSet& /* input */,
    const std::vector<cpp2::VertexProp>* /* vertexProps */,
    const std::vector<cpp2::EdgeProp>* /* edgeProps */,
    const std::vector<cpp2::Expr>* /* expressions */,
    bool /* dedup */,
    const std::vector<cpp2::OrderBy>& /* orderBy */,
    int64_t /* limit */,
    const Expression* /* filter */) {
  LOG(WARNING) << "KvtStorageClient::getProps - Not implemented";
  return makeErrorResponse<cpp2::GetPropResponse>("getProps not implemented");
}

KvtStorageRpcRespFuture<cpp2::ScanResponse> KvtStorageClient::scanVertex(
    const CommonRequestParam& /* param */,
    const std::vector<cpp2::VertexProp>& /* vertexProp */,
    int64_t /* limit */,
    const Expression* /* filter */) {
  LOG(WARNING) << "KvtStorageClient::scanVertex - Not implemented";
  return makeErrorResponse<cpp2::ScanResponse>("scanVertex not implemented");
}

KvtStorageRpcRespFuture<cpp2::ScanResponse> KvtStorageClient::scanEdge(
    const CommonRequestParam& /* param */,
    const std::vector<cpp2::EdgeProp>& /* edgeProp */,
    int64_t /* limit */,
    const Expression* /* filter */) {
  LOG(WARNING) << "KvtStorageClient::scanEdge - Not implemented";
  return makeErrorResponse<cpp2::ScanResponse>("scanEdge not implemented");
}

KvtStorageRpcRespFuture<cpp2::LookupIndexResp> KvtStorageClient::lookupIndex(
    const CommonRequestParam& /* param */,
    const std::vector<storage::cpp2::IndexQueryContext>& /* indexQueryContext */,
    bool /* isEdge */,
    int32_t /* schemaId */,
    const std::vector<std::string>& /* returnColumns */,
    const std::vector<storage::cpp2::OrderBy>& /* orderBy */,
    int64_t /* limit */) {
  LOG(WARNING) << "KvtStorageClient::lookupIndex - Not implemented";
  return makeErrorResponse<cpp2::LookupIndexResp>("lookupIndex not implemented");
}

}  // namespace storage
}  // namespace nebula