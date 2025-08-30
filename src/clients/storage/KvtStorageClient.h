/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_KVTSTORAGECLIENT_H
#define CLIENTS_STORAGE_KVTSTORAGECLIENT_H

#include "clients/storage/StorageClientBase.h"
#include "clients/storage/KvtStore.h"
#include "common/base/Base.h"
#include "common/thrift/ThriftClientManager.h"
#include "interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace storage {

template <typename T>
using KvtStorageRpcRespFuture = folly::SemiFuture<StorageRpcResponse<T>>;

/**
 * A KVT-based storage client that replaces Thrift calls with direct KVT access
 */
class KvtStorageClient {
 public:
  struct CommonRequestParam {
    GraphSpaceID space;
    SessionID session;
    ExecutionPlanID plan;
    bool profile{false};
    bool useExperimentalFeature{false};
    folly::EventBase* evb{nullptr};

    CommonRequestParam(GraphSpaceID space_,
                       SessionID sess,
                       ExecutionPlanID plan_,
                       bool profile_ = false,
                       bool experimental = false,
                       folly::EventBase* evb_ = nullptr)
        : space(space_), session(sess), plan(plan_), profile(profile_),
          useExperimentalFeature(experimental), evb(evb_) {}

    cpp2::RequestCommon toReqCommon() const;
  };

  KvtStorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                   meta::MetaClient* metaClient)
      : ioThreadPool_(ioThreadPool), metaClient_(metaClient), kvtStore_(KvtStore::instance()) {}

  virtual ~KvtStorageClient() = default;

  // Vertex operations
  KvtStorageRpcRespFuture<cpp2::ExecResponse> addVertices(
      const CommonRequestParam& param,
      std::vector<cpp2::NewVertex> vertices,
      std::unordered_map<TagID, std::vector<std::string>> propNames,
      bool ifNotExists,
      bool ignoreExistedIndex);

  KvtStorageRpcRespFuture<cpp2::ExecResponse> deleteVertices(
      const CommonRequestParam& param,
      std::vector<Value> ids);

  KvtStorageRpcRespFuture<cpp2::ExecResponse> deleteTags(
      const CommonRequestParam& param,
      std::vector<cpp2::DelTags> delTags);

  folly::Future<StatusOr<storage::cpp2::UpdateResponse>> updateVertex(
      const CommonRequestParam& param,
      Value vertexId,
      TagID tagId,
      std::vector<cpp2::UpdatedProp> updatedProps,
      bool insertable,
      std::vector<std::string> returnProps,
      std::string condition);

  // Edge operations
  KvtStorageRpcRespFuture<cpp2::ExecResponse> addEdges(
      const CommonRequestParam& param,
      std::vector<cpp2::NewEdge> edges,
      std::vector<std::string> propNames,
      bool ifNotExists,
      bool ignoreExistedIndex);

  KvtStorageRpcRespFuture<cpp2::ExecResponse> deleteEdges(
      const CommonRequestParam& param,
      std::vector<storage::cpp2::EdgeKey> edges);

  folly::Future<StatusOr<storage::cpp2::UpdateResponse>> updateEdge(
      const CommonRequestParam& param,
      storage::cpp2::EdgeKey edgeKey,
      std::vector<cpp2::UpdatedProp> updatedProps,
      bool insertable,
      std::vector<std::string> returnProps,
      std::string condition);

  // Query operations
  KvtStorageRpcRespFuture<cpp2::GetNeighborsResponse> getNeighbors(
      const CommonRequestParam& param,
      std::vector<std::string> colNames,
      const std::vector<Value>& vids,
      const std::vector<EdgeType>& edgeTypes,
      cpp2::EdgeDirection edgeDirection,
      const std::vector<cpp2::StatProp>* statProps,
      const std::vector<cpp2::VertexProp>* vertexProps,
      const std::vector<cpp2::EdgeProp>* edgeProps,
      const std::vector<cpp2::Expr>* expressions,
      bool dedup = false,
      bool random = false,
      const std::vector<cpp2::OrderBy>& orderBy = std::vector<cpp2::OrderBy>(),
      int64_t limit = std::numeric_limits<int64_t>::max(),
      const Expression* filter = nullptr,
      const Expression* tagFilter = nullptr);

  KvtStorageRpcRespFuture<cpp2::GetDstBySrcResponse> getDstBySrc(
      const CommonRequestParam& param,
      const std::vector<Value>& vertices,
      const std::vector<EdgeType>& edgeTypes);

  KvtStorageRpcRespFuture<cpp2::GetPropResponse> getProps(
      const CommonRequestParam& param,
      const DataSet& input,
      const std::vector<cpp2::VertexProp>* vertexProps,
      const std::vector<cpp2::EdgeProp>* edgeProps,
      const std::vector<cpp2::Expr>* expressions,
      bool dedup = false,
      const std::vector<cpp2::OrderBy>& orderBy = std::vector<cpp2::OrderBy>(),
      int64_t limit = std::numeric_limits<int64_t>::max(),
      const Expression* filter = nullptr);

  // Scan operations
  KvtStorageRpcRespFuture<cpp2::ScanResponse> scanVertex(
      const CommonRequestParam& param,
      const std::vector<cpp2::VertexProp>& vertexProp,
      int64_t limit,
      const Expression* filter);

  KvtStorageRpcRespFuture<cpp2::ScanResponse> scanEdge(
      const CommonRequestParam& param,
      const std::vector<cpp2::EdgeProp>& edgeProp,
      int64_t limit,
      const Expression* filter);

  // Index operations
  KvtStorageRpcRespFuture<cpp2::LookupIndexResp> lookupIndex(
      const CommonRequestParam& param,
      const std::vector<storage::cpp2::IndexQueryContext>& indexQueryContext,
      bool isEdge,
      int32_t schemaId,
      const std::vector<std::string>& returnColumns,
      const std::vector<storage::cpp2::OrderBy>& orderBy,
      int64_t limit);

  // KV operations
  folly::SemiFuture<StorageRpcResponse<cpp2::KVGetResponse>> get(
      GraphSpaceID space,
      std::vector<std::string>&& keys,
      bool returnPartly = false,
      folly::EventBase* evb = nullptr);

  folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> put(
      GraphSpaceID space,
      std::vector<KeyValue> kvs,
      folly::EventBase* evb = nullptr);

  folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> remove(
      GraphSpaceID space,
      std::vector<std::string> keys,
      folly::EventBase* evb = nullptr);

 private:
  // Helper methods for table management
  std::string getTableName(GraphSpaceID space, const std::string& type);

  // Helper methods for key generation
  std::string generateVertexKey(GraphSpaceID space, const Value& vid, TagID tag);
  std::string generateEdgeKey(GraphSpaceID space, const Value& src, EdgeType type,
                              int64_t rank, const Value& dst);

  // Helper methods for batch operations (emulated with individual ops)
  template<typename T>
  KvtStorageRpcRespFuture<T> executeBatchOperation(
      const std::vector<std::pair<std::string, std::string>>& kvPairs,
      bool isWrite);

  // Helper methods for response creation
  template<typename T>
  KvtStorageRpcRespFuture<T> makeSuccessResponse(T response);

  template<typename T>
  KvtStorageRpcRespFuture<T> makeErrorResponse(const std::string& error);

  std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
  meta::MetaClient* metaClient_;
  KvtStore* kvtStore_;
};

}  // namespace storage
}  // namespace nebula

#endif  // CLIENTS_STORAGE_KVTSTORAGECLIENT_H