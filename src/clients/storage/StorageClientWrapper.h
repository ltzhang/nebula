/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_STORAGECLIENTWRAPPER_H_
#define CLIENTS_STORAGE_STORAGECLIENTWRAPPER_H_

#include "common/base/Base.h"

#ifdef USE_MEMSTORE
#include "clients/storage/MemStorageClient.h"
#else
#include "clients/storage/StorageClient.h"
#endif

namespace nebula {
namespace storage {

/**
 * A wrapper that provides a unified interface for both StorageClient and MemStorageClient
 * The actual implementation is chosen at compile time based on USE_MEMSTORE flag
 */
class StorageClientWrapper {
 public:
#ifdef USE_MEMSTORE
  using ClientType = MemStorageClient;
  using CommonRequestParam = MemStorageClient::CommonRequestParam;
#else
  using ClientType = StorageClient;
  using CommonRequestParam = StorageClient::CommonRequestParam;
#endif

  StorageClientWrapper(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                       meta::MetaClient* metaClient)
      : client_(ioThreadPool, metaClient) {}

  virtual ~StorageClientWrapper() = default;

  // Vertex operations
  auto addVertices(const CommonRequestParam& param,
                   std::vector<cpp2::NewVertex> vertices,
                   std::unordered_map<TagID, std::vector<std::string>> propNames,
                   bool ifNotExists,
                   bool ignoreExistedIndex) {
    return client_.addVertices(param, std::move(vertices), propNames, ifNotExists, ignoreExistedIndex);
  }

  auto deleteVertices(const CommonRequestParam& param, std::vector<Value> ids) {
    return client_.deleteVertices(param, std::move(ids));
  }

  auto deleteTags(const CommonRequestParam& param, std::vector<cpp2::DelTags> delTags) {
    return client_.deleteTags(param, std::move(delTags));
  }

  auto updateVertex(const CommonRequestParam& param,
                    Value vertexId,
                    TagID tagId,
                    std::vector<cpp2::UpdatedProp> updatedProps,
                    bool insertable,
                    std::vector<std::string> returnProps,
                    std::string condition) {
    return client_.updateVertex(param, vertexId, tagId, std::move(updatedProps),
                               insertable, std::move(returnProps), std::move(condition));
  }

  // Edge operations
  auto addEdges(const CommonRequestParam& param,
                std::vector<cpp2::NewEdge> edges,
                std::vector<std::string> propNames,
                bool ifNotExists,
                bool ignoreExistedIndex) {
    return client_.addEdges(param, std::move(edges), std::move(propNames), ifNotExists, ignoreExistedIndex);
  }

  auto deleteEdges(const CommonRequestParam& param, std::vector<cpp2::EdgeKey> edges) {
    return client_.deleteEdges(param, std::move(edges));
  }

  auto updateEdge(const CommonRequestParam& param,
                  cpp2::EdgeKey edgeKey,
                  std::vector<cpp2::UpdatedProp> updatedProps,
                  bool insertable,
                  std::vector<std::string> returnProps,
                  std::string condition) {
    return client_.updateEdge(param, edgeKey, std::move(updatedProps),
                             insertable, std::move(returnProps), std::move(condition));
  }

  // Query operations
  auto getNeighbors(const CommonRequestParam& param,
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
                    const Expression* tagFilter = nullptr) {
    return client_.getNeighbors(param, std::move(colNames), vids, edgeTypes, edgeDirection,
                               statProps, vertexProps, edgeProps, expressions, dedup, random,
                               orderBy, limit, filter, tagFilter);
  }

  auto getDstBySrc(const CommonRequestParam& param,
                   const std::vector<Value>& vertices,
                   const std::vector<EdgeType>& edgeTypes) {
    return client_.getDstBySrc(param, vertices, edgeTypes);
  }

  auto getProps(const CommonRequestParam& param,
                const DataSet& input,
                const std::vector<cpp2::VertexProp>* vertexProps,
                const std::vector<cpp2::EdgeProp>* edgeProps,
                const std::vector<cpp2::Expr>* expressions,
                bool dedup = false,
                const std::vector<cpp2::OrderBy>& orderBy = std::vector<cpp2::OrderBy>(),
                int64_t limit = std::numeric_limits<int64_t>::max(),
                const Expression* filter = nullptr) {
    return client_.getProps(param, input, vertexProps, edgeProps, expressions,
                           dedup, orderBy, limit, filter);
  }

  // Scan operations
  auto scanVertex(const CommonRequestParam& param,
                  const std::vector<cpp2::VertexProp>& vertexProp,
                  int64_t limit,
                  const Expression* filter) {
    return client_.scanVertex(param, vertexProp, limit, filter);
  }

  auto scanEdge(const CommonRequestParam& param,
                const std::vector<cpp2::EdgeProp>& edgeProp,
                int64_t limit,
                const Expression* filter) {
    return client_.scanEdge(param, edgeProp, limit, filter);
  }

  // KV operations
  auto get(GraphSpaceID space,
           std::vector<std::string>&& keys,
           bool returnPartly = false,
           folly::EventBase* evb = nullptr) {
    return client_.get(space, std::move(keys), returnPartly, evb);
  }

  auto put(GraphSpaceID space,
           std::vector<KeyValue> kvs,
           folly::EventBase* evb = nullptr) {
    return client_.put(space, std::move(kvs), evb);
  }

  auto remove(GraphSpaceID space,
              std::vector<std::string> keys,
              folly::EventBase* evb = nullptr) {
    return client_.remove(space, std::move(keys), evb);
  }

#ifdef USE_MEMSTORE
  // Additional methods only available in MemStore mode
  auto getMemStore() { return static_cast<MemStorageClient&>(client_).memStore_; }
#endif

 private:
  ClientType client_;
};

}  // namespace storage
}  // namespace nebula

#endif  // CLIENTS_STORAGE_STORAGECLIENTWRAPPER_H_