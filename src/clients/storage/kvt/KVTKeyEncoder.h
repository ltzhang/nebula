/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_KVT_KVTKEYENCODER_H
#define CLIENTS_STORAGE_KVT_KVTKEYENCODER_H

#include <string>
#include <sstream>
#include "common/base/Base.h"
#include "common/datatypes/Value.h"
#include "interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace storage {

/**
 * KVTKeyEncoder encodes NebulaGraph's data model into KVT keys
 * 
 * Key format:
 * - Vertex: "v:<spaceId>:<partId>:<vertexId>:<tagId>"
 * - Edge: "e:<spaceId>:<partId>:<srcId>:<edgeType>:<ranking>:<dstId>"
 * - Index: "i:<spaceId>:<indexId>:<indexValue>"
 */
class KVTKeyEncoder {
 public:
  static constexpr char VERTEX_PREFIX = 'v';
  static constexpr char EDGE_PREFIX = 'e';
  static constexpr char INDEX_PREFIX = 'i';
  static constexpr char REVERSE_EDGE_PREFIX = 'r';  // For reverse edge index
  static constexpr char SEPARATOR = ':';

  /**
   * Encode a vertex key
   * @param spaceId Graph space ID
   * @param partId Partition ID
   * @param vertexId Vertex ID (as Value)
   * @param tagId Tag ID
   * @return Encoded key string
   */
  static std::string encodeVertexKey(GraphSpaceID spaceId,
                                     PartitionID partId,
                                     const Value& vertexId,
                                     TagID tagId);

  /**
   * Encode an edge key
   * @param spaceId Graph space ID
   * @param partId Partition ID
   * @param srcId Source vertex ID
   * @param edgeType Edge type
   * @param ranking Edge ranking
   * @param dstId Destination vertex ID
   * @return Encoded key string
   */
  static std::string encodeEdgeKey(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   const Value& srcId,
                                   EdgeType edgeType,
                                   EdgeRanking ranking,
                                   const Value& dstId);

  /**
   * Encode an edge key from cpp2::EdgeKey
   */
  static std::string encodeEdgeKey(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   const cpp2::EdgeKey& edgeKey);

  /**
   * Encode a reverse edge index key
   * This allows efficient lookup of incoming edges to a destination vertex
   * @param spaceId Graph space ID
   * @param partId Partition ID
   * @param dstId Destination vertex ID
   * @param edgeType Edge type
   * @param ranking Edge ranking
   * @param srcId Source vertex ID
   * @return Encoded reverse edge index key
   */
  static std::string encodeReverseEdgeKey(GraphSpaceID spaceId,
                                          PartitionID partId,
                                          const Value& dstId,
                                          EdgeType edgeType,
                                          EdgeRanking ranking,
                                          const Value& srcId);

  /**
   * Encode an index key
   * @param spaceId Graph space ID
   * @param indexId Index ID
   * @param indexValue Index value
   * @return Encoded key string
   */
  static std::string encodeIndexKey(GraphSpaceID spaceId,
                                    IndexID indexId,
                                    const std::string& indexValue);

  /**
   * Create a prefix for scanning vertices
   * @param spaceId Graph space ID
   * @param partId Partition ID
   * @param vertexId Vertex ID (optional, if empty scan all vertices in partition)
   * @return Key prefix for scanning
   */
  static std::string vertexPrefix(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  const Value* vertexId = nullptr);

  /**
   * Create a prefix for scanning edges
   * @param spaceId Graph space ID
   * @param partId Partition ID
   * @param srcId Source vertex ID (optional)
   * @param edgeType Edge type (optional, 0 means all types)
   * @return Key prefix for scanning
   */
  static std::string edgePrefix(GraphSpaceID spaceId,
                                PartitionID partId,
                                const Value* srcId = nullptr,
                                EdgeType edgeType = 0);

  /**
   * Create a prefix for scanning reverse edges (incoming edges to a vertex)
   * @param spaceId Graph space ID
   * @param partId Partition ID
   * @param dstId Destination vertex ID (optional)
   * @param edgeType Edge type (optional, 0 means all types)
   * @return Key prefix for scanning reverse edges
   */
  static std::string reverseEdgePrefix(GraphSpaceID spaceId,
                                       PartitionID partId,
                                       const Value* dstId = nullptr,
                                       EdgeType edgeType = 0);

  /**
   * Decode a vertex key
   * @param key Encoded key
   * @param spaceId Output space ID
   * @param partId Output partition ID
   * @param vertexId Output vertex ID
   * @param tagId Output tag ID
   * @return true if decode successful
   */
  static bool decodeVertexKey(const std::string& key,
                              GraphSpaceID& spaceId,
                              PartitionID& partId,
                              Value& vertexId,
                              TagID& tagId);

  /**
   * Decode an edge key
   * @param key Encoded key
   * @param spaceId Output space ID
   * @param partId Output partition ID
   * @param srcId Output source vertex ID
   * @param edgeType Output edge type
   * @param ranking Output edge ranking
   * @param dstId Output destination vertex ID
   * @return true if decode successful
   */
  static bool decodeEdgeKey(const std::string& key,
                            GraphSpaceID& spaceId,
                            PartitionID& partId,
                            Value& srcId,
                            EdgeType& edgeType,
                            EdgeRanking& ranking,
                            Value& dstId);

  /**
   * Decode a reverse edge key
   * @param key Encoded reverse edge key
   * @param spaceId Output space ID
   * @param partId Output partition ID
   * @param dstId Output destination vertex ID
   * @param edgeType Output edge type
   * @param ranking Output edge ranking
   * @param srcId Output source vertex ID
   * @return true if decode successful
   */
  static bool decodeReverseEdgeKey(const std::string& key,
                                   GraphSpaceID& spaceId,
                                   PartitionID& partId,
                                   Value& dstId,
                                   EdgeType& edgeType,
                                   EdgeRanking& ranking,
                                   Value& srcId);

  /**
   * Helper to convert Value to string for key encoding
   */
  static std::string valueToKeyString(const Value& value);

  /**
   * Helper to parse Value from key string
   */
  static Value keyStringToValue(const std::string& str);

 private:
  /**
   * Escape special characters in value strings
   */
  static std::string escapeValue(const std::string& value);

  /**
   * Unescape special characters in value strings
   */
  static std::string unescapeValue(const std::string& value);
};

}  // namespace storage
}  // namespace nebula

#endif  // CLIENTS_STORAGE_KVT_KVTKEYENCODER_H