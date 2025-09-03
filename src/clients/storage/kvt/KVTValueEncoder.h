/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_KVT_KVTVALUEENCODER_H
#define CLIENTS_STORAGE_KVT_KVTVALUEENCODER_H

#include <string>
#include <vector>
#include <unordered_map>
#include "common/base/Base.h"
#include "common/datatypes/Value.h"
#include "interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace storage {

/**
 * KVTValueEncoder encodes/decodes property values for storage in KVT
 * 
 * The value format is a simple serialization of property maps
 * We use a simple format: <num_props><prop1_name_len><prop1_name><prop1_value_type><prop1_value>...
 */
class KVTValueEncoder {
 public:
  /**
   * Encode vertex properties to a value string
   * @param props Map of property names to values
   * @return Encoded value string
   */
  static std::string encodeVertexProps(const std::unordered_map<std::string, Value>& props);
  
  /**
   * Encode edge properties to a value string
   * @param props Map of property names to values
   * @return Encoded value string
   */
  static std::string encodeEdgeProps(const std::unordered_map<std::string, Value>& props);
  
  /**
   * Decode vertex properties from a value string
   * @param encoded Encoded value string
   * @return Map of property names to values
   */
  static std::unordered_map<std::string, Value> decodeVertexProps(const std::string& encoded);
  
  /**
   * Decode edge properties from a value string
   * @param encoded Encoded value string
   * @return Map of property names to values
   */
  static std::unordered_map<std::string, Value> decodeEdgeProps(const std::string& encoded);
  
  /**
   * Encode a single Value to bytes
   * @param value The value to encode
   * @return Encoded bytes
   */
  static std::string encodeValue(const Value& value);
  
  /**
   * Decode a single Value from bytes
   * @param data The data buffer
   * @param offset Current offset in the buffer (will be updated)
   * @return Decoded value
   */
  static Value decodeValue(const std::string& data, size_t& offset);
  
  /**
   * Helper to encode NewVertex properties
   * @param vertex The new vertex with tags and properties
   * @param tagId The specific tag to encode
   * @param propNames Property names for the tag
   * @return Encoded value string
   */
  static std::string encodeNewVertex(const cpp2::NewVertex& vertex, 
                                     TagID tagId,
                                     const std::vector<std::string>& propNames);
  
  /**
   * Helper to encode NewEdge properties
   * @param edge The new edge with properties
   * @param propNames Property names for the edge
   * @return Encoded value string
   */
  static std::string encodeNewEdge(const cpp2::NewEdge& edge,
                                   const std::vector<std::string>& propNames);

 private:
  // Value type markers
  static constexpr uint8_t TYPE_NULL = 0;
  static constexpr uint8_t TYPE_BOOL = 1;
  static constexpr uint8_t TYPE_INT = 2;
  static constexpr uint8_t TYPE_FLOAT = 3;
  static constexpr uint8_t TYPE_STRING = 4;
  static constexpr uint8_t TYPE_DATE = 5;
  static constexpr uint8_t TYPE_TIME = 6;
  static constexpr uint8_t TYPE_DATETIME = 7;
  static constexpr uint8_t TYPE_LIST = 8;
  static constexpr uint8_t TYPE_SET = 9;
  static constexpr uint8_t TYPE_MAP = 10;
  
  /**
   * Write a fixed-size integer to buffer
   */
  template <typename T>
  static void writeInt(std::string& buffer, T value);
  
  /**
   * Read a fixed-size integer from buffer
   */
  template <typename T>
  static T readInt(const std::string& buffer, size_t& offset);
  
  /**
   * Write a string with length prefix
   */
  static void writeString(std::string& buffer, const std::string& str);
  
  /**
   * Read a string with length prefix
   */
  static std::string readString(const std::string& buffer, size_t& offset);
};

// Template implementations
template <typename T>
void KVTValueEncoder::writeInt(std::string& buffer, T value) {
  buffer.append(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <typename T>
T KVTValueEncoder::readInt(const std::string& buffer, size_t& offset) {
  if (offset + sizeof(T) > buffer.size()) {
    throw std::out_of_range("Buffer underflow");
  }
  T value;
  memcpy(&value, buffer.data() + offset, sizeof(T));
  offset += sizeof(T);
  return value;
}

}  // namespace storage
}  // namespace nebula

#endif  // CLIENTS_STORAGE_KVT_KVTVALUEENCODER_H