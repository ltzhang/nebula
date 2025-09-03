/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/kvt/KVTKeyEncoder.h"
#include <sstream>
#include <vector>
#include "common/base/Logging.h"

namespace nebula {
namespace storage {

std::string KVTKeyEncoder::encodeVertexKey(GraphSpaceID spaceId,
                                           PartitionID partId,
                                           const Value& vertexId,
                                           TagID tagId) {
  std::stringstream ss;
  ss << VERTEX_PREFIX << SEPARATOR
     << spaceId << SEPARATOR
     << partId << SEPARATOR
     << escapeValue(valueToKeyString(vertexId)) << SEPARATOR
     << tagId;
  return ss.str();
}

std::string KVTKeyEncoder::encodeEdgeKey(GraphSpaceID spaceId,
                                         PartitionID partId,
                                         const Value& srcId,
                                         EdgeType edgeType,
                                         EdgeRanking ranking,
                                         const Value& dstId) {
  std::stringstream ss;
  ss << EDGE_PREFIX << SEPARATOR
     << spaceId << SEPARATOR
     << partId << SEPARATOR
     << escapeValue(valueToKeyString(srcId)) << SEPARATOR
     << edgeType << SEPARATOR
     << ranking << SEPARATOR
     << escapeValue(valueToKeyString(dstId));
  return ss.str();
}

std::string KVTKeyEncoder::encodeEdgeKey(GraphSpaceID spaceId,
                                         PartitionID partId,
                                         const cpp2::EdgeKey& edgeKey) {
  return encodeEdgeKey(spaceId,
                      partId,
                      edgeKey.get_src(),
                      edgeKey.get_edge_type(),
                      edgeKey.get_ranking(),
                      edgeKey.get_dst());
}

std::string KVTKeyEncoder::encodeReverseEdgeKey(GraphSpaceID spaceId,
                                                PartitionID partId,
                                                const Value& dstId,
                                                EdgeType edgeType,
                                                EdgeRanking ranking,
                                                const Value& srcId) {
  std::stringstream ss;
  ss << REVERSE_EDGE_PREFIX << SEPARATOR
     << spaceId << SEPARATOR
     << partId << SEPARATOR
     << escapeValue(valueToKeyString(dstId)) << SEPARATOR
     << edgeType << SEPARATOR
     << ranking << SEPARATOR
     << escapeValue(valueToKeyString(srcId));
  return ss.str();
}

std::string KVTKeyEncoder::encodeIndexKey(GraphSpaceID spaceId,
                                          IndexID indexId,
                                          const std::string& indexValue) {
  std::stringstream ss;
  ss << INDEX_PREFIX << SEPARATOR
     << spaceId << SEPARATOR
     << indexId << SEPARATOR
     << escapeValue(indexValue);
  return ss.str();
}

std::string KVTKeyEncoder::vertexPrefix(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        const Value* vertexId) {
  std::stringstream ss;
  ss << VERTEX_PREFIX << SEPARATOR
     << spaceId << SEPARATOR
     << partId << SEPARATOR;
  
  if (vertexId != nullptr) {
    ss << escapeValue(valueToKeyString(*vertexId)) << SEPARATOR;
  }
  
  return ss.str();
}

std::string KVTKeyEncoder::edgePrefix(GraphSpaceID spaceId,
                                      PartitionID partId,
                                      const Value* srcId,
                                      EdgeType edgeType) {
  std::stringstream ss;
  ss << EDGE_PREFIX << SEPARATOR
     << spaceId << SEPARATOR
     << partId << SEPARATOR;
  
  if (srcId != nullptr) {
    ss << escapeValue(valueToKeyString(*srcId)) << SEPARATOR;
    if (edgeType != 0) {
      ss << edgeType << SEPARATOR;
    }
  }
  
  return ss.str();
}

std::string KVTKeyEncoder::reverseEdgePrefix(GraphSpaceID spaceId,
                                             PartitionID partId,
                                             const Value* dstId,
                                             EdgeType edgeType) {
  std::stringstream ss;
  ss << REVERSE_EDGE_PREFIX << SEPARATOR
     << spaceId << SEPARATOR
     << partId << SEPARATOR;
  
  if (dstId != nullptr) {
    ss << escapeValue(valueToKeyString(*dstId)) << SEPARATOR;
    if (edgeType != 0) {
      ss << edgeType << SEPARATOR;
    }
  }
  
  return ss.str();
}

bool KVTKeyEncoder::decodeVertexKey(const std::string& key,
                                    GraphSpaceID& spaceId,
                                    PartitionID& partId,
                                    Value& vertexId,
                                    TagID& tagId) {
  if (key.empty() || key[0] != VERTEX_PREFIX) {
    return false;
  }
  
  std::stringstream ss(key);
  std::string token;
  std::vector<std::string> tokens;
  
  while (std::getline(ss, token, SEPARATOR)) {
    tokens.push_back(token);
  }
  
  if (tokens.size() != 5) {
    return false;
  }
  
  try {
    // tokens[0] is the prefix 'v'
    spaceId = std::stoi(tokens[1]);
    partId = std::stoi(tokens[2]);
    vertexId = keyStringToValue(unescapeValue(tokens[3]));
    tagId = std::stoi(tokens[4]);
    return true;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to decode vertex key: " << e.what();
    return false;
  }
}

bool KVTKeyEncoder::decodeEdgeKey(const std::string& key,
                                  GraphSpaceID& spaceId,
                                  PartitionID& partId,
                                  Value& srcId,
                                  EdgeType& edgeType,
                                  EdgeRanking& ranking,
                                  Value& dstId) {
  if (key.empty() || key[0] != EDGE_PREFIX) {
    return false;
  }
  
  std::stringstream ss(key);
  std::string token;
  std::vector<std::string> tokens;
  
  while (std::getline(ss, token, SEPARATOR)) {
    tokens.push_back(token);
  }
  
  if (tokens.size() != 7) {
    return false;
  }
  
  try {
    // tokens[0] is the prefix 'e'
    spaceId = std::stoi(tokens[1]);
    partId = std::stoi(tokens[2]);
    srcId = keyStringToValue(unescapeValue(tokens[3]));
    edgeType = std::stoi(tokens[4]);
    ranking = std::stoll(tokens[5]);
    dstId = keyStringToValue(unescapeValue(tokens[6]));
    return true;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to decode edge key: " << e.what();
    return false;
  }
}

bool KVTKeyEncoder::decodeReverseEdgeKey(const std::string& key,
                                         GraphSpaceID& spaceId,
                                         PartitionID& partId,
                                         Value& dstId,
                                         EdgeType& edgeType,
                                         EdgeRanking& ranking,
                                         Value& srcId) {
  if (key.empty() || key[0] != REVERSE_EDGE_PREFIX) {
    return false;
  }
  
  std::stringstream ss(key);
  std::string token;
  std::vector<std::string> tokens;
  
  while (std::getline(ss, token, SEPARATOR)) {
    tokens.push_back(token);
  }
  
  if (tokens.size() != 7) {
    return false;
  }
  
  try {
    // tokens[0] is the prefix 'r'
    spaceId = std::stoi(tokens[1]);
    partId = std::stoi(tokens[2]);
    dstId = keyStringToValue(unescapeValue(tokens[3]));
    edgeType = std::stoi(tokens[4]);
    ranking = std::stoll(tokens[5]);
    srcId = keyStringToValue(unescapeValue(tokens[6]));
    return true;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to decode reverse edge key: " << e.what();
    return false;
  }
}

std::string KVTKeyEncoder::valueToKeyString(const Value& value) {
  switch (value.type()) {
    case Value::Type::INT:
      return std::to_string(value.getInt());
    case Value::Type::STRING:
      return value.getStr();
    case Value::Type::FLOAT:
      return std::to_string(value.getFloat());
    case Value::Type::BOOL:
      return value.getBool() ? "true" : "false";
    case Value::Type::DATE:
      return value.getDate().toString();
    case Value::Type::TIME:
      return value.getTime().toString();
    case Value::Type::DATETIME:
      return value.getDateTime().toString();
    case Value::Type::VERTEX:
      // For vertex values, use the vertex ID
      return valueToKeyString(value.getVertex().vid);
    default:
      LOG(WARNING) << "Unsupported value type for key encoding: " 
                   << static_cast<int>(value.type());
      return value.toString();
  }
}

Value KVTKeyEncoder::keyStringToValue(const std::string& str) {
  // Try to parse as integer first
  try {
    size_t pos;
    int64_t intVal = std::stoll(str, &pos);
    if (pos == str.length()) {
      return Value(intVal);
    }
  } catch (...) {
    // Not an integer
  }
  
  // Try to parse as float
  try {
    size_t pos;
    double floatVal = std::stod(str, &pos);
    if (pos == str.length()) {
      return Value(floatVal);
    }
  } catch (...) {
    // Not a float
  }
  
  // Check for boolean
  if (str == "true") {
    return Value(true);
  } else if (str == "false") {
    return Value(false);
  }
  
  // Default to string
  return Value(str);
}

std::string KVTKeyEncoder::escapeValue(const std::string& value) {
  std::string result;
  result.reserve(value.length());
  
  for (char c : value) {
    if (c == SEPARATOR) {
      result += "\\:";
    } else if (c == '\\') {
      result += "\\\\";
    } else {
      result += c;
    }
  }
  
  return result;
}

std::string KVTKeyEncoder::unescapeValue(const std::string& value) {
  std::string result;
  result.reserve(value.length());
  
  bool escaped = false;
  for (char c : value) {
    if (escaped) {
      if (c == ':') {
        result += SEPARATOR;
      } else {
        result += c;
      }
      escaped = false;
    } else if (c == '\\') {
      escaped = true;
    } else {
      result += c;
    }
  }
  
  return result;
}

}  // namespace storage
}  // namespace nebula