/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/kvt/KVTValueEncoder.h"
#include "common/base/Logging.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Set.h"
#include "common/datatypes/Map.h"

namespace nebula {
namespace storage {

void KVTValueEncoder::writeString(std::string& buffer, const std::string& str) {
  uint32_t len = str.length();
  writeInt(buffer, len);
  buffer.append(str);
}

std::string KVTValueEncoder::readString(const std::string& buffer, size_t& offset) {
  uint32_t len = readInt<uint32_t>(buffer, offset);
  if (offset + len > buffer.size()) {
    throw std::out_of_range("Buffer underflow reading string");
  }
  std::string result(buffer.data() + offset, len);
  offset += len;
  return result;
}

std::string KVTValueEncoder::encodeValue(const Value& value) {
  std::string buffer;
  
  switch (value.type()) {
    case Value::Type::NULLVALUE:
      buffer.push_back(TYPE_NULL);
      break;
      
    case Value::Type::BOOL:
      buffer.push_back(TYPE_BOOL);
      buffer.push_back(value.getBool() ? 1 : 0);
      break;
      
    case Value::Type::INT:
      buffer.push_back(TYPE_INT);
      writeInt(buffer, value.getInt());
      break;
      
    case Value::Type::FLOAT:
      buffer.push_back(TYPE_FLOAT);
      writeInt(buffer, value.getFloat());
      break;
      
    case Value::Type::STRING:
      buffer.push_back(TYPE_STRING);
      writeString(buffer, value.getStr());
      break;
      
    case Value::Type::DATE: {
      buffer.push_back(TYPE_DATE);
      auto date = value.getDate();
      writeInt(buffer, date.year);
      buffer.push_back(date.month);
      buffer.push_back(date.day);
      break;
    }
    
    case Value::Type::TIME: {
      buffer.push_back(TYPE_TIME);
      auto time = value.getTime();
      buffer.push_back(time.hour);
      buffer.push_back(time.minute);
      buffer.push_back(time.sec);
      writeInt(buffer, time.microsec);
      break;
    }
    
    case Value::Type::DATETIME: {
      buffer.push_back(TYPE_DATETIME);
      auto dt = value.getDateTime();
      writeInt(buffer, dt.year);
      buffer.push_back(dt.month);
      buffer.push_back(dt.day);
      buffer.push_back(dt.hour);
      buffer.push_back(dt.minute);
      buffer.push_back(dt.sec);
      writeInt(buffer, dt.microsec);
      break;
    }
    
    case Value::Type::LIST: {
      buffer.push_back(TYPE_LIST);
      const auto& list = value.getList();
      writeInt(buffer, static_cast<uint32_t>(list.values.size()));
      for (const auto& item : list.values) {
        buffer.append(encodeValue(item));
      }
      break;
    }
    
    case Value::Type::SET: {
      buffer.push_back(TYPE_SET);
      const auto& set = value.getSet();
      writeInt(buffer, static_cast<uint32_t>(set.values.size()));
      for (const auto& item : set.values) {
        buffer.append(encodeValue(item));
      }
      break;
    }
    
    case Value::Type::MAP: {
      buffer.push_back(TYPE_MAP);
      const auto& map = value.getMap();
      writeInt(buffer, static_cast<uint32_t>(map.kvs.size()));
      for (const auto& [k, v] : map.kvs) {
        writeString(buffer, k);
        buffer.append(encodeValue(v));
      }
      break;
    }
    
    default:
      LOG(WARNING) << "Unsupported value type for encoding: " 
                   << static_cast<int>(value.type());
      buffer.push_back(TYPE_NULL);
      break;
  }
  
  return buffer;
}

Value KVTValueEncoder::decodeValue(const std::string& data, size_t& offset) {
  if (offset >= data.size()) {
    throw std::out_of_range("Buffer underflow");
  }
  
  uint8_t type = data[offset++];
  
  switch (type) {
    case TYPE_NULL:
      return Value();
      
    case TYPE_BOOL:
      return Value(data[offset++] != 0);
      
    case TYPE_INT:
      return Value(readInt<int64_t>(data, offset));
      
    case TYPE_FLOAT:
      return Value(readInt<double>(data, offset));
      
    case TYPE_STRING:
      return Value(readString(data, offset));
      
    case TYPE_DATE: {
      int16_t year = readInt<int16_t>(data, offset);
      int8_t month = data[offset++];
      int8_t day = data[offset++];
      return Value(Date(year, month, day));
    }
    
    case TYPE_TIME: {
      int8_t hour = data[offset++];
      int8_t minute = data[offset++];
      int8_t sec = data[offset++];
      int32_t microsec = readInt<int32_t>(data, offset);
      return Value(Time(hour, minute, sec, microsec));
    }
    
    case TYPE_DATETIME: {
      int16_t year = readInt<int16_t>(data, offset);
      int8_t month = data[offset++];
      int8_t day = data[offset++];
      int8_t hour = data[offset++];
      int8_t minute = data[offset++];
      int8_t sec = data[offset++];
      int32_t microsec = readInt<int32_t>(data, offset);
      return Value(DateTime(year, month, day, hour, minute, sec, microsec));
    }
    
    case TYPE_LIST: {
      uint32_t size = readInt<uint32_t>(data, offset);
      List list;
      list.reserve(size);
      for (uint32_t i = 0; i < size; i++) {
        list.emplace_back(decodeValue(data, offset));
      }
      return Value(std::move(list));
    }
    
    case TYPE_SET: {
      uint32_t size = readInt<uint32_t>(data, offset);
      Set set;
      for (uint32_t i = 0; i < size; i++) {
        set.values.emplace(decodeValue(data, offset));
      }
      return Value(std::move(set));
    }
    
    case TYPE_MAP: {
      uint32_t size = readInt<uint32_t>(data, offset);
      Map map;
      for (uint32_t i = 0; i < size; i++) {
        std::string key = readString(data, offset);
        Value val = decodeValue(data, offset);
        map.kvs.emplace(std::move(key), std::move(val));
      }
      return Value(std::move(map));
    }
    
    default:
      LOG(WARNING) << "Unknown value type: " << static_cast<int>(type);
      return Value();
  }
}

std::string KVTValueEncoder::encodeVertexProps(
    const std::unordered_map<std::string, Value>& props) {
  std::string buffer;
  
  // Write number of properties
  writeInt(buffer, static_cast<uint32_t>(props.size()));
  
  // Write each property
  for (const auto& [name, value] : props) {
    writeString(buffer, name);
    buffer.append(encodeValue(value));
  }
  
  return buffer;
}

std::string KVTValueEncoder::encodeEdgeProps(
    const std::unordered_map<std::string, Value>& props) {
  // Same format as vertex props
  return encodeVertexProps(props);
}

std::unordered_map<std::string, Value> KVTValueEncoder::decodeVertexProps(
    const std::string& encoded) {
  if (encoded.empty()) {
    return {};
  }
  
  std::unordered_map<std::string, Value> props;
  size_t offset = 0;
  
  try {
    uint32_t numProps = readInt<uint32_t>(encoded, offset);
    
    for (uint32_t i = 0; i < numProps; i++) {
      std::string name = readString(encoded, offset);
      Value value = decodeValue(encoded, offset);
      props.emplace(std::move(name), std::move(value));
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to decode vertex props: " << e.what();
  }
  
  return props;
}

std::unordered_map<std::string, Value> KVTValueEncoder::decodeEdgeProps(
    const std::string& encoded) {
  // Same format as vertex props
  return decodeVertexProps(encoded);
}

std::string KVTValueEncoder::encodeNewVertex(const cpp2::NewVertex& vertex,
                                             TagID tagId,
                                             const std::vector<std::string>& propNames) {
  std::unordered_map<std::string, Value> props;
  
  // Find the tag in the vertex
  for (const auto& tag : vertex.get_tags()) {
    if (tag.get_tag_id() == tagId) {
      const auto& values = tag.get_props();
      
      // Match property names with values
      for (size_t i = 0; i < propNames.size() && i < values.size(); i++) {
        props[propNames[i]] = values[i];
      }
      break;
    }
  }
  
  return encodeVertexProps(props);
}

std::string KVTValueEncoder::encodeNewEdge(const cpp2::NewEdge& edge,
                                           const std::vector<std::string>& propNames) {
  std::unordered_map<std::string, Value> props;
  
  const auto& values = edge.get_props();
  
  // Match property names with values
  for (size_t i = 0; i < propNames.size() && i < values.size(); i++) {
    props[propNames[i]] = values[i];
  }
  
  return encodeEdgeProps(props);
}

}  // namespace storage
}  // namespace nebula