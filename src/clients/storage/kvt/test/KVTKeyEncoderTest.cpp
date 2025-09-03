/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include "clients/storage/kvt/KVTKeyEncoder.h"
#include "common/datatypes/Value.h"

namespace nebula {
namespace storage {

TEST(KVTKeyEncoderTest, EncodeDecodeVertexKey) {
  GraphSpaceID spaceId = 100;
  PartitionID partId = 5;
  Value vertexId(123456L);
  TagID tagId = 10;
  
  // Encode
  std::string key = KVTKeyEncoder::encodeVertexKey(spaceId, partId, vertexId, tagId);
  
  // Verify format
  EXPECT_EQ(key[0], KVTKeyEncoder::VERTEX_PREFIX);
  EXPECT_NE(key.find(":100:5:"), std::string::npos);
  
  // Decode
  GraphSpaceID decodedSpaceId;
  PartitionID decodedPartId;
  Value decodedVertexId;
  TagID decodedTagId;
  
  bool success = KVTKeyEncoder::decodeVertexKey(
      key, decodedSpaceId, decodedPartId, decodedVertexId, decodedTagId);
  
  EXPECT_TRUE(success);
  EXPECT_EQ(spaceId, decodedSpaceId);
  EXPECT_EQ(partId, decodedPartId);
  EXPECT_EQ(vertexId, decodedVertexId);
  EXPECT_EQ(tagId, decodedTagId);
}

TEST(KVTKeyEncoderTest, EncodeDecodeEdgeKey) {
  GraphSpaceID spaceId = 200;
  PartitionID partId = 10;
  Value srcId("source_vertex");
  EdgeType edgeType = 15;
  EdgeRanking ranking = 100;
  Value dstId("dest_vertex");
  
  // Encode
  std::string key = KVTKeyEncoder::encodeEdgeKey(
      spaceId, partId, srcId, edgeType, ranking, dstId);
  
  // Verify format
  EXPECT_EQ(key[0], KVTKeyEncoder::EDGE_PREFIX);
  EXPECT_NE(key.find(":200:10:"), std::string::npos);
  
  // Decode
  GraphSpaceID decodedSpaceId;
  PartitionID decodedPartId;
  Value decodedSrcId;
  EdgeType decodedEdgeType;
  EdgeRanking decodedRanking;
  Value decodedDstId;
  
  bool success = KVTKeyEncoder::decodeEdgeKey(
      key, decodedSpaceId, decodedPartId, decodedSrcId, 
      decodedEdgeType, decodedRanking, decodedDstId);
  
  EXPECT_TRUE(success);
  EXPECT_EQ(spaceId, decodedSpaceId);
  EXPECT_EQ(partId, decodedPartId);
  EXPECT_EQ(srcId, decodedSrcId);
  EXPECT_EQ(edgeType, decodedEdgeType);
  EXPECT_EQ(ranking, decodedRanking);
  EXPECT_EQ(dstId, decodedDstId);
}

TEST(KVTKeyEncoderTest, VertexPrefix) {
  GraphSpaceID spaceId = 300;
  PartitionID partId = 15;
  
  // Without vertex ID
  std::string prefix1 = KVTKeyEncoder::vertexPrefix(spaceId, partId);
  EXPECT_EQ(prefix1, "v:300:15:");
  
  // With vertex ID
  Value vertexId(999L);
  std::string prefix2 = KVTKeyEncoder::vertexPrefix(spaceId, partId, &vertexId);
  EXPECT_EQ(prefix2, "v:300:15:999:");
}

TEST(KVTKeyEncoderTest, EdgePrefix) {
  GraphSpaceID spaceId = 400;
  PartitionID partId = 20;
  
  // Without source ID
  std::string prefix1 = KVTKeyEncoder::edgePrefix(spaceId, partId);
  EXPECT_EQ(prefix1, "e:400:20:");
  
  // With source ID but no edge type
  Value srcId("vertex123");
  std::string prefix2 = KVTKeyEncoder::edgePrefix(spaceId, partId, &srcId);
  EXPECT_EQ(prefix2, "e:400:20:vertex123:");
  
  // With source ID and edge type
  EdgeType edgeType = 25;
  std::string prefix3 = KVTKeyEncoder::edgePrefix(spaceId, partId, &srcId, edgeType);
  EXPECT_EQ(prefix3, "e:400:20:vertex123:25:");
}

TEST(KVTKeyEncoderTest, ValueToKeyString) {
  // Test integer
  Value intVal(42L);
  EXPECT_EQ(KVTKeyEncoder::valueToKeyString(intVal), "42");
  
  // Test string
  Value strVal("hello");
  EXPECT_EQ(KVTKeyEncoder::valueToKeyString(strVal), "hello");
  
  // Test float
  Value floatVal(3.14);
  std::string floatStr = KVTKeyEncoder::valueToKeyString(floatVal);
  EXPECT_NE(floatStr.find("3.14"), std::string::npos);
  
  // Test boolean
  Value boolVal(true);
  EXPECT_EQ(KVTKeyEncoder::valueToKeyString(boolVal), "true");
}

TEST(KVTKeyEncoderTest, KeyStringToValue) {
  // Test integer parsing
  Value intVal = KVTKeyEncoder::keyStringToValue("123");
  EXPECT_EQ(intVal.type(), Value::Type::INT);
  EXPECT_EQ(intVal.getInt(), 123);
  
  // Test float parsing
  Value floatVal = KVTKeyEncoder::keyStringToValue("3.14");
  EXPECT_EQ(floatVal.type(), Value::Type::FLOAT);
  EXPECT_DOUBLE_EQ(floatVal.getFloat(), 3.14);
  
  // Test boolean parsing
  Value boolVal = KVTKeyEncoder::keyStringToValue("true");
  EXPECT_EQ(boolVal.type(), Value::Type::BOOL);
  EXPECT_TRUE(boolVal.getBool());
  
  // Test string parsing
  Value strVal = KVTKeyEncoder::keyStringToValue("hello_world");
  EXPECT_EQ(strVal.type(), Value::Type::STRING);
  EXPECT_EQ(strVal.getStr(), "hello_world");
}

TEST(KVTKeyEncoderTest, EscapeUnescapeValue) {
  // Test values with special characters
  Value vertexIdWithColon("vertex:with:colons");
  std::string encoded = KVTKeyEncoder::encodeVertexKey(100, 1, vertexIdWithColon, 10);
  
  // Should be able to decode back
  GraphSpaceID spaceId;
  PartitionID partId;
  Value decodedVertexId;
  TagID tagId;
  
  bool success = KVTKeyEncoder::decodeVertexKey(
      encoded, spaceId, partId, decodedVertexId, tagId);
  
  EXPECT_TRUE(success);
  EXPECT_EQ(vertexIdWithColon, decodedVertexId);
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}