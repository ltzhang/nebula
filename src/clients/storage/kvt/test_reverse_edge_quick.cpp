#include <iostream>
#include <string>
#include <cassert>
#include "../kvt_inc.h"
#include "../KVTKeyEncoder.h"

using namespace nebula;
using namespace nebula::storage;

void testReverseEdgeEncoding() {
    std::cout << "Testing reverse edge encoding..." << std::endl;
    
    // Test forward edge encoding
    Value srcId("vertex1");
    Value dstId("vertex2");
    GraphSpaceID spaceId = 1;
    PartitionID partId = 0;
    EdgeType edgeType = 100;
    EdgeRanking ranking = 0;
    
    std::string forwardKey = KVTKeyEncoder::encodeEdgeKey(
        spaceId, partId, srcId, edgeType, ranking, dstId);
    
    std::cout << "Forward edge key: " << forwardKey << std::endl;
    assert(forwardKey[0] == KVTKeyEncoder::EDGE_PREFIX);
    
    // Test reverse edge encoding
    std::string reverseKey = KVTKeyEncoder::encodeReverseEdgeKey(
        spaceId, partId, dstId, edgeType, ranking, srcId);
    
    std::cout << "Reverse edge key: " << reverseKey << std::endl;
    assert(reverseKey[0] == KVTKeyEncoder::REVERSE_EDGE_PREFIX);
    
    // Test decoding
    GraphSpaceID decodedSpaceId;
    PartitionID decodedPartId;
    Value decodedDstId, decodedSrcId;
    EdgeType decodedEdgeType;
    EdgeRanking decodedRanking;
    
    bool success = KVTKeyEncoder::decodeReverseEdgeKey(
        reverseKey, decodedSpaceId, decodedPartId, 
        decodedDstId, decodedEdgeType, decodedRanking, decodedSrcId);
    
    assert(success);
    assert(decodedSpaceId == spaceId);
    assert(decodedPartId == partId);
    assert(decodedDstId == dstId);
    assert(decodedSrcId == srcId);
    assert(decodedEdgeType == edgeType);
    assert(decodedRanking == ranking);
    
    std::cout << "✓ Reverse edge encoding/decoding works correctly" << std::endl;
}

void testReverseEdgePrefix() {
    std::cout << "Testing reverse edge prefix generation..." << std::endl;
    
    Value dstId("targetVertex");
    GraphSpaceID spaceId = 1;
    PartitionID partId = 0;
    EdgeType edgeType = 200;
    
    // Test prefix without edge type
    std::string prefix1 = KVTKeyEncoder::reverseEdgePrefix(
        spaceId, partId, &dstId);
    std::cout << "Prefix (no edge type): " << prefix1 << std::endl;
    assert(prefix1[0] == KVTKeyEncoder::REVERSE_EDGE_PREFIX);
    
    // Test prefix with edge type
    std::string prefix2 = KVTKeyEncoder::reverseEdgePrefix(
        spaceId, partId, &dstId, edgeType);
    std::cout << "Prefix (with edge type): " << prefix2 << std::endl;
    assert(prefix2[0] == KVTKeyEncoder::REVERSE_EDGE_PREFIX);
    assert(prefix2.length() > prefix1.length());
    
    std::cout << "✓ Reverse edge prefix generation works correctly" << std::endl;
}

int main() {
    std::cout << "================================================" << std::endl;
    std::cout << "   Quick Reverse Edge Test                     " << std::endl;
    std::cout << "================================================" << std::endl;
    
    try {
        testReverseEdgeEncoding();
        testReverseEdgePrefix();
        
        std::cout << "\n================================================" << std::endl;
        std::cout << "   ALL TESTS PASSED!                           " << std::endl;
        std::cout << "================================================" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cout << "\n✗ Test failed: " << e.what() << std::endl;
        return 1;
    }
}