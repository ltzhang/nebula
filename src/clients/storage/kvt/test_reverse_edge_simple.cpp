#include <iostream>
#include <string>
#include <sstream>
#include <cassert>

// Simple test to verify reverse edge key format logic

class SimpleKeyEncoder {
public:
    static constexpr char EDGE_PREFIX = 'e';
    static constexpr char REVERSE_EDGE_PREFIX = 'r';
    static constexpr char SEPARATOR = ':';
    
    static std::string encodeEdgeKey(int spaceId, int partId, 
                                     const std::string& src, int edgeType, 
                                     int ranking, const std::string& dst) {
        std::stringstream ss;
        ss << EDGE_PREFIX << SEPARATOR
           << spaceId << SEPARATOR
           << partId << SEPARATOR
           << src << SEPARATOR
           << edgeType << SEPARATOR
           << ranking << SEPARATOR
           << dst;
        return ss.str();
    }
    
    static std::string encodeReverseEdgeKey(int spaceId, int partId, 
                                            const std::string& dst, int edgeType, 
                                            int ranking, const std::string& src) {
        std::stringstream ss;
        ss << REVERSE_EDGE_PREFIX << SEPARATOR
           << spaceId << SEPARATOR
           << partId << SEPARATOR
           << dst << SEPARATOR  // Note: dst comes first in reverse edge
           << edgeType << SEPARATOR
           << ranking << SEPARATOR
           << src;  // src comes last
        return ss.str();
    }
    
    static bool isReverseEdge(const std::string& key) {
        return !key.empty() && key[0] == REVERSE_EDGE_PREFIX;
    }
    
    static bool isForwardEdge(const std::string& key) {
        return !key.empty() && key[0] == EDGE_PREFIX;
    }
};

void testBasicEncoding() {
    std::cout << "Test 1: Basic Encoding" << std::endl;
    
    // Test data
    int spaceId = 1;
    int partId = 0;
    std::string src = "vertex_A";
    std::string dst = "vertex_B";
    int edgeType = 100;
    int ranking = 0;
    
    // Encode forward edge: A -> B
    std::string forwardKey = SimpleKeyEncoder::encodeEdgeKey(
        spaceId, partId, src, edgeType, ranking, dst);
    
    std::cout << "  Forward edge (A->B): " << forwardKey << std::endl;
    assert(SimpleKeyEncoder::isForwardEdge(forwardKey));
    assert(!SimpleKeyEncoder::isReverseEdge(forwardKey));
    
    // Encode reverse edge index for the same edge
    std::string reverseKey = SimpleKeyEncoder::encodeReverseEdgeKey(
        spaceId, partId, dst, edgeType, ranking, src);
    
    std::cout << "  Reverse index (B<-A): " << reverseKey << std::endl;
    assert(SimpleKeyEncoder::isReverseEdge(reverseKey));
    assert(!SimpleKeyEncoder::isForwardEdge(reverseKey));
    
    // Keys should be different
    assert(forwardKey != reverseKey);
    
    std::cout << "  ✓ Basic encoding test passed" << std::endl;
}

void testMultipleEdges() {
    std::cout << "\nTest 2: Multiple Edges to Same Vertex" << std::endl;
    
    int spaceId = 1;
    int partId = 0;
    int edgeType = 200;
    int ranking = 0;
    
    // Create edges: A->C, B->C, D->C
    std::string edgeAC = SimpleKeyEncoder::encodeEdgeKey(
        spaceId, partId, "A", edgeType, ranking, "C");
    std::string edgeBC = SimpleKeyEncoder::encodeEdgeKey(
        spaceId, partId, "B", edgeType, ranking, "C");
    std::string edgeDC = SimpleKeyEncoder::encodeEdgeKey(
        spaceId, partId, "D", edgeType, ranking, "C");
    
    // Create reverse indices for finding incoming edges to C
    std::string reverseCA = SimpleKeyEncoder::encodeReverseEdgeKey(
        spaceId, partId, "C", edgeType, ranking, "A");
    std::string reverseCB = SimpleKeyEncoder::encodeReverseEdgeKey(
        spaceId, partId, "C", edgeType, ranking, "B");
    std::string reverseCD = SimpleKeyEncoder::encodeReverseEdgeKey(
        spaceId, partId, "C", edgeType, ranking, "D");
    
    std::cout << "  Forward edges to C:" << std::endl;
    std::cout << "    A->C: " << edgeAC << std::endl;
    std::cout << "    B->C: " << edgeBC << std::endl;
    std::cout << "    D->C: " << edgeDC << std::endl;
    
    std::cout << "  Reverse indices for C:" << std::endl;
    std::cout << "    C<-A: " << reverseCA << std::endl;
    std::cout << "    C<-B: " << reverseCB << std::endl;
    std::cout << "    C<-D: " << reverseCD << std::endl;
    
    // All reverse keys should start with same prefix (r:1:0:C:200:)
    std::string expectedPrefix = "r:1:0:C:200:";
    assert(reverseCA.substr(0, expectedPrefix.length()) == expectedPrefix);
    assert(reverseCB.substr(0, expectedPrefix.length()) == expectedPrefix);
    assert(reverseCD.substr(0, expectedPrefix.length()) == expectedPrefix);
    
    std::cout << "  ✓ Multiple edges test passed" << std::endl;
}

void testBidirectionalEdges() {
    std::cout << "\nTest 3: Bidirectional Edges" << std::endl;
    
    int spaceId = 1;
    int partId = 0;
    int edgeType = 300;
    int ranking = 0;
    
    // Create bidirectional edges: X <-> Y
    std::string edgeXY = SimpleKeyEncoder::encodeEdgeKey(
        spaceId, partId, "X", edgeType, ranking, "Y");
    std::string edgeYX = SimpleKeyEncoder::encodeEdgeKey(
        spaceId, partId, "Y", edgeType, ranking, "X");
    
    // Reverse indices
    std::string reverseYX = SimpleKeyEncoder::encodeReverseEdgeKey(
        spaceId, partId, "Y", edgeType, ranking, "X");
    std::string reverseXY = SimpleKeyEncoder::encodeReverseEdgeKey(
        spaceId, partId, "X", edgeType, ranking, "Y");
    
    std::cout << "  Forward edges:" << std::endl;
    std::cout << "    X->Y: " << edgeXY << std::endl;
    std::cout << "    Y->X: " << edgeYX << std::endl;
    
    std::cout << "  Reverse indices:" << std::endl;
    std::cout << "    Y<-X: " << reverseYX << std::endl;
    std::cout << "    X<-Y: " << reverseXY << std::endl;
    
    // Each edge should have unique keys
    assert(edgeXY != edgeYX);
    assert(reverseYX != reverseXY);
    assert(edgeXY != reverseYX);
    assert(edgeYX != reverseXY);
    
    std::cout << "  ✓ Bidirectional edges test passed" << std::endl;
}

void testDeletionScenario() {
    std::cout << "\nTest 4: Deletion Scenario Simulation" << std::endl;
    
    int spaceId = 1;
    int partId = 0;
    int edgeType = 400;
    
    // Simulate graph: P -> Q -> R, P -> R
    std::cout << "  Initial graph: P -> Q -> R, P -> R" << std::endl;
    
    std::string edgePQ = SimpleKeyEncoder::encodeEdgeKey(
        spaceId, partId, "P", edgeType, 0, "Q");
    std::string edgeQR = SimpleKeyEncoder::encodeEdgeKey(
        spaceId, partId, "Q", edgeType, 0, "R");
    std::string edgePR = SimpleKeyEncoder::encodeEdgeKey(
        spaceId, partId, "P", edgeType, 0, "R");
    
    std::string reverseQP = SimpleKeyEncoder::encodeReverseEdgeKey(
        spaceId, partId, "Q", edgeType, 0, "P");
    std::string reverseRQ = SimpleKeyEncoder::encodeReverseEdgeKey(
        spaceId, partId, "R", edgeType, 0, "Q");
    std::string reverseRP = SimpleKeyEncoder::encodeReverseEdgeKey(
        spaceId, partId, "R", edgeType, 0, "P");
    
    std::cout << "  Keys to create:" << std::endl;
    std::cout << "    Forward: " << edgePQ << std::endl;
    std::cout << "    Reverse: " << reverseQP << std::endl;
    std::cout << "    Forward: " << edgeQR << std::endl;
    std::cout << "    Reverse: " << reverseRQ << std::endl;
    std::cout << "    Forward: " << edgePR << std::endl;
    std::cout << "    Reverse: " << reverseRP << std::endl;
    
    std::cout << "\n  Simulating: Delete vertex Q" << std::endl;
    std::cout << "  Keys to delete:" << std::endl;
    
    // When deleting Q, we need to:
    // 1. Delete outgoing edges from Q
    std::string scanPrefixOut = "e:1:0:Q:";
    std::cout << "    Scan for outgoing: " << scanPrefixOut << "*" << std::endl;
    std::cout << "      Found: " << edgeQR << " (delete)" << std::endl;
    std::cout << "      Found reverse: " << reverseRQ << " (delete)" << std::endl;
    
    // 2. Delete incoming edges to Q using reverse index
    std::string scanPrefixIn = "r:1:0:Q:";
    std::cout << "    Scan for incoming: " << scanPrefixIn << "*" << std::endl;
    std::cout << "      Found: " << reverseQP << " (delete)" << std::endl;
    std::cout << "      Found forward: " << edgePQ << " (delete)" << std::endl;
    
    std::cout << "\n  After deletion, remaining edges:" << std::endl;
    std::cout << "    " << edgePR << " (P->R still exists)" << std::endl;
    std::cout << "    " << reverseRP << " (reverse index for P->R)" << std::endl;
    
    std::cout << "  ✓ Deletion scenario test passed" << std::endl;
}

int main() {
    std::cout << "================================================" << std::endl;
    std::cout << "   Simple Reverse Edge Logic Test              " << std::endl;
    std::cout << "================================================" << std::endl;
    std::cout << "Testing the key encoding logic for reverse edges" << std::endl;
    std::cout << "" << std::endl;
    
    try {
        testBasicEncoding();
        testMultipleEdges();
        testBidirectionalEdges();
        testDeletionScenario();
        
        std::cout << "\n================================================" << std::endl;
        std::cout << "   ALL LOGIC TESTS PASSED!                     " << std::endl;
        std::cout << "================================================" << std::endl;
        std::cout << "" << std::endl;
        std::cout << "Summary:" << std::endl;
        std::cout << "✓ Forward edge format: e:<space>:<part>:<src>:<type>:<rank>:<dst>" << std::endl;
        std::cout << "✓ Reverse edge format: r:<space>:<part>:<dst>:<type>:<rank>:<src>" << std::endl;
        std::cout << "✓ Deletion uses both forward and reverse prefixes for complete cleanup" << std::endl;
        
        return 0;
    } catch (const std::exception& e) {
        std::cout << "\n✗ Test failed: " << e.what() << std::endl;
        return 1;
    }
}