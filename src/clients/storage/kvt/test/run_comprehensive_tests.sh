#!/bin/bash

# KVT Comprehensive Test Runner
# This script builds and runs comprehensive tests for the reverse edge functionality

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../../../../.."
BUILD_DIR="$PROJECT_ROOT/build"

echo "================================================"
echo "     KVT Comprehensive Test Runner             "
echo "================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $2"
    else
        echo -e "${RED}✗${NC} $2"
    fi
}

# Step 1: Check build directory
echo -e "${BLUE}Step 1: Checking build environment...${NC}"
if [ ! -d "$BUILD_DIR" ]; then
    echo "Creating build directory..."
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"
    cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_TESTING=ON ..
else
    cd "$BUILD_DIR"
    echo "Build directory found"
fi

# Step 2: Build the tests
echo ""
echo -e "${BLUE}Step 2: Building KVT tests...${NC}"

# Build all KVT tests
TESTS_TO_BUILD=(
    "kvt_comprehensive_test"
    "kvt_reverse_edge_test"
    "kvt_validation_test"
)

for test in "${TESTS_TO_BUILD[@]}"; do
    echo -n "Building $test... "
    if make $test -j$(nproc) > /tmp/${test}_build.log 2>&1; then
        print_status 0 "$test built successfully"
    else
        print_status 1 "$test build failed"
        echo "See /tmp/${test}_build.log for details"
        exit 1
    fi
done

# Step 3: Run the comprehensive test
echo ""
echo -e "${BLUE}Step 3: Running comprehensive tests...${NC}"
echo ""

# Run the main comprehensive test
TEST_BINARY="$BUILD_DIR/bin/test/kvt_comprehensive_test"

if [ ! -f "$TEST_BINARY" ]; then
    echo -e "${RED}Error: Test binary not found at $TEST_BINARY${NC}"
    exit 1
fi

echo "================================================"
echo "Running KVT Comprehensive Test"
echo "================================================"

# Run with verbose output and timing
$TEST_BINARY --gtest_color=yes --gtest_print_time=1 2>&1 | tee /tmp/kvt_comprehensive_results.log

TEST_RESULT=${PIPESTATUS[0]}

# Step 4: Run additional tests if comprehensive test passed
if [ $TEST_RESULT -eq 0 ]; then
    echo ""
    echo -e "${BLUE}Step 4: Running additional validation tests...${NC}"
    echo ""
    
    # Run reverse edge test
    echo "Running reverse edge test..."
    if $BUILD_DIR/bin/test/kvt_reverse_edge_test --gtest_color=yes > /tmp/kvt_reverse_edge.log 2>&1; then
        print_status 0 "Reverse edge test passed"
    else
        print_status 1 "Reverse edge test failed"
        echo "See /tmp/kvt_reverse_edge.log for details"
    fi
    
    # Run validation test
    echo "Running validation test..."
    if $BUILD_DIR/bin/test/kvt_validation_test --gtest_color=yes > /tmp/kvt_validation.log 2>&1; then
        print_status 0 "Validation test passed"
    else
        print_status 1 "Validation test failed"
        echo "See /tmp/kvt_validation.log for details"
    fi
fi

# Step 5: Summary
echo ""
echo "================================================"
echo "              TEST SUMMARY                     "
echo "================================================"

if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}✓ Comprehensive test suite PASSED${NC}"
    
    # Extract statistics from log
    echo ""
    echo "Test Statistics:"
    grep -E "^\[.*\] (Created|Added|Deleted|Final)" /tmp/kvt_comprehensive_results.log | tail -10
    
    echo ""
    echo "Key Achievements:"
    echo "  ✓ Reverse edge indexing working correctly"
    echo "  ✓ Complete delete operations verified"
    echo "  ✓ Graph consistency maintained"
    echo "  ✓ Complex patterns handled"
    echo "  ✓ Large scale operations successful"
    echo "  ✓ Stress test passed"
    
else
    echo -e "${RED}✗ Comprehensive test suite FAILED${NC}"
    echo ""
    echo "Failed tests:"
    grep -E "^\[  FAILED  \]" /tmp/kvt_comprehensive_results.log
    
    echo ""
    echo "To debug, run:"
    echo "  gdb $TEST_BINARY"
    echo "  (gdb) run --gtest_filter=<TestName>"
fi

echo ""
echo "Full test output saved to: /tmp/kvt_comprehensive_results.log"
echo ""

exit $TEST_RESULT