#!/bin/bash

# KVT Validation Test Runner
# This script builds and runs all KVT validation tests

set -e  # Exit on error

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../../../../.."
BUILD_DIR="$PROJECT_ROOT/build"

echo "================================================"
echo "     KVT Validation Test Runner                "
echo "================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}[PASS]${NC} $2"
    else
        echo -e "${RED}[FAIL]${NC} $2"
    fi
}

# Step 1: Check if build directory exists
echo "Step 1: Checking build environment..."
if [ ! -d "$BUILD_DIR" ]; then
    echo -e "${YELLOW}Build directory not found. Creating and configuring...${NC}"
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"
    cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_TESTING=ON ..
else
    cd "$BUILD_DIR"
    echo -e "${GREEN}Build directory found${NC}"
fi

# Step 2: Build the tests
echo ""
echo "Step 2: Building KVT tests..."
echo "Building kvt_validation_test..."
make kvt_validation_test -j$(nproc) 2>&1 | tail -5
print_status ${PIPESTATUS[0]} "kvt_validation_test build"

echo "Building kvt_nebula_integration_test..."
make kvt_nebula_integration_test -j$(nproc) 2>&1 | tail -5
print_status ${PIPESTATUS[0]} "kvt_nebula_integration_test build"

echo "Building existing KVT tests..."
make kvt_key_encoder_test -j$(nproc) 2>&1 | tail -5
print_status ${PIPESTATUS[0]} "kvt_key_encoder_test build"

make kvt_transaction_manager_test -j$(nproc) 2>&1 | tail -5
print_status ${PIPESTATUS[0]} "kvt_transaction_manager_test build"

make kvt_storage_client_test -j$(nproc) 2>&1 | tail -5
print_status ${PIPESTATUS[0]} "kvt_storage_client_test build"

# Step 3: Run the tests
echo ""
echo "Step 3: Running KVT validation tests..."
echo ""

TEST_RESULTS=()
FAILED_TESTS=()

# Function to run a test and capture result
run_test() {
    local test_name=$1
    local test_binary="$BUILD_DIR/bin/test/$test_name"
    
    if [ ! -f "$test_binary" ]; then
        echo -e "${YELLOW}Warning: $test_name binary not found at $test_binary${NC}"
        return 1
    fi
    
    echo "================================================"
    echo "Running: $test_name"
    echo "================================================"
    
    if $test_binary --gtest_color=yes 2>&1; then
        TEST_RESULTS+=("$test_name: PASSED")
        echo -e "${GREEN}$test_name: PASSED${NC}"
        return 0
    else
        TEST_RESULTS+=("$test_name: FAILED")
        FAILED_TESTS+=("$test_name")
        echo -e "${RED}$test_name: FAILED${NC}"
        return 1
    fi
}

# Run all tests
echo "Running core KVT validation test..."
run_test "kvt_validation_test" || true

echo ""
echo "Running KVT-NebulaGraph integration test..."
run_test "kvt_nebula_integration_test" || true

echo ""
echo "Running KVT key encoder test..."
run_test "kvt_key_encoder_test" || true

echo ""
echo "Running KVT transaction manager test..."
run_test "kvt_transaction_manager_test" || true

echo ""
echo "Running KVT storage client test..."
run_test "kvt_storage_client_test" || true

# Step 4: Summary
echo ""
echo "================================================"
echo "              TEST SUMMARY                     "
echo "================================================"

for result in "${TEST_RESULTS[@]}"; do
    if [[ $result == *"PASSED"* ]]; then
        echo -e "${GREEN}✓ $result${NC}"
    else
        echo -e "${RED}✗ $result${NC}"
    fi
done

echo ""
if [ ${#FAILED_TESTS[@]} -eq 0 ]; then
    echo -e "${GREEN}All tests passed successfully!${NC}"
    exit 0
else
    echo -e "${RED}${#FAILED_TESTS[@]} test(s) failed:${NC}"
    for test in "${FAILED_TESTS[@]}"; do
        echo -e "${RED}  - $test${NC}"
    done
    
    echo ""
    echo "To debug a specific test, run:"
    echo "  gdb $BUILD_DIR/bin/test/<test_name>"
    echo "Or run with verbose output:"
    echo "  $BUILD_DIR/bin/test/<test_name> --gtest_filter=* --gtest_print_time=1"
    
    exit 1
fi