#!/bin/bash

# Quick KVT Validation Script
# This script performs a quick validation of the KVT implementation

set -e

echo "================================================"
echo "     KVT Quick Validation                      "
echo "================================================"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

PROJECT_ROOT="/home/lintaoz/work/nebula"
cd "$PROJECT_ROOT"

echo "1. Checking KVT source files..."
echo "-----------------------------------"

FILES_TO_CHECK=(
    "src/clients/storage/kvt/kvt_inc.h"
    "src/clients/storage/kvt/kvt_mem.cpp"
    "src/clients/storage/kvt/kvt_mem.h"
    "src/clients/storage/kvt/KVTStorageClient.cpp"
    "src/clients/storage/kvt/KVTStorageClient.h"
    "src/clients/storage/kvt/KVTKeyEncoder.cpp"
    "src/clients/storage/kvt/KVTKeyEncoder.h"
    "src/clients/storage/kvt/KVTTransactionManager.cpp"
    "src/clients/storage/kvt/KVTTransactionManager.h"
    "src/clients/storage/kvt/KVTValueEncoder.cpp"
    "src/clients/storage/kvt/KVTValueEncoder.h"
)

all_files_exist=true
for file in "${FILES_TO_CHECK[@]}"; do
    if [ -f "$file" ]; then
        echo -e "${GREEN}✓${NC} $file"
    else
        echo -e "${RED}✗${NC} $file - NOT FOUND"
        all_files_exist=false
    fi
done

if [ "$all_files_exist" = true ]; then
    echo -e "${GREEN}All KVT source files present${NC}"
else
    echo -e "${RED}Some KVT source files missing${NC}"
    exit 1
fi

echo ""
echo "2. Checking build configuration..."
echo "-----------------------------------"

# Check if KVT is enabled by default
if grep -q 'option(ENABLE_KVT "Enable KVT storage backend" ON)' src/clients/storage/CMakeLists.txt; then
    echo -e "${GREEN}✓${NC} KVT enabled by default in CMakeLists.txt"
else
    echo -e "${YELLOW}⚠${NC} KVT not enabled by default"
fi

# Check if kvt_mem is included in build
if grep -q 'kvt_mem_obj' src/daemons/CMakeLists.txt; then
    echo -e "${GREEN}✓${NC} kvt_mem linked in executables"
else
    echo -e "${RED}✗${NC} kvt_mem not linked in executables"
fi

echo ""
echo "3. Checking test files..."
echo "-----------------------------------"

TEST_FILES=(
    "src/clients/storage/kvt/test/kvt_validation_test.cpp"
    "src/clients/storage/kvt/test/kvt_nebula_integration_test.cpp"
    "src/clients/storage/kvt/test/KVTKeyEncoderTest.cpp"
    "src/clients/storage/kvt/test/KVTTransactionManagerTest.cpp"
    "src/clients/storage/kvt/test/KVTStorageClientTest.cpp"
)

test_count=0
for file in "${TEST_FILES[@]}"; do
    if [ -f "$file" ]; then
        echo -e "${GREEN}✓${NC} $(basename $file)"
        ((test_count++))
    else
        echo -e "${YELLOW}⚠${NC} $(basename $file) - not found"
    fi
done

echo -e "Found ${test_count}/${#TEST_FILES[@]} test files"

echo ""
echo "4. Analyzing implementation status..."
echo "-----------------------------------"

# Count TODOs in implementation
todo_count=$(grep -r "TODO\|FIXME" src/clients/storage/kvt/*.cpp src/clients/storage/kvt/*.h 2>/dev/null | wc -l || echo "0")
echo "Found $todo_count TODOs/FIXMEs in implementation"

# Check key operations implementation
echo ""
echo "Checking implemented operations:"

operations=(
    "addVertices"
    "addEdges"
    "getProps"
    "updateVertex"
    "updateEdge"
    "deleteVertices"
    "deleteEdges"
)

for op in "${operations[@]}"; do
    if grep -q "KVTStorageClient::$op" src/clients/storage/kvt/KVTStorageClient.cpp; then
        # Check if it has TODO
        if grep -A5 "KVTStorageClient::$op" src/clients/storage/kvt/KVTStorageClient.cpp | grep -q "TODO"; then
            echo -e "${YELLOW}⚠${NC} $op - implemented but has TODOs"
        else
            echo -e "${GREEN}✓${NC} $op - implemented"
        fi
    else
        echo -e "${RED}✗${NC} $op - not found"
    fi
done

echo ""
echo "5. Documentation status..."
echo "-----------------------------------"

DOC_FILES=(
    "src/clients/storage/KVT_README.md"
    "src/clients/storage/HowTo.md"
)

for file in "${DOC_FILES[@]}"; do
    if [ -f "$file" ]; then
        line_count=$(wc -l < "$file")
        echo -e "${GREEN}✓${NC} $(basename $file) - $line_count lines"
    else
        echo -e "${RED}✗${NC} $(basename $file) - not found"
    fi
done

echo ""
echo "================================================"
echo "              VALIDATION SUMMARY                "
echo "================================================"

if [ "$all_files_exist" = true ] && [ $test_count -gt 0 ]; then
    echo -e "${GREEN}✓ KVT implementation structure is valid${NC}"
    echo -e "${GREEN}✓ Core components are in place${NC}"
    echo -e "${YELLOW}⚠ $todo_count items still need implementation${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Build the project: cd build && cmake .. && make -j\$(nproc)"
    echo "2. Run tests: ./src/clients/storage/kvt/test/run_validation_tests.sh"
    echo "3. Start fixing TODOs in priority order"
    exit 0
else
    echo -e "${RED}✗ KVT implementation has issues${NC}"
    echo "Please check the output above for details"
    exit 1
fi