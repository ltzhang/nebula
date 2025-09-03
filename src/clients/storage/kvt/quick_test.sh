#!/bin/bash

# Quick test to validate KVT is working
# This compiles and runs a minimal test without full build

echo "================================================"
echo "     KVT Quick Test                            "
echo "================================================"
echo ""

cd /home/lintaoz/work/nebula/src/clients/storage/kvt

# Compile kvt_mem
echo "Compiling kvt_mem..."
g++ -c -std=c++17 -O2 -fPIC kvt_mem.cpp -o kvt_mem.o
if [ $? -eq 0 ]; then
    echo "✓ kvt_mem compiled successfully"
else
    echo "✗ Failed to compile kvt_mem"
    exit 1
fi

# Create a simple test program
cat > quick_test.cpp << 'EOF'
#include <iostream>
#include <cstring>
#include <string>
#include "kvt_inc.h"

int main() {
    std::cout << "Starting KVT quick test..." << std::endl;
    
    // Initialize KVT
    KVTError init_result = kvt_initialize();
    if (init_result != KVTError::SUCCESS) {
        std::cout << "✗ Failed to initialize KVT" << std::endl;
        return 1;
    }
    std::cout << "✓ KVT initialized" << std::endl;
    
    // Create a table
    std::string error;
    uint64_t table_id;
    KVTError create_result = kvt_create_table("test_table", "hash", table_id, error);
    if (create_result != KVTError::SUCCESS) {
        std::cout << "✗ Failed to create table: " << error << std::endl;
        return 1;
    }
    std::cout << "✓ Table created with ID: " << table_id << std::endl;
    
    // Start a transaction
    uint64_t txn_id;
    KVTError txn_result = kvt_start_transaction(txn_id, error);
    if (txn_result != KVTError::SUCCESS) {
        std::cout << "✗ Failed to start transaction: " << error << std::endl;
        return 1;
    }
    std::cout << "✓ Transaction started with ID: " << txn_id << std::endl;
    
    // Put a key-value pair
    std::string key = "test_key";
    std::string value = "test_value";
    KVTError result = kvt_set(txn_id, table_id, key, value, error);
    if (result != KVTError::SUCCESS) {
        std::cout << "✗ Failed to set key-value: " << error << std::endl;
        return 1;
    }
    std::cout << "✓ Set operation successful" << std::endl;
    
    // Commit transaction
    result = kvt_commit_transaction(txn_id, error);
    if (result != KVTError::SUCCESS) {
        std::cout << "✗ Failed to commit transaction: " << error << std::endl;
        return 1;
    }
    std::cout << "✓ Transaction committed" << std::endl;
    
    // Read back the value in a new transaction
    txn_result = kvt_start_transaction(txn_id, error);
    if (txn_result != KVTError::SUCCESS) {
        std::cout << "✗ Failed to start read transaction: " << error << std::endl;
        return 1;
    }
    
    std::string retrieved;
    result = kvt_get(txn_id, table_id, key, retrieved, error);
    if (result != KVTError::SUCCESS) {
        std::cout << "✗ Failed to get value: " << error << std::endl;
        return 1;
    }
    
    if (retrieved == value) {
        std::cout << "✓ Retrieved correct value: " << retrieved << std::endl;
    } else {
        std::cout << "✗ Value mismatch. Expected: " << value << ", Got: " << retrieved << std::endl;
        return 1;
    }
    
    kvt_commit_transaction(txn_id, error);
    
    // Cleanup
    kvt_shutdown();
    std::cout << "✓ KVT shutdown" << std::endl;
    
    std::cout << "\n=== All tests passed! KVT is working correctly ===" << std::endl;
    return 0;
}
EOF

# Compile the test
echo ""
echo "Compiling quick test..."
g++ -std=c++17 -O2 quick_test.cpp kvt_mem.o -o quick_test
if [ $? -eq 0 ]; then
    echo "✓ Test compiled successfully"
else
    echo "✗ Failed to compile test"
    exit 1
fi

# Run the test
echo ""
echo "Running quick test..."
echo "------------------------"
./quick_test
result=$?

# Cleanup
rm -f quick_test quick_test.cpp kvt_mem.o

if [ $result -eq 0 ]; then
    echo ""
    echo "================================================"
    echo "     SUCCESS: KVT is working correctly!        "
    echo "================================================"
    echo ""
    echo "You can now:"
    echo "1. Build the full project: cd /home/lintaoz/work/nebula/build && make -j"
    echo "2. Run comprehensive tests: ./src/clients/storage/kvt/test/run_validation_tests.sh"
else
    echo ""
    echo "================================================"
    echo "     FAILED: KVT has issues                    "
    echo "================================================"
fi

exit $result