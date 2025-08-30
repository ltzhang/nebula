# KVT Storage Engine Integration for NebulaGraph

This document provides detailed instructions for building and testing NebulaGraph with the KVT (Key-Value Transaction) storage engine.

## Overview

The KVT storage engine provides an alternative to NebulaGraph's default distributed storage, offering transactional key-value operations with ACID properties. This integration allows NebulaGraph to use KVT as its underlying storage backend for development, testing, and specialized use cases.

## Prerequisites

- Ubuntu 20.04+ or compatible Linux distribution
- GCC 9+ or Clang 10+
- CMake 3.9+
- Git
- All standard NebulaGraph build dependencies

## Directory Structure

```
nebula/
├── src/clients/storage/
│   ├── KvtStorageClient.h          # KVT storage client header
│   ├── KvtStorageClient.cpp        # KVT storage client implementation
│   ├── KvtStore.h                  # KVT store wrapper header
│   ├── KvtStore.cpp                # KVT store wrapper implementation
│   ├── StorageClientWrapper.h      # Unified storage client wrapper
│   ├── kvt/                        # KVT implementation directory
│   │   ├── kvt_inc.h -> ../include/kvt_inc.h  # KVT API header (symlink)
│   │   ├── kvt_mem.h               # KVT memory implementation header
│   │   ├── kvt_mem.cpp             # KVT memory implementation
│   │   ├── kvt_impl.cpp            # KVT API implementation
│   │   └── kvt_sample.cpp          # KVT usage example
│   └── README_KVT.md               # This file
```

## Building NebulaGraph with KVT Storage

### Step 1: Ensure KVT Files are Present

First, verify that the KVT source files are available:

```bash
cd /path/to/nebula
ls -la src/clients/storage/kvt/
```

You should see the KVT implementation files including `kvt_inc.h`, `kvt_mem.h`, `kvt_mem.cpp`, and `kvt_impl.cpp`.

### Step 2: Create Build Directory

```bash
cd /path/to/nebula
mkdir -p build_kvt
cd build_kvt
```

### Step 3: Configure Build with KVT

Configure the build system with KVT storage enabled:

```bash
cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_KVT=ON ..
```

For release build:
```bash
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_KVT=ON ..
```

### Step 4: Build the Project

Build the entire project:
```bash
make -j$(nproc)
```

Or build specific KVT components for testing:
```bash
# Build KVT store implementation
make kvt_store_obj

# Build KVT storage client
make kvt_storage_client_obj

# Build KVT implementation
make kvt_impl_obj
```

### Step 5: Verify Build Success

Check that the build completed without errors:
```bash
echo $?
```
Should return `0` for successful build.

## Testing the KVT Storage Engine

### Unit Testing KVT Components

#### Test 1: Build KVT Store Object

```bash
cd build_kvt
make kvt_store_obj
```

Expected output:
```
[100%] Building CXX object src/clients/storage/CMakeFiles/kvt_store_obj.dir/KvtStore.cpp.o
[100%] Built target kvt_store_obj
```

#### Test 2: Build KVT Storage Client

```bash
cd build_kvt
make kvt_storage_client_obj
```

Expected output:
```
[100%] Building CXX object src/clients/storage/CMakeFiles/kvt_storage_client_obj.dir/KvtStorageClient.cpp.o
[100%] Built target kvt_storage_client_obj
```

#### Test 3: Run KVT Sample Program

Build and run the KVT sample to verify the KVT API works correctly:

```bash
cd build_kvt
# Build a simple test executable (you may need to create this)
g++ -I../src/clients/storage/kvt -I../src ../src/clients/storage/kvt/kvt_sample.cpp ../src/clients/storage/kvt/kvt_impl.cpp ../src/clients/storage/kvt/kvt_mem.cpp -o kvt_sample_test

# Run the test
./kvt_sample_test
```

Expected output:
```
==================================
     KVT API Sample Program      
==================================
✓ KVT system initialized

========== Basic Operations Test ==========
✓ Created table 'users' with ID: 1
✓ Duplicate table creation correctly failed: Table users already exists
✓ Set user:1 = Alice
✓ Retrieved user:1 = Alice
✓ Updated user:1 = Alice Smith
✓ Verified update: user:1 = Alice Smith

========== Transaction Test ==========
✓ Started transaction ID: 1
✓ Set user:2 = Bob (in transaction)
✓ Set user:3 = Charlie (in transaction)
✓ Read user:2 in transaction = Bob
✓ Transaction committed successfully
✓ Verified user:2 after commit = Bob

========== Rollback Test ==========
✓ Started transaction ID: 2
✓ Set user:4 = David (in transaction)
✓ Transaction rolled back successfully
✓ Verified user:4 does not exist after rollback

========== Range Scan Test ==========
✓ Created range-partitioned table 'products' with ID: 2
✓ Inserted 5 products
✓ Scan from prod:002 to prod:004 returned 3 items:
  prod:002 = Mouse
  prod:003 = Keyboard
  prod:004 = Monitor

========== Concurrent Transactions Test ==========
✓ Started transaction 1: 3
✓ Started transaction 2: 4
✓ TX1: Set user:10
✓ TX2: Set user:11
✓ TX1: Committed
✓ TX2: Committed
✓ Verified user:10 = Transaction1_User
✓ Verified user:11 = Transaction2_User

========== All Tests Completed Successfully ==========

✓ KVT system shutdown
```

### Integration Testing

#### Test 4: Build Storage Client Switch Test

If testing is enabled, build and run the storage client switch test:

```bash
cd build_kvt
make storage_client_switch_test
./bin/storage_client_switch_test
```

#### Test 5: Build Graph Service with KVT

Build the graph service with KVT storage:

```bash
cd build_kvt
make nebula-graphd
```

#### Test 6: Build Storage Service Components

```bash
cd build_kvt
make nebula-storaged
```

### Runtime Testing

#### Test 7: Start NebulaGraph Services

Start the meta service:
```bash
cd build_kvt
./bin/nebula-metad --flagfile=conf/nebula-metad.conf.default --daemonize=false
```

In another terminal, start the storage service:
```bash
cd build_kvt
./bin/nebula-storaged --flagfile=conf/nebula-storaged.conf.default --daemonize=false
```

In a third terminal, start the graph service:
```bash
cd build_kvt
./bin/nebula-graphd --flagfile=conf/nebula-graphd.conf.default --daemonize=false
```

#### Test 8: Basic Graph Operations

Connect to the graph service and perform basic operations:

```bash
cd build_kvt
./bin/nebula-console -addr 127.0.0.1 -port 9669 -u root -p password
```

In the console, try:
```sql
CREATE SPACE test_kvt(vid_type=FIXED_STRING(32));
USE test_kvt;
CREATE TAG person(name string, age int);
INSERT VERTEX person(name, age) VALUES "alice":("Alice", 25);
FETCH PROP ON person "alice";
```

## Troubleshooting

### Common Build Issues

#### Issue 1: Missing KVT Headers
**Error**: `kvt_inc.h: No such file or directory`

**Solution**: Ensure the KVT symlink is properly created:
```bash
cd src/clients/storage/kvt
ls -la kvt_inc.h
# Should show: kvt_inc.h -> ../include/kvt_inc.h
```

#### Issue 2: Unused Parameter Warnings
**Error**: `unused parameter 'paramName' [-Werror=unused-parameter]`

**Solution**: Parameters in stub implementations are marked with `/* paramName */` to suppress warnings.

#### Issue 3: USE_KVT Option Not Recognized
**Error**: `Manually-specified variables were not used by the project: USE_KVT`

**Solution**: Ensure the main CMakeLists.txt includes the USE_KVT option definition:
```bash
grep -n "USE_KVT" CMakeLists.txt
# Should show the option definition
```

### Runtime Issues

#### Issue 1: KVT Initialization Failure
**Symptom**: `Failed to initialize KVT system` in logs

**Solution**: 
1. Check that KVT implementation is properly compiled
2. Verify no conflicting storage options are enabled
3. Ensure sufficient memory is available

#### Issue 2: Transaction Conflicts
**Symptom**: Transaction commit failures

**Solution**: The current KVT implementation uses a simple serializable approach. For high concurrency, consider implementing more sophisticated concurrency control.

## Performance Considerations

### Memory Usage
- KVT stores all data in memory
- Monitor memory consumption for large datasets
- Consider implementing data persistence if needed

### Concurrency
- Current implementation allows only one transaction at a time
- For production use, implement concurrent transaction support
- Consider implementing 2PL or OCC concurrency control

### Scalability
- KVT is designed for single-node operation
- For distributed scenarios, consider partitioning strategies
- Monitor performance with large key spaces

## Development Notes

### Extending KVT Storage Client

To add new operations to the KVT storage client:

1. Add method declaration to `KvtStorageClient.h`
2. Implement method in `KvtStorageClient.cpp`
3. Use KvtStore wrapper for KVT operations
4. Follow transaction patterns from existing methods

### Adding Batch Operations

When KVT adds native batch support:

1. Update `kvt_inc.h` with new batch API functions
2. Implement batch functions in `kvt_impl.cpp`
3. Update `KvtStore.cpp` to use native batch operations
4. Modify `KvtStorageClient.cpp` to use improved batch methods

## Configuration Options

### Build-time Options

- `USE_KVT=ON`: Enable KVT storage backend
- `CMAKE_BUILD_TYPE=Debug`: Enable debug information and logging
- `CMAKE_BUILD_TYPE=Release`: Optimized production build

### Runtime Configuration

KVT storage client inherits configuration from NebulaGraph's standard storage configuration files. No additional KVT-specific configuration is currently required.

## Support and Contributing

For issues related to KVT integration:
1. Check the troubleshooting section above
2. Verify build configuration matches this guide
3. Review KVT sample program output for API validation

For KVT API improvements:
1. Modify the KVT implementation in the `kvt/` directory
2. Update the KVT wrapper classes as needed
3. Test changes with the provided test procedures