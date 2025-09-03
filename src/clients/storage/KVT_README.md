# KVT Integration into NebulaGraph

## Overview

KVT (Key-Value Transactional store) is a transactional storage interface being integrated into NebulaGraph to replace the traditional distributed storage layer. Instead of the Graph Service making remote RPC calls to Storage Service nodes, KVT is embedded directly into the storage client, allowing the Graph Service to interact with a local transactional key-value store.

**Current Status**: Early development phase (~35% complete). Core functionality working with memory implementation.

## Architecture Changes

### Traditional NebulaGraph Architecture
```
┌─────────────┐
│Graph Service│
└──────┬──────┘
       │ RPC (Thrift)
       ▼
┌─────────────┐
│Storage      │
│Service      │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  RocksDB    │
└─────────────┘
```

### KVT-Integrated Architecture
```
┌─────────────┐
│Graph Service│
└──────┬──────┘
       │ Direct Call
       ▼
┌─────────────┐
│Storage      │
│Client + KVT │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│KVT Interface│
│(kvt_inc.h)  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│KVT Impl     │
│(kvt_mem.cpp)│
└─────────────┘
```

## Components

### 1. KVT Interface (`kvt/kvt_inc.h`)
The core API defining the transactional key-value store interface:

- **Transaction Management**
  - `kvt_begin()`: Start a new transaction
  - `kvt_commit()`: Commit the current transaction
  - `kvt_rollback()`: Rollback the current transaction

- **Data Operations**
  - `kvt_get()`: Retrieve a value by key
  - `kvt_put()`: Insert or update a key-value pair
  - `kvt_delete()`: Remove a key-value pair
  - `kvt_scan()`: Scan a range of keys

- **Properties**
  - ACID compliance (Atomicity, Consistency, Isolation, Durability)
  - Snapshot isolation for concurrent transactions
  - Support for both point queries and range scans

### 2. KVT Memory Implementation (`kvt/kvt_mem.cpp`)
A reference implementation providing in-memory transactional storage:

- **Transaction Support**: Full MVCC (Multi-Version Concurrency Control)
- **Conflict Detection**: Optimistic concurrency control with validation at commit
- **Data Structures**: 
  - Primary storage using ordered maps
  - Transaction-local write sets and read sets
  - Version tracking for isolation

### 3. Storage Client Integration
Modified storage client components to use KVT:

- **GraphStorageClient**: Primary interface for Graph Service
- **StorageClientBase**: Base implementation with KVT integration
- **Transaction Wrapper**: Manages KVT transactions for graph operations

## Building

### Prerequisites
- CMake 3.13+
- C++17 compatible compiler
- NebulaGraph build dependencies

### Build KVT Memory Implementation
```bash
cd src/clients/storage/kvt
g++ -c -std=c++17 -O2 kvt_mem.cpp -o kvt_mem.o
```

### Build NebulaGraph with KVT
```bash
# From project root
mkdir -p build && cd build

# Configure with KVT enabled
cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_KVT=ON ..

# Build
make -j$(nproc)
```

### Link KVT Implementation
The build system will automatically link `kvt_mem.o` when KVT is enabled. To use a different KVT implementation, replace the object file or modify the CMake configuration.

## Testing

### Available Tests

#### Unit Tests
Located in `src/clients/storage/kvt/test/`:
- `kvt_validation_test.cpp` - Core KVT API validation
- `kvt_nebula_integration_test.cpp` - NebulaGraph integration tests  
- `KVTKeyEncoderTest.cpp` - Key encoding tests
- `KVTTransactionManagerTest.cpp` - Transaction manager tests
- `KVTStorageClientTest.cpp` - Storage client tests

#### Quick Validation
```bash
# Quick test without full build
cd src/clients/storage/kvt
./quick_test.sh

# Validation script
./validate_kvt.sh
```

#### Building and Running Tests
```bash
# Build with testing enabled (KVT tests are built automatically)
cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_TESTING=ON ..
make -j$(nproc)

# After build, test binaries are available in bin/test/
ls bin/test/kvt_*

# Run all KVT tests
../src/clients/storage/kvt/test/run_validation_tests.sh

# Run individual tests
./bin/test/kvt_validation_test
./bin/test/kvt_key_encoder_test
./bin/test/kvt_transaction_manager_test
./bin/test/kvt_reverse_edge_test
./bin/test/kvt_comprehensive_test
./bin/test/kvt_nebula_integration_test
```

**Note**: Performance benchmarks and distributed tests are not yet implemented.

## Configuration

### Enable KVT in Graph Service
KVT is enabled by default when building with CMake. To use KVT at runtime:

```yaml
# conf/nebula-graphd.conf
--enable_kvt=true
--kvt_memory_limit=4GB  # For memory implementation
```

**Note**: Most KVT-specific configuration options are planned but not yet implemented. Currently, KVT uses default settings with the memory implementation.

## Design Decisions

### 1. Client-Side Integration
**Decision**: Integrate KVT at the storage client level rather than replacing Storage Service entirely.

**Rationale**:
- Minimal changes to Graph Service logic
- Preserves existing query execution paths
- Allows gradual migration and A/B testing
- Maintains compatibility with distributed deployments

### 2. Transaction Boundaries
**Decision**: Map graph operations to KVT transactions at the statement level.

**Mapping**:
- Single statement → Single transaction
- Batch operations → Single transaction with multiple operations
- Multi-statement queries → Transaction per statement (future: session transactions)

### 3. Data Model Mapping
**Decision**: Preserve NebulaGraph's data model, mapping to KVT's key-value model.

**Schema**:
```
Vertices:  {PartID}:{VID}:{TagID} → {Properties}
Edges:     {PartID}:{SrcVID}:{EdgeType}:{Rank}:{DstVID} → {Properties}
Indexes:   {IndexID}:{IndexValue} → {VID/EdgeKey}
```

### 4. Isolation Level
**Decision**: Default to Snapshot Isolation for optimal performance/consistency trade-off.

**Benefits**:
- No read locks required
- High concurrency for read-heavy workloads
- Predictable performance

## Future Optimization Opportunities

These optimizations are planned for future development:

1. **Batch Processing** - Group multiple operations into single KVT transactions
2. **Caching Layer** - Add LRU cache for frequently accessed data
3. **Parallel Execution** - Enable concurrent transaction processing
4. **Index Optimization** - Implement efficient secondary indexes
5. **Memory Layout** - Optimize data structures for graph workloads
6. **Compression** - Add value compression for large properties

## Current Implementation Status

### Completed Features
- ✅ Core KVT interface (`kvt_inc.h`)
- ✅ Memory implementation with MVCC (`kvt_mem.cpp`)
- ✅ Basic CRUD operations for vertices and edges
- ✅ Transaction support with isolation
- ✅ Key/Value encoders for NebulaGraph data model
- ✅ CMake integration (enabled by default)

### In Progress (with TODOs)
- ⚠️ Partition calculation (currently hardcoded to partition 0)
- ⚠️ Complete delete operations (missing reverse edge cleanup)
- ⚠️ Index operations (27 TODOs remaining)

## Limitations

### Current Limitations
- Memory-only implementation (no persistence)
- No distributed transaction support
- Partition calculation not implemented
- Missing scan operations (scanVertex, scanEdge)
- No index support (lookupIndex operations)
- Admin operations not implemented

### Not Yet Implemented
- Production storage backend (RocksDB integration)
- Performance benchmarking tools
- Distributed deployment support
- Recovery and backup mechanisms

## Troubleshooting

### Common Issues

1. **Transaction Conflicts**
   - Symptom: Frequent rollbacks and retries
   - Solution: Adjust isolation level or increase retry count

2. **Memory Issues (kvt_mem)**
   - Symptom: Out of memory errors
   - Solution: Increase memory limit or implement eviction

3. **Performance Degradation**
   - Symptom: Slower than traditional storage
   - Solution: Check transaction boundaries, enable batching

### Debug Options
```bash
# Check if KVT is working
cd src/clients/storage/kvt
./quick_test.sh

# Validate implementation
./validate_kvt.sh
```

**Note**: Advanced debug logging and monitoring tools are not yet implemented.

## API Examples

### Basic Usage (Current API)
```cpp
// Initialize KVT
KVTError result = kvt_initialize();

// Create a table
std::string error;
uint64_t table_id;
result = kvt_create_table("my_table", "hash", table_id, error);

// Start transaction
uint64_t txn_id;
result = kvt_start_transaction(txn_id, error);

// Write operation
result = kvt_set(txn_id, table_id, "key1", "value1", error);

// Read operation
std::string value;
result = kvt_get(txn_id, table_id, "key1", value, error);

// Commit transaction
result = kvt_commit_transaction(txn_id, error);

// Cleanup
kvt_shutdown();
```

### Graph Operations (Through KVTStorageClient)
```cpp
// Using KVTStorageClient for NebulaGraph operations
KVTStorageClient client(nullptr, nullptr);

// Add vertices
std::vector<cpp2::NewVertex> vertices;
cpp2::NewVertex vertex;
vertex.id_ref() = "vertex_001";
// ... set tags and properties
vertices.push_back(vertex);

auto future = client.addVertices(spaceId, vertices, {}, true, requestParams);
auto result = std::move(future).get();

// Add edges
std::vector<cpp2::NewEdge> edges;
// ... configure edges
auto edgeFuture = client.addEdges(spaceId, edges, {}, true, requestParams);
```

**Note**: Direct KVT scan operations are not yet implemented. Use KVTStorageClient for graph operations.

## Development Status

This is an active development project. Key files:
- Interface: `kvt/kvt_inc.h`
- Memory Implementation: `kvt/kvt_mem.cpp`
- Storage Client: `kvt/KVTStorageClient.cpp`
- Tests: `kvt/test/*.cpp`
- Quick Test: `kvt/quick_test.sh`

For the latest status, run:
```bash
cd src/clients/storage/kvt
./validate_kvt.sh
```