# KVT Integration into NebulaGraph

## Overview

KVT (Key-Value Transactional store) is a transactional storage interface being integrated into NebulaGraph to replace the traditional distributed storage layer. Instead of the Graph Service making remote RPC calls to Storage Service nodes, KVT is embedded directly into the storage client, allowing the Graph Service to interact with a local transactional key-value store.

This integration provides ACID transactional capabilities, improved performance by eliminating network overhead, and a cleaner abstraction for storage operations.

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

### Unit Tests
```bash
# Run KVT-specific tests
cd build
./bin/kvt_test

# Run integration tests
./bin/storage_client_kvt_test
```

### Functional Tests
```bash
# Start test environment with KVT
cd tests
make kvt-up

# Run KVT integration tests
python3 -m pytest tests/kvt/

# Run standard tests with KVT backend
make test-with-kvt
```

### Performance Tests
```bash
# Benchmark KVT operations
./bin/kvt_benchmark --operations=1000000

# Compare with traditional storage
./scripts/compare_storage_perf.sh
```

## Configuration

### Enable KVT in Graph Service
```yaml
# conf/nebula-graphd.conf
--enable_kvt=true
--kvt_impl_path=/path/to/kvt_impl.so  # Optional: custom implementation
--kvt_memory_limit=8GB
--kvt_transaction_timeout=5000ms
```

### KVT-Specific Settings
```yaml
# Transaction settings
--kvt_isolation_level=SNAPSHOT  # SNAPSHOT, SERIALIZABLE
--kvt_max_concurrent_txn=1000
--kvt_conflict_retry_count=3

# Memory settings (for kvt_mem)
--kvt_mem_table_size=1GB
--kvt_mem_cache_size=256MB
```

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

## Optimization Opportunities

### 1. Batch Processing
- **Current**: Individual KVT operations per graph element
- **Optimization**: Batch multiple operations into single KVT calls
- **Expected Gain**: 30-50% reduction in overhead for bulk operations

### 2. Caching Layer
- **Current**: Direct KVT access for all reads
- **Optimization**: Add LRU cache for hot vertices/edges
- **Expected Gain**: 2-3x improvement for repeated queries

### 3. Parallel Execution
- **Current**: Sequential transaction execution
- **Optimization**: Parallel execution with conflict resolution
- **Expected Gain**: Near-linear scaling for non-conflicting workloads

### 4. Index Optimization
- **Current**: Secondary indexes as separate KVT entries
- **Optimization**: Composite keys for covering indexes
- **Expected Gain**: 50% reduction in index lookup latency

### 5. Memory Layout
- **Current**: Generic key-value storage
- **Optimization**: Graph-aware memory layout with locality optimization
- **Expected Gain**: Better cache utilization, 20-30% performance improvement

### 6. Compression
- **Current**: No compression
- **Optimization**: Selective compression for large property values
- **Expected Gain**: 40-60% memory reduction for property-heavy graphs

## Migration Guide

### Phase 1: Development Testing
1. Build with KVT enabled
2. Run existing tests with KVT backend
3. Verify correctness and performance

### Phase 2: Performance Validation
1. Load production-like datasets
2. Run benchmark queries
3. Compare metrics with traditional storage

### Phase 3: Production Rollout
1. Deploy in read-only replica first
2. Enable for specific query patterns
3. Gradual rollout with monitoring

## Limitations and Future Work

### Current Limitations
- Memory-based reference implementation (kvt_mem) not suitable for production
- No distributed transaction support across partitions
- Limited to single-node deployments

### Future Enhancements
1. **Production KVT Implementation**: Integration with RocksDB or other persistent stores
2. **Distributed Transactions**: Cross-partition transaction support
3. **Advanced Features**:
   - Time-travel queries
   - Multi-version schema evolution
   - Incremental backups
4. **Query Optimization**: KVT-aware query planning and execution

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
# Enable KVT debug logging
export KVT_LOG_LEVEL=DEBUG

# Trace transaction flow
export KVT_TRACE_TXN=1

# Monitor KVT metrics
./bin/kvt_monitor --port=9999
```

## API Examples

### Basic Usage
```cpp
// Initialize KVT
kvt_init();

// Simple transaction
kvt_txn_t* txn = kvt_begin();
kvt_put(txn, "key1", "value1");
kvt_put(txn, "key2", "value2");
kvt_commit(txn);

// Read operation
txn = kvt_begin();
char* value = kvt_get(txn, "key1");
kvt_commit(txn);

// Range scan
txn = kvt_begin();
kvt_iterator_t* iter = kvt_scan(txn, "key1", "key9");
while (kvt_iter_valid(iter)) {
    // Process key-value pair
    kvt_iter_next(iter);
}
kvt_commit(txn);
```

### Graph Operations
```cpp
// Insert vertex
kvt_txn_t* txn = kvt_begin();
string vertex_key = format_vertex_key(partition_id, vertex_id, tag_id);
kvt_put(txn, vertex_key, serialize_properties(properties));
kvt_commit(txn);

// Traverse edges
txn = kvt_begin();
string edge_prefix = format_edge_prefix(partition_id, src_vertex_id);
kvt_iterator_t* iter = kvt_scan_prefix(txn, edge_prefix);
while (kvt_iter_valid(iter)) {
    // Process outgoing edges
    kvt_iter_next(iter);
}
kvt_commit(txn);
```

## Contributing

When contributing to KVT integration:

1. Follow NebulaGraph coding standards
2. Add tests for new functionality
3. Update this documentation for API changes
4. Run performance benchmarks for optimization PRs
5. Ensure backward compatibility

## References

- [NebulaGraph Documentation](https://docs.nebula-graph.io/)
- [KVT Interface Specification](kvt/kvt_inc.h)
- [Storage Client Architecture](../README.md)
- [Transaction Design Document](docs/kvt_transactions.md)