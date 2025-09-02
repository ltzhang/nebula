# KVT Integration Plan for NebulaGraph

## Executive Summary
This document outlines the integration strategy for replacing NebulaGraph's distributed storage layer (based on KVStore/RocksDB) with the KVT (Key-Value Transactional) store. The integration will be implemented at the StorageClient level, allowing the graph compute layer to directly interact with KVT instead of making remote RPC calls to distributed storage nodes.

## Architecture Overview

### Current Architecture
```
Graph Service (nebula-graphd)
    ↓ (Query execution)
StorageClient 
    ↓ (RPC calls via Thrift)
Storage Service (nebula-storaged)
    ↓
KVStore (RocksDB)
```

### Target Architecture with KVT
```
Graph Service (nebula-graphd)
    ↓ (Query execution)
KVTStorageClient (NEW)
    ↓ (Direct function calls)
KVT Store (linked library)
```

## Phase 1: Foundation (Week 1-2)

### 1.1 Create KVTStorageClient Class
- **Location**: `src/clients/storage/kvt/KVTStorageClient.h/cpp`
- **Purpose**: Replace StorageClient with direct KVT operations
- **Key Features**:
  - Inherit from `StorageClientBase<ThriftClientType, ThriftClientManType<ThriftClientType>>`
  - Override all virtual methods from StorageClient
  - Implement mock RPC response structures for compatibility
  - Initialize KVT system in constructor

### 1.2 Implement Key Encoding Strategy
- **Location**: `src/clients/storage/kvt/KVTKeyEncoder.h/cpp`
- **Purpose**: Map NebulaGraph's data model to KVT key-value pairs
- **Key Mappings**:
  ```
  Vertex Key: "v:<spaceId>:<partId>:<vertexId>:<tagId>"
  Edge Key: "e:<spaceId>:<partId>:<srcId>:<edgeType>:<ranking>:<dstId>"
  Index Key: "i:<spaceId>:<indexId>:<indexValue>"
  ```
- **Value Format**: Encoded property data using NebulaGraph's existing codec

### 1.3 Transaction Management
- **Location**: `src/clients/storage/kvt/KVTTransactionManager.h/cpp`
- **Purpose**: Manage KVT transaction lifecycle
- **Features**:
  - Transaction pool management
  - Automatic commit/rollback on scope exit
  - Retry logic for transaction conflicts
  - Batch operation optimization

## Phase 2: Core Operations (Week 3-4)

### 2.1 Implement Graph Operations

#### getNeighbors()
```cpp
- Start KVT transaction
- Scan edge keys with prefix "e:<spaceId>:<partId>:<vertexId>:*"
- Filter by edge types and direction
- Fetch vertex properties if requested
- Apply filters and limits
- Commit transaction
- Return GetNeighborsResponse
```

#### getProps()
```cpp
- Start KVT transaction
- For each vertex/edge in input:
  - Construct appropriate key
  - Use kvt_get() to fetch value
  - Decode properties
- Batch operations using kvt_batch_execute()
- Commit transaction
- Return GetPropResponse
```

#### addVertices/addEdges
```cpp
- Start KVT transaction
- For each vertex/edge:
  - Encode properties to value
  - Construct appropriate key
  - Use kvt_set() to store
- Handle "IF NOT EXISTS" logic
- Update indexes if applicable
- Commit transaction
- Return ExecResponse
```

#### deleteVertices/deleteEdges
```cpp
- Start KVT transaction
- For each vertex/edge:
  - Construct appropriate key
  - Use kvt_del() to remove
  - Clean up related edges (for vertex deletion)
  - Update indexes
- Commit transaction
- Return ExecResponse
```

### 2.2 Data Serialization
- Reuse existing NebulaGraph codecs where possible
- Implement thin wrapper for KVT-specific encoding
- Handle schema versioning and evolution
- Optimize for common access patterns

## Phase 3: Integration (Week 5-6)

### 3.1 Modify QueryContext
- Add factory method to create appropriate storage client
- Configuration flag: `--enable_kvt_storage=true/false`
- Allow runtime switching between implementations
- Maintain backward compatibility

### 3.2 Update Build System
```cmake
# In src/clients/storage/CMakeLists.txt
if(ENABLE_KVT)
  nebula_add_library(
    kvt_storage_client_obj OBJECT
    kvt/KVTStorageClient.cpp
    kvt/KVTKeyEncoder.cpp
    kvt/KVTTransactionManager.cpp
  )
  
  # Link kvt_mem implementation
  target_link_libraries(kvt_storage_client_obj
    ${CMAKE_CURRENT_SOURCE_DIR}/kvt/kvt_mem.o
  )
endif()
```

### 3.3 Configuration Management
```cpp
DEFINE_bool(enable_kvt_storage, false, "Use KVT storage instead of distributed storage");
DEFINE_string(kvt_partition_method, "hash", "Partition method for KVT tables");
DEFINE_int32(kvt_transaction_retry_times, 3, "Retry times for transaction conflicts");
```

## Phase 4: Testing & Optimization (Week 7-8)

### 4.1 Testing Strategy
- **Unit Tests**: Test each KVT operation in isolation
- **Integration Tests**: Test with graph executor
- **Compatibility Tests**: Ensure same results as original storage
- **Performance Tests**: Benchmark against RocksDB implementation
- **Stress Tests**: Test transaction conflicts and concurrency

### 4.2 Performance Optimizations
- Implement connection pooling for KVT operations
- Add caching layer for frequently accessed data
- Optimize scan operations with better key design
- Batch multiple operations within single transaction
- Profile and optimize hot paths

### 4.3 Error Handling
- Map KVT error codes to NebulaGraph error codes
- Implement comprehensive logging
- Add metrics and monitoring
- Handle partial failures gracefully

## Phase 5: Advanced Features (Future)

### 5.1 Secondary Indexes
- Implement index tables in KVT
- Maintain index consistency during updates
- Optimize index scans

### 5.2 Distributed Transactions
- Extend to support cross-partition transactions
- Implement two-phase commit if needed
- Handle distributed deadlock detection

### 5.3 Schema Management
- Store schema information in KVT
- Handle schema evolution
- Implement online schema changes

## Implementation Checklist

### Immediate Tasks
- [ ] Create KVTStorageClient class structure
- [ ] Implement KVTKeyEncoder with basic encoding
- [ ] Create KVTTransactionManager
- [ ] Implement getProps() as first operation
- [ ] Add unit tests for basic operations
- [ ] Update CMakeLists.txt

### Short-term Tasks
- [ ] Implement all CRUD operations
- [ ] Add integration with graph executor
- [ ] Create performance benchmarks
- [ ] Add configuration flags
- [ ] Write documentation

### Long-term Tasks
- [ ] Optimize performance
- [ ] Add advanced features
- [ ] Implement monitoring and metrics
- [ ] Create migration tools
- [ ] Production hardening

## Risk Mitigation

### Technical Risks
1. **Performance Regression**: Mitigate with thorough benchmarking and optimization
2. **Data Consistency**: Implement comprehensive transaction management
3. **Compatibility Issues**: Maintain extensive test coverage

### Implementation Risks
1. **Scope Creep**: Focus on core functionality first
2. **Integration Complexity**: Use feature flags for gradual rollout
3. **Testing Coverage**: Implement tests in parallel with features

## Success Metrics
- All existing tests pass with KVT backend
- Performance within 10% of RocksDB implementation
- Zero data corruption issues
- Successful integration with graph layer
- Clean abstraction maintaining storage interface

## Appendix: KVT API Usage Examples

### Table Creation
```cpp
uint64_t table_id;
std::string error;
KVTError err = kvt_create_table("nebula_space_1", "hash", table_id, error);
```

### Transaction Example
```cpp
uint64_t tx_id;
kvt_start_transaction(tx_id, error);

// Perform operations
kvt_set(tx_id, table_id, key, value, error);
kvt_get(tx_id, table_id, key, value, error);

// Commit
kvt_commit_transaction(tx_id, error);
```

### Batch Operations
```cpp
KVTBatchOps ops;
ops.push_back({OP_SET, table_id, "key1", "value1"});
ops.push_back({OP_GET, table_id, "key2", ""});

KVTBatchResults results;
kvt_batch_execute(tx_id, ops, results, error);
```

## Document History
- v1.0 - Initial plan creation
- Last updated: [Current Date]
- Author: KVT Integration Team