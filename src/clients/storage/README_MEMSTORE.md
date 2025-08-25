# MemStore Build Flag Usage

## Overview

The `USE_MEMSTORE` build flag allows you to switch the StorageClient implementation from the distributed Thrift-based storage to a simple in-memory storage using `std::map`.

## Build Configuration

### Enable MemStore (Development/Testing)

```bash
mkdir build && cd build
cmake -DUSE_MEMSTORE=ON -DCMAKE_BUILD_TYPE=Debug -DENABLE_TESTING=ON ..
make -j$(nproc)
```

### Disable MemStore (Production)

```bash
mkdir build && cd build  
cmake -DUSE_MEMSTORE=OFF -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

## Implementation Details

When `USE_MEMSTORE=ON`:
- `StorageClient` becomes an alias for `MemStorageClient`
- All graph layer operations use the in-memory storage
- Data is stored in `std::map<std::string, std::string>`
- Thread safety provided by global mutex
- Supports cursor-based scanning

When `USE_MEMSTORE=OFF` (default):
- `StorageClient` becomes an alias for `OrigStorageClient` 
- Uses the original Thrift-based distributed storage
- Full production functionality

## Key Generation

The MemStore uses these key patterns:
- **Vertices**: `v:{spaceId}:{vid}:{tagId}`
- **Edges**: `e:{spaceId}:{src}:{type}:{rank}:{dst}`

## Testing

```bash
# Build with MemStore enabled
cmake -DUSE_MEMSTORE=ON -DENABLE_TESTING=ON ..
make storage_client_switch_test

# Run the test
./bin/storage_client_switch_test
```

## Usage in Graph Layer

No changes needed in graph layer code! The conditional typedef ensures:

```cpp
// This works with both implementations
auto* storageClient = qctx->getStorageClient();
auto response = storageClient->addVertices(param, vertices, propNames, false, false);
```

## Benefits

- **Development**: Fast iteration without needing full storage cluster
- **Testing**: Simplified test environments
- **Debugging**: Easy to inspect and manipulate data
- **CI/CD**: Faster test runs in continuous integration

## Limitations

- **MemStore only**: No persistence, no distribution, no advanced storage features
- **Simplified implementation**: Some complex operations may return stub responses
- **Memory only**: Data lost on process restart