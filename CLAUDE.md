# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

### Building NebulaGraph
```bash
# Create build directory
mkdir -p build && cd build

# Configure with CMake (Release build)
cmake -DCMAKE_BUILD_TYPE=Release ..

# Configure with Debug build
cmake -DCMAKE_BUILD_TYPE=Debug ..

# Build with multiple threads
make -j$(nproc)

# Build specific components
make nebula-graphd   # Graph service
make nebula-storaged  # Storage service  
make nebula-metad     # Meta service
make nebula-standalone # Standalone version
```

### Package Building
```bash
# Build packages (from project root)
./package/package.sh -v <version> -t Release
```

## Testing

### Running Tests
```bash
# From tests directory
cd tests

# Install test dependencies
make install-deps

# Start test cluster
make up

# Run all tests
make test-all

# Run specific test categories
make test        # Unit tests
make tck         # TCK compliance tests
make slow-query  # Slow query tests
make ldbc        # LDBC benchmark tests

# Run standalone tests
make standalone-up
make test-standalone-all

# Stop test cluster
make down
```

### Single Test Execution
```bash
# Run specific test file
python3 -m pytest tests/<test_file>.py

# Run with specific markers
python3 -m pytest -m "not skip" tests/

# Run last failed tests
make fail
```

## Development Commands

### Code Formatting
```bash
# Format C++ code
make clang-format

# Format test features (Gherkin)
cd tests && make fmt
```

### Service Management
```bash
# Using scripts
./scripts/nebula.service -c <config_file> start <service_name>
./scripts/nebula.service -c <config_file> stop <service_name>

# Service names: metad, storaged, graphd
```

## Architecture Overview

NebulaGraph is a distributed graph database with three main components:

### Core Services

1. **Graph Service (nebula-graphd)**: Query processing layer
   - Handles OpenCypher-compatible queries
   - Query parsing, validation, optimization, and execution
   - Session management and authentication
   - Located in `src/graph/`

2. **Storage Service (nebula-storaged)**: Data persistence layer
   - Manages graph data storage and retrieval
   - Handles partitioning and replication
   - Implements RocksDB-based storage engine
   - Located in `src/storage/`

3. **Meta Service (nebula-metad)**: Metadata management
   - Cluster topology and schema management
   - User and role management
   - Maintains consistency via RAFT consensus
   - Located in `src/meta/`

### Key Components

- **KVStore** (`src/kvstore/`): Key-value storage abstraction layer using RocksDB
- **Raft** (`src/kvstore/raftex/`): RAFT consensus protocol implementation for consistency
- **Parser** (`src/parser/`): OpenCypher query language parser
- **Optimizer** (`src/graph/optimizer/`): Query optimization engine
- **Executor** (`src/graph/executor/`): Query execution framework
- **Clients** (`src/clients/`): Client libraries for inter-service communication

### Data Flow

1. Client sends OpenCypher query to Graph Service
2. Graph Service parses, validates, and optimizes the query
3. Executor requests data from Storage Service via storage client
4. Storage Service retrieves data from RocksDB through KVStore
5. Results flow back through the stack to the client

### Inter-Service Communication

- Services communicate via Apache Thrift RPC
- Meta Client in each service maintains cluster metadata cache
- Storage Client handles data operations between Graph and Storage services

## KVT Integration Note

Current effort involves integrating KVT (Key-Value Transactional store) at `src/clients/storage/kvt/` as a replacement for the storage layer. The KVT interface (`kvt_inc.h`) provides ACID transactional capabilities with a memory reference implementation (`kvt_mem.cpp`).

- kvt is an interface to a key-value transactional store. The API is exposed as kvt_inc.h, you should regard kvt to have all the nice properties such as scalable, high performance, ACID, durable, and so on. Do not assume any weakness unless the interface does not support it. In that case, explicitly mention it in documents. 

- In the future, we will mostly work under the `src/clients/storage/` directory, so treat that as the root direcotry unless absolutely necessary (such as change build script, change execution configuration, build the project and so on). 

- The kvt_mem reference implements the interface with transaction support, but do not assume that all KVT stores have the weakness of the memory implementation. You can use kvt_mem by link `kvt_mem.o`, which can be produced with `g++ -c kvt_mem.cpp` command. 

- we integrate kvt into nebula graph (replacing the storage layer) by inserting the kvt store into the client code, so when the nebulag graph executor needs data, it goes through the client and directly interact with the linked kvt store instead of remotely call into the original distributed KVStore. 

