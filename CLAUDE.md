# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NebulaGraph is a distributed, high-performance graph database built with C++. It consists of three main services:
- **Graph Service**: Query engine handling client connections and query processing
- **Storage Service**: Distributed storage engine managing graph data 
- **Meta Service**: Metadata management and cluster coordination

## Build System

NebulaGraph uses CMake as its build system. The project requires C++17 and has extensive third-party dependencies.

### Common Build Commands

```bash
# Install third-party dependencies
./third-party/install-third-party.sh --prefix=/opt/vesoft/third-party

# Build the project
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# Build specific components
make nebula-graphd    # Graph service daemon
make nebula-storaged  # Storage service daemon  
make nebula-metad     # Meta service daemon
```

### Development Build Options

```bash
# Debug build with testing enabled
cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_TESTING=ON ..

# Build with sanitizers for development
cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_ASAN=ON ..

# Build with native optimizations
cmake -DENABLE_NATIVE=ON ..
```

### Code Formatting

The project uses clang-format-10 for consistent code formatting:

```bash
# Format all source files
make clang-format

# Check formatting without changes
git diff -U0 --no-color HEAD~1 | clang-format-diff-10 -p1
```

## Testing

The project has comprehensive testing infrastructure split between C++ unit tests and Python integration tests.

### Running Tests

```bash
# C++ unit tests (from build directory)
ctest -j$(nproc)
# Run specific test by name
ctest -R "TestName" -V

# Python integration tests
cd tests
make init-all  # Install test dependencies
make up        # Start test cluster  
make test      # Run pytest cases
make tck       # Run TCK (Technology Compatibility Kit) cases
make down      # Stop test cluster

# Run specific test categories
pytest -k "match" -m "not skip" .
pytest --address="127.0.0.1:9669" -m "not skip" .

# Test cluster management
make kill      # Kill any running nebula processes
make ps        # Show running nebula processes
```

### Test Structure

- **C++ tests**: Located alongside source files in `test/` subdirectories
- **Python tests**: Located in `tests/` directory, organized by feature
- **TCK tests**: Gherkin-based feature files in `tests/tck/features/`

## Architecture

### Directory Structure

- `src/common/`: Shared utilities, data types, and infrastructure
- `src/graph/`: Graph service implementation (query processing, validation, execution)
- `src/storage/`: Storage service implementation (data storage, indexing, transactions)
- `src/meta/`: Meta service implementation (cluster management, schema)
- `src/interface/`: Thrift service definitions
- `src/clients/`: Client libraries for interacting with services
- `src/kvstore/`: Distributed key-value storage foundation with Raft consensus
- `src/parser/`: SQL-like query language parser and AST

### Key Components

**Graph Service** (`src/graph/`):
- `validator/`: Query validation and semantic analysis
- `planner/`: Query planning and optimization  
- `executor/`: Query execution engine
- `optimizer/`: Cost-based query optimization

**Storage Service** (`src/storage/`):
- `exec/`: Storage execution nodes and query processing
- `index/`: Secondary indexing support
- `transaction/`: ACID transaction processing
- `admin/`: Administrative operations (backup, compaction, etc.)

**Common Infrastructure** (`src/common/`):
- `datatypes/`: Graph data types (Vertex, Edge, Path, etc.)
- `expression/`: Expression evaluation framework
- `function/`: Built-in and UDF function management

### Service Communication

Services communicate via Apache Thrift RPC. Interface definitions are in `src/interface/*.thrift`:
- `graph.thrift`: Client-to-graph service interface
- `storage.thrift`: Graph-to-storage service interface  
- `meta.thrift`: Metadata service interface
- `raftex.thrift`: Raft consensus protocol interface

## Development Workflow

### Adding Features

1. **Schema changes**: Update thrift interfaces if needed
2. **Validation**: Add validators in `src/graph/validator/`  
3. **Planning**: Add planners in `src/graph/planner/`
4. **Execution**: Add executors in `src/graph/executor/` or `src/storage/`
5. **Testing**: Add both unit tests and integration tests

### Common Development Tasks

```bash
# Build and run specific service
./bin/nebula-graphd --flagfile=conf/nebula-graphd.conf.default
./bin/nebula-storaged --flagfile=conf/nebula-storaged.conf.default  
./bin/nebula-metad --flagfile=conf/nebula-metad.conf.default

# Debug with specific test data  
cd tests && python3 nebula-test-run.py --debug
```

### Working with Thrift Interfaces

When modifying service interfaces, regenerate thrift bindings:

```bash
# Thrift definitions are in src/interface/*.thrift
# Changes require rebuilding to regenerate C++ bindings
make clean && make -j$(nproc)
```

### Memory and Performance Analysis

The codebase includes extensive benchmarking and memory analysis tools:

```bash
# Build with sanitizers for memory debugging
cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_ASAN=ON ..

# Run performance benchmarks (various benchmark files in test directories)
# Example: src/storage/test/StorageDAGBenchmark.cpp
```

## Performance Considerations

- The codebase is heavily optimized for performance with extensive use of move semantics and memory pools
- Expression evaluation is optimized with visitor patterns and caching
- Storage uses RocksDB with custom compaction filters and merge operators
- Query execution uses vectorized processing where possible

## Dependencies

Major third-party dependencies include:
- RocksDB: Persistent storage engine
- Apache Thrift: RPC framework
- Folly: Facebook's C++ library collection  
- Boost: C++ utilities
- Google Test/Benchmark: Testing frameworks
- Flex/Bison: Parser generation