# NebulaGraph with KVT - Complete Setup and Testing Guide

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Building NebulaGraph](#building-nebulagraph)
3. [Single Node Setup](#single-node-setup)
4. [Switching Storage Backends](#switching-storage-backends)
5. [Quick Functionality Tests](#quick-functionality-tests)
6. [Benchmarking](#benchmarking)
7. [Multi-Node Cluster Setup](#multi-node-cluster-setup)
8. [Stress Testing](#stress-testing)
9. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)

## Prerequisites

Before starting, ensure you have:
```bash
# Check system requirements
uname -a  # Linux required
gcc --version  # GCC 7.5+ or Clang 9.0+
cmake --version  # CMake 3.13+
make --version  # GNU Make 3.75+

# Install dependencies (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    curl \
    git \
    libtool \
    m4 \
    autoconf \
    automake \
    python3 \
    python3-pip

# Install dependencies (CentOS/RHEL)
sudo yum install -y \
    gcc-c++ \
    cmake \
    curl \
    git \
    libtool \
    m4 \
    autoconf \
    automake \
    python3 \
    python3-pip
```

## Building NebulaGraph

### Step 1: Clone and Navigate to Repository
```bash
# If you haven't cloned yet
git clone https://github.com/vesoft-inc/nebula.git
cd nebula

# Or if you're already in the project
cd /home/lintaoz/work/nebula
```

### Step 2: Build NebulaGraph
```bash
# Create build directory
mkdir -p build
cd build

# Configure (KVT is enabled by default)
# The kvt_mem implementation will be automatically compiled and linked
cmake -DCMAKE_BUILD_TYPE=Release ..

# To explicitly disable KVT (use traditional storage only):
# cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_KVT=OFF ..

# Build (use all CPU cores)
make -j$(nproc)

# Verify build success
ls -la bin/
# Should see: nebula-graphd, nebula-metad, nebula-storaged, etc.
# The KVT memory implementation (kvt_mem) is already linked into the executables
```

## Single Node Setup

### Step 1: Prepare Configuration Files
```bash
# From build directory
cd ..

# Create config directory
mkdir -p conf

# Generate default configurations
cat > conf/nebula-metad.conf << EOF
--port=9559
--ws_http_port=19559
--data_path=data/meta
--log_dir=logs
--v=0
--minloglevel=0
EOF

cat > conf/nebula-graphd.conf << EOF
--port=9669
--ws_http_port=19669
--meta_server_addrs=127.0.0.1:9559
--log_dir=logs
--v=0
--minloglevel=0
--enable_authorize=false
--enable_kvt=true
--kvt_memory_limit=4GB
EOF

cat > conf/nebula-storaged.conf << EOF
--port=9779
--ws_http_port=19779
--data_path=data/storage
--meta_server_addrs=127.0.0.1:9559
--log_dir=logs
--v=0
--minloglevel=0
EOF
```

### Step 2: Create Data Directories
```bash
# Create necessary directories
mkdir -p data/meta data/storage logs

# Set permissions
chmod -R 755 data logs
```

### Step 3: Start Services
```bash
# Start Meta Service first
./build/bin/nebula-metad --flagfile=conf/nebula-metad.conf --daemonize=true

# Wait for Meta Service to be ready
sleep 5

# Start Storage Service (skip if using KVT-only mode)
./build/bin/nebula-storaged --flagfile=conf/nebula-storaged.conf --daemonize=true

# Wait for Storage Service
sleep 5

# Start Graph Service
./build/bin/nebula-graphd --flagfile=conf/nebula-graphd.conf --daemonize=true

# Verify all services are running
ps aux | grep nebula
```

### Step 4: Connect with Console
```bash
# Build console if not already built
cd build
make nebula-console

# Connect to Graph Service
./bin/nebula-console -addr 127.0.0.1 -port 9669

# In the console, test basic commands:
# CREATE SPACE IF NOT EXISTS test(partition_num=10, replica_factor=1);
# USE test;
# CREATE TAG person(name string, age int);
# CREATE EDGE follow(degree int);
# EXIT
```

## Switching Storage Backends

### Traditional Storage Backend (RocksDB)
```bash
# Stop all services
./scripts/nebula.service stop all

# Modify Graph Service config
sed -i 's/--enable_kvt=true/--enable_kvt=false/g' conf/nebula-graphd.conf

# Restart services
./scripts/nebula.service start all
```

### KVT Storage Backend
```bash
# Stop all services
./scripts/nebula.service stop all

# Modify Graph Service config
sed -i 's/--enable_kvt=false/--enable_kvt=true/g' conf/nebula-graphd.conf

# For KVT-only mode (no Storage Service needed)
cat >> conf/nebula-graphd.conf << EOF
--kvt_standalone=true
EOF

# Restart only Meta and Graph services
./build/bin/nebula-metad --flagfile=conf/nebula-metad.conf --daemonize=true
sleep 5
./build/bin/nebula-graphd --flagfile=conf/nebula-graphd.conf --daemonize=true
```

## Quick Functionality Tests

### Step 1: Basic CRUD Operations
```bash
# Create test script
cat > test_basic.ngql << 'EOF'
-- Create space
CREATE SPACE IF NOT EXISTS social(partition_num=10, replica_factor=1);
USE social;

-- Create schema
CREATE TAG IF NOT EXISTS person(name string, age int, city string);
CREATE EDGE IF NOT EXISTS follow(degree int, since timestamp);

-- Wait for schema to propagate
:sleep 3

-- Insert vertices
INSERT VERTEX person(name, age, city) VALUES 
  "user1":("Alice", 25, "NYC"),
  "user2":("Bob", 30, "LA"),
  "user3":("Charlie", 35, "Chicago");

-- Insert edges
INSERT EDGE follow(degree, since) VALUES 
  "user1"->"user2":(95, 1609459200),
  "user2"->"user3":(85, 1609459200);

-- Query data
GO FROM "user1" OVER follow YIELD follow.degree;
FETCH PROP ON person "user1", "user2", "user3";

-- Update operations
UPDATE VERTEX "user1" SET person.age = 26;
UPDATE EDGE "user1"->"user2" OF follow SET degree = 100;

-- Verify updates
FETCH PROP ON person "user1";
FETCH PROP ON follow "user1"->"user2";

-- Delete operations
DELETE VERTEX "user3";
DELETE EDGE follow "user1"->"user2";
EOF

# Run test
./build/bin/nebula-console -addr 127.0.0.1 -port 9669 -e test_basic.ngql
```

### Step 2: Transaction Test (KVT-specific)
```bash
cat > test_transaction.ngql << 'EOF'
USE social;

-- Transaction test (if supported)
BEGIN;
INSERT VERTEX person(name, age, city) VALUES "user4":("David", 28, "Boston");
INSERT EDGE follow(degree, since) VALUES "user4"->"user1":(75, 1609459200);
COMMIT;

-- Verify transaction
GO FROM "user4" OVER follow YIELD follow.degree;
EOF

./build/bin/nebula-console -addr 127.0.0.1 -port 9669 -e test_transaction.ngql
```

## Benchmarking

### Step 1: Download LDBC Datasets
```bash
# Create benchmark directory
mkdir -p benchmarks
cd benchmarks

# Download LDBC Social Network Benchmark data
# SF1 (Scale Factor 1 - ~1GB)
wget https://github.com/ldbc/ldbc_snb_datagen_spark/raw/main/sf1.tar.gz
tar -xzf sf1.tar.gz

# For larger tests - SF10 (~10GB)
# wget https://github.com/ldbc/ldbc_snb_datagen_spark/raw/main/sf10.tar.gz
# tar -xzf sf10.tar.gz

cd ..
```

### Step 2: Import LDBC Data
```bash
# Create LDBC schema
cat > ldbc_schema.ngql << 'EOF'
CREATE SPACE IF NOT EXISTS ldbc(partition_num=100, replica_factor=1);
USE ldbc;

CREATE TAG Person(firstName string, lastName string, birthday timestamp, 
                  locationIP string, browserUsed string, gender string, 
                  creationDate timestamp);
CREATE TAG Company(name string, url string);
CREATE TAG University(name string, url string);

CREATE EDGE KNOWS(creationDate timestamp);
CREATE EDGE LIKES(creationDate timestamp);
CREATE EDGE HAS_INTEREST(creationDate timestamp);

:sleep 5
EOF

./build/bin/nebula-console -addr 127.0.0.1 -port 9669 -e ldbc_schema.ngql

# Import data using nebula-importer
# First, install nebula-importer
go get -u github.com/vesoft-inc/nebula-importer

# Create importer config
cat > ldbc_import.yaml << 'EOF'
version: v2
description: LDBC SF1 Import
settings:
  retry: 3
  concurrency: 10
  
clientSettings:
  address: 127.0.0.1:9669
  user: root
  password: nebula
  
files:
  - path: ./benchmarks/sf1/person.csv
    type: csv
    schema:
      space: ldbc
      type: vertex
      vertex:
        vid: 0
        tags:
          - name: Person
            props:
              firstName: 1
              lastName: 2
              birthday: 3
              locationIP: 4
              browserUsed: 5
EOF

# Run import
nebula-importer --config ldbc_import.yaml
```

### Step 3: Run Performance Benchmarks
```bash
# Create benchmark queries
cat > benchmark_queries.ngql << 'EOF'
USE ldbc;

-- Query 1: 1-hop traversal
PROFILE {
  GO FROM "person1" OVER KNOWS YIELD dst(edge) AS friend | 
  LIMIT 100;
}

-- Query 2: 2-hop traversal
PROFILE {
  GO 2 STEPS FROM "person1" OVER KNOWS YIELD dst(edge) AS friend | 
  LIMIT 100;
}

-- Query 3: Complex pattern matching
PROFILE {
  MATCH (p:Person)-[:KNOWS]->(friend:Person)-[:KNOWS]->(fof:Person)
  WHERE id(p) == "person1"
  RETURN p, friend, fof
  LIMIT 10;
}
EOF

# Run benchmark with traditional storage
echo "=== Traditional Storage Benchmark ===" > benchmark_results.txt
./scripts/nebula.service stop all
sed -i 's/--enable_kvt=true/--enable_kvt=false/g' conf/nebula-graphd.conf
./scripts/nebula.service start all
sleep 10
time ./build/bin/nebula-console -addr 127.0.0.1 -port 9669 -e benchmark_queries.ngql >> benchmark_results.txt 2>&1

# Run benchmark with KVT
echo "=== KVT Storage Benchmark ===" >> benchmark_results.txt
./scripts/nebula.service stop all
sed -i 's/--enable_kvt=false/--enable_kvt=true/g' conf/nebula-graphd.conf
./scripts/nebula.service start all
sleep 10
time ./build/bin/nebula-console -addr 127.0.0.1 -port 9669 -e benchmark_queries.ngql >> benchmark_results.txt 2>&1

# Compare results
echo "Benchmark complete. Check benchmark_results.txt for comparison"
```

### Step 4: Automated Performance Test
```bash
# Create performance test script
cat > perf_test.py << 'EOF'
#!/usr/bin/env python3
import time
import subprocess
import statistics

def run_query(query, iterations=100):
    times = []
    for _ in range(iterations):
        start = time.time()
        subprocess.run([
            "./build/bin/nebula-console",
            "-addr", "127.0.0.1",
            "-port", "9669",
            "-e", query
        ], capture_output=True)
        times.append(time.time() - start)
    return {
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) > 1 else 0,
        "min": min(times),
        "max": max(times)
    }

# Test queries
queries = {
    "simple_fetch": "USE ldbc; FETCH PROP ON Person 'person1';",
    "1hop": "USE ldbc; GO FROM 'person1' OVER KNOWS;",
    "2hop": "USE ldbc; GO 2 STEPS FROM 'person1' OVER KNOWS;",
}

for name, query in queries.items():
    print(f"\nTesting {name}:")
    results = run_query(query)
    for metric, value in results.items():
        print(f"  {metric}: {value:.3f}s")
EOF

chmod +x perf_test.py
python3 perf_test.py
```

## Multi-Node Cluster Setup

### Step 1: Prepare Three Nodes
```bash
# Assuming you have 3 machines: node1, node2, node3
# On each node, follow the build instructions first

# Node 1 (Meta Leader)
ssh node1
cd /home/lintaoz/work/nebula
cat > conf/nebula-metad-node1.conf << EOF
--port=9559
--ws_http_port=19559
--data_path=data/meta
--log_dir=logs
--meta_server_addrs=node1:9559,node2:9559,node3:9559
--local_ip=node1
EOF

# Node 2 (Meta Follower + Storage)
ssh node2
cd /home/lintaoz/work/nebula
cat > conf/nebula-metad-node2.conf << EOF
--port=9559
--ws_http_port=19559
--data_path=data/meta
--log_dir=logs
--meta_server_addrs=node1:9559,node2:9559,node3:9559
--local_ip=node2
EOF

cat > conf/nebula-storaged-node2.conf << EOF
--port=9779
--ws_http_port=19779
--data_path=data/storage
--meta_server_addrs=node1:9559,node2:9559,node3:9559
--local_ip=node2
EOF

# Node 3 (Meta Follower + Graph)
ssh node3
cd /home/lintaoz/work/nebula
cat > conf/nebula-metad-node3.conf << EOF
--port=9559
--ws_http_port=19559
--data_path=data/meta
--log_dir=logs
--meta_server_addrs=node1:9559,node2:9559,node3:9559
--local_ip=node3
EOF

cat > conf/nebula-graphd-node3.conf << EOF
--port=9669
--ws_http_port=19669
--meta_server_addrs=node1:9559,node2:9559,node3:9559
--log_dir=logs
--local_ip=node3
--enable_kvt=true
EOF
```

### Step 2: Start Cluster
```bash
# Start Meta Services first (all nodes simultaneously)
# On node1
./build/bin/nebula-metad --flagfile=conf/nebula-metad-node1.conf --daemonize=true

# On node2
./build/bin/nebula-metad --flagfile=conf/nebula-metad-node2.conf --daemonize=true

# On node3
./build/bin/nebula-metad --flagfile=conf/nebula-metad-node3.conf --daemonize=true

# Wait for Meta cluster to form
sleep 10

# Start Storage Service on node2
ssh node2
./build/bin/nebula-storaged --flagfile=conf/nebula-storaged-node2.conf --daemonize=true

# Start Graph Service on node3
ssh node3
./build/bin/nebula-graphd --flagfile=conf/nebula-graphd-node3.conf --daemonize=true

# Verify cluster status from any node
./build/bin/nebula-console -addr node3 -port 9669
# In console:
# SHOW HOSTS;
# SHOW HOSTS META;
# SHOW HOSTS STORAGE;
```

## Stress Testing

### Step 1: Install Stress Testing Tools
```bash
# Install Apache JMeter for load testing
wget https://downloads.apache.org/jmeter/binaries/apache-jmeter-5.5.tgz
tar -xzf apache-jmeter-5.5.tgz
export PATH=$PATH:$(pwd)/apache-jmeter-5.5/bin

# Install Python stress test dependencies
pip3 install nebula3-python concurrent-futures numpy
```

### Step 2: Create Stress Test Script
```bash
cat > stress_test.py << 'EOF'
#!/usr/bin/env python3
import concurrent.futures
import time
import random
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config

# Configuration
NEBULA_ADDRESS = "127.0.0.1"
NEBULA_PORT = 9669
NUM_THREADS = 50
NUM_OPERATIONS = 10000
SPACE_NAME = "stress_test"

# Initialize connection pool
config = Config()
config.max_connection_pool_size = 100
pool = ConnectionPool()
pool.init([(NEBULA_ADDRESS, NEBULA_PORT)], config)

def setup_space(session):
    """Create test space and schema"""
    session.execute(f'CREATE SPACE IF NOT EXISTS {SPACE_NAME}(partition_num=100, replica_factor=1)')
    time.sleep(3)
    session.execute(f'USE {SPACE_NAME}')
    session.execute('CREATE TAG IF NOT EXISTS entity(value int, data string)')
    session.execute('CREATE EDGE IF NOT EXISTS relation(weight double)')
    time.sleep(3)

def worker_insert(thread_id, num_ops):
    """Insert vertices and edges"""
    session = pool.get_session('root', 'nebula')
    session.execute(f'USE {SPACE_NAME}')
    
    start_time = time.time()
    for i in range(num_ops):
        vid = f"vertex_{thread_id}_{i}"
        value = random.randint(1, 1000)
        data = f"data_{thread_id}_{i}"
        
        # Insert vertex
        query = f'INSERT VERTEX entity(value, data) VALUES "{vid}":({value}, "{data}")'
        session.execute(query)
        
        # Insert random edge
        if i > 0:
            target_vid = f"vertex_{thread_id}_{random.randint(0, i-1)}"
            weight = random.random()
            edge_query = f'INSERT EDGE relation(weight) VALUES "{vid}"->"{target_vid}":({weight})'
            session.execute(edge_query)
    
    elapsed = time.time() - start_time
    ops_per_sec = num_ops / elapsed
    print(f"Thread {thread_id}: {num_ops} ops in {elapsed:.2f}s ({ops_per_sec:.2f} ops/s)")
    session.release()
    return ops_per_sec

def worker_read(thread_id, num_ops):
    """Read operations"""
    session = pool.get_session('root', 'nebula')
    session.execute(f'USE {SPACE_NAME}')
    
    start_time = time.time()
    for i in range(num_ops):
        vid = f"vertex_{random.randint(0, NUM_THREADS-1)}_{random.randint(0, 100)}"
        
        # Fetch vertex
        query = f'FETCH PROP ON entity "{vid}"'
        session.execute(query)
        
        # Traverse edges
        if i % 10 == 0:
            traverse_query = f'GO FROM "{vid}" OVER relation'
            session.execute(traverse_query)
    
    elapsed = time.time() - start_time
    ops_per_sec = num_ops / elapsed
    print(f"Thread {thread_id} (read): {num_ops} ops in {elapsed:.2f}s ({ops_per_sec:.2f} ops/s)")
    session.release()
    return ops_per_sec

def worker_mixed(thread_id, num_ops):
    """Mixed read/write operations"""
    session = pool.get_session('root', 'nebula')
    session.execute(f'USE {SPACE_NAME}')
    
    start_time = time.time()
    for i in range(num_ops):
        operation = random.choice(['insert', 'read', 'update', 'delete'])
        vid = f"vertex_{thread_id}_{i}"
        
        if operation == 'insert':
            query = f'INSERT VERTEX entity(value, data) VALUES "{vid}":({i}, "data_{i}")'
        elif operation == 'read':
            query = f'FETCH PROP ON entity "{vid}"'
        elif operation == 'update':
            query = f'UPDATE VERTEX "{vid}" SET entity.value = {i * 2}'
        else:  # delete
            query = f'DELETE VERTEX "{vid}"'
        
        try:
            session.execute(query)
        except:
            pass  # Handle failures gracefully in stress test
    
    elapsed = time.time() - start_time
    ops_per_sec = num_ops / elapsed
    print(f"Thread {thread_id} (mixed): {num_ops} ops in {elapsed:.2f}s ({ops_per_sec:.2f} ops/s)")
    session.release()
    return ops_per_sec

# Main execution
if __name__ == "__main__":
    # Setup
    session = pool.get_session('root', 'nebula')
    setup_space(session)
    session.release()
    
    print(f"\nStarting stress test with {NUM_THREADS} threads...")
    
    # Test 1: Write-heavy workload
    print("\n=== Write-Heavy Workload ===")
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(worker_insert, i, NUM_OPERATIONS // NUM_THREADS) 
                   for i in range(NUM_THREADS)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    print(f"Average write throughput: {sum(results)/len(results):.2f} ops/s")
    
    # Test 2: Read-heavy workload
    print("\n=== Read-Heavy Workload ===")
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(worker_read, i, NUM_OPERATIONS // NUM_THREADS) 
                   for i in range(NUM_THREADS)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    print(f"Average read throughput: {sum(results)/len(results):.2f} ops/s")
    
    # Test 3: Mixed workload
    print("\n=== Mixed Workload ===")
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(worker_mixed, i, NUM_OPERATIONS // NUM_THREADS) 
                   for i in range(NUM_THREADS)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    print(f"Average mixed throughput: {sum(results)/len(results):.2f} ops/s")
    
    pool.close()
EOF

chmod +x stress_test.py

# Run stress test
python3 stress_test.py
```

### Step 3: Long-Running Stability Test
```bash
cat > stability_test.sh << 'EOF'
#!/bin/bash

DURATION_HOURS=24
REPORT_INTERVAL=3600  # Report every hour

echo "Starting ${DURATION_HOURS}-hour stability test..."
START_TIME=$(date +%s)
END_TIME=$((START_TIME + DURATION_HOURS * 3600))

while [ $(date +%s) -lt $END_TIME ]; do
    CURRENT_TIME=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[$CURRENT_TIME] Running test iteration..."
    
    # Run stress test
    python3 stress_test.py > stress_$(date +%s).log 2>&1
    
    # Check service health
    ps aux | grep nebula | grep -v grep > /dev/null
    if [ $? -ne 0 ]; then
        echo "ERROR: Services crashed!"
        exit 1
    fi
    
    # Check memory usage
    echo "Memory usage:"
    ps aux | grep nebula | awk '{sum+=$6} END {print "Total: " sum/1024 " MB"}'
    
    # Sleep before next iteration
    sleep $REPORT_INTERVAL
done

echo "Stability test completed successfully!"
EOF

chmod +x stability_test.sh
nohup ./stability_test.sh > stability_test.log 2>&1 &
```

## Monitoring and Troubleshooting

### Monitor Service Health
```bash
# Check service status
cat > check_health.sh << 'EOF'
#!/bin/bash
echo "=== NebulaGraph Service Status ==="
ps aux | grep nebula | grep -v grep

echo -e "\n=== Port Status ==="
netstat -tlnp 2>/dev/null | grep -E "9559|9669|9779"

echo -e "\n=== Memory Usage ==="
ps aux | grep nebula | awk '{printf "%-20s %s MB\n", $11, $6/1024}'

echo -e "\n=== Log Errors (last 10) ==="
tail -n 10 logs/nebula-*.ERROR 2>/dev/null

echo -e "\n=== Cluster Status ==="
echo "SHOW HOSTS;" | ./build/bin/nebula-console -addr 127.0.0.1 -port 9669 2>/dev/null
EOF

chmod +x check_health.sh
./check_health.sh
```

### Debug Issues
```bash
# Enable debug logging
sed -i 's/--v=0/--v=4/g' conf/*.conf
./scripts/nebula.service restart all

# Check logs
tail -f logs/nebula-graphd.INFO
tail -f logs/nebula-metad.INFO
tail -f logs/nebula-storaged.INFO

# For KVT-specific debugging
export KVT_LOG_LEVEL=DEBUG
export KVT_TRACE_TXN=1
./scripts/nebula.service restart graphd
```

### Performance Monitoring
```bash
# Create monitoring script
cat > monitor_perf.sh << 'EOF'
#!/bin/bash
while true; do
    clear
    echo "=== NebulaGraph Performance Monitor ==="
    echo "Time: $(date)"
    
    # CPU usage
    echo -e "\n--- CPU Usage ---"
    top -bn1 | grep nebula | head -5
    
    # Memory usage
    echo -e "\n--- Memory Usage ---"
    free -h
    
    # Disk I/O
    echo -e "\n--- Disk I/O ---"
    iostat -x 1 2 | tail -n +4
    
    # Query metrics (if available)
    echo -e "\n--- Recent Queries ---"
    tail -n 5 logs/nebula-graphd.query.log 2>/dev/null
    
    sleep 5
done
EOF

chmod +x monitor_perf.sh
./monitor_perf.sh
```

## Common Issues and Solutions

### Issue 1: Service Won't Start
```bash
# Check if ports are in use
lsof -i :9559
lsof -i :9669
lsof -i :9779

# Kill existing processes if needed
pkill -f nebula

# Clean up data and restart
rm -rf data/* logs/*
./scripts/nebula.service start all
```

### Issue 2: Connection Refused
```bash
# Check firewall
sudo iptables -L | grep -E "9559|9669|9779"

# Add firewall rules if needed
sudo iptables -A INPUT -p tcp --dport 9559 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9669 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9779 -j ACCEPT
```

### Issue 3: Out of Memory (KVT)
```bash
# Increase memory limit
sed -i 's/--kvt_memory_limit=4GB/--kvt_memory_limit=8GB/g' conf/nebula-graphd.conf
./scripts/nebula.service restart graphd

# Or enable swap
sudo dd if=/dev/zero of=/swapfile bs=1G count=8
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

## Summary Commands Quick Reference

```bash
# Build (KVT enabled by default)
cd /home/lintaoz/work/nebula && mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && make -j$(nproc)

# Single node start
./build/bin/nebula-metad --flagfile=conf/nebula-metad.conf --daemonize=true
./build/bin/nebula-graphd --flagfile=conf/nebula-graphd.conf --daemonize=true

# Connect
./build/bin/nebula-console -addr 127.0.0.1 -port 9669

# Switch to KVT
sed -i 's/--enable_kvt=false/--enable_kvt=true/g' conf/nebula-graphd.conf

# Run tests
python3 stress_test.py

# Monitor
./check_health.sh
tail -f logs/nebula-graphd.INFO

# Stop all
./scripts/nebula.service stop all
```

This guide should help you get started with NebulaGraph and KVT testing quickly!