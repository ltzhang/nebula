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

mkdir -p data/meta data/storage logs

# Set permissions
chmod -R 755 data logs

### Step 3: Start Services

# Start Meta Service first
/home/lintaoz/nebula/build/bin/nebula-metad --flagfile=conf/nebula-metad.conf --daemonize=true

# Wait for Meta Service to be ready
sleep 5

# Start Storage Service (skip if using KVT-only mode)
/home/lintaoz/nebula/build/bin/nebula-storaged --flagfile=conf/nebula-storaged.conf --daemonize=true

# Wait for Storage Service
sleep 5

# Start Graph Service
/home/lintaoz/build/bin/nebula-graphd --flagfile=conf/nebula-graphd.conf --daemonize=true

# Verify all services are running
ps aux | grep nebula

# Connect to Graph Service
/home/lintaoz/bin/nebula-console -addr 127.0.0.1 -port 9669

