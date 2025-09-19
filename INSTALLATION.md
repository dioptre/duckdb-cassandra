# DuckDB Cassandra Extension - Installation Guide

## Prerequisites

1. **System Dependencies**:
   ```bash
   # macOS
   brew install libuv cassandra-cpp-driver
   
   # Ubuntu/Debian  
   apt-get install libuv1-dev libssl-dev zlib1g-dev
   
   # CentOS/RHEL
   yum install libuv-devel openssl-devel zlib-devel
   ```

2. **Cassandra Cluster**:
   - Local Cassandra instance running on `127.0.0.1:9042`
   - OR remote cluster with accessible contact points
   - Valid keyspace and tables for testing

## Building the Extension

### Option 1: Bulletproof Build (Recommended)
The extension includes a prebuild step that automatically builds the DataStax C++ driver statically:

```bash
# Clone and build
git clone <repository-url>
cd duckdb-cassandra
git submodule update --init --recursive
make
```

### Option 2: Quick Development Build  
If you have system dependencies installed:

```bash
make clean
make
```

## Build Process

The build process includes:

1. **DataStax Driver Prebuild** (1-2 minutes):
   - Automatically builds `libcassandra_static.a` 
   - Installs headers to `build/release/datastax-install/`
   - Zero external dependencies required

2. **DuckDB Extension Build** (5-10 minutes):
   - Links against static DataStax library
   - Creates `cassandra.duckdb_extension` loadable module
   - Includes comprehensive type mapping and query execution

## Installation

After successful build:

```bash
# The extension binary will be at:
./build/release/extension/cassandra/cassandra.duckdb_extension

# The DuckDB binary with extension pre-loaded:
./build/release/duckdb
```

## Verification

Test the extension installation:

```sql
-- Start DuckDB with the built binary
./build/release/duckdb

-- Load the extension
LOAD 'cassandra';

-- Test connection
ATTACH 'host=127.0.0.1 port=9042 keyspace=sfpla' AS cass (TYPE cassandra);

-- List tables  
SHOW TABLES FROM cass.sfpla;
```

## Troubleshooting

### Build Issues

1. **"libuv not found"**: 
   ```bash
   brew install libuv  # macOS
   apt-get install libuv1-dev  # Ubuntu
   ```

2. **"DataStax driver build failed"**:
   - Ensure OpenSSL and zlib are installed
   - Check CMake version >= 3.5

3. **"Extension compilation errors"**:
   - Verify all source files are present
   - Check include paths in CMakeLists.txt

### Runtime Issues

1. **"Cannot connect to Cassandra"**:
   - Verify Cassandra is running: `cqlsh localhost`
   - Check contact points and port configuration
   - Verify authentication credentials

2. **"Keyspace not found"**:
   - List available keyspaces: `DESCRIBE KEYSPACES;` in cqlsh
   - Ensure proper permissions for keyspace access

## Next Steps

Once built successfully, proceed to the [Usage Guide](demo_extension.sql) for complete examples.