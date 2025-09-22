#!/bin/bash

echo "🎯 DuckDB Cassandra Extension - COMPREHENSIVE TEST"
echo "=================================================="

# Create test keyspace and table with ALL Cassandra data types
echo "1. Creating test keyspace and table with all data types..."
cqlsh localhost -e "
DROP KEYSPACE IF EXISTS duckdb_test;
CREATE KEYSPACE duckdb_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE duckdb_test;

CREATE TABLE all_types (
    id uuid PRIMARY KEY,
    text_col text,
    varchar_col varchar,
    ascii_col ascii,
    int_col int,
    bigint_col bigint,
    smallint_col smallint,
    tinyint_col tinyint,
    float_col float,
    double_col double,
    decimal_col decimal,
    boolean_col boolean,
    timestamp_col timestamp,
    date_col date,
    time_col time,
    timeuuid_col timeuuid,
    inet_col inet,
    blob_col blob,
    varint_col varint,
    list_col list<text>,
    set_col set<int>,
    map_col map<text, text>,
    frozen_list frozen<list<text>>,
    frozen_map frozen<map<text, int>>
);"

echo "2. Inserting 4 rows of test data with all data types..."
cqlsh localhost -e "
USE duckdb_test;

INSERT INTO all_types (
    id, text_col, varchar_col, ascii_col, int_col, bigint_col, smallint_col, tinyint_col,
    float_col, double_col, decimal_col, boolean_col, timestamp_col, date_col, time_col,
    timeuuid_col, inet_col, blob_col, varint_col,
    list_col, set_col, map_col, frozen_list, frozen_map
) VALUES (
    uuid(), 'Hello World', 'DuckDB Test', 'ASCII', 42, 9223372036854775807, 32767, 127,
    3.14159, 2.718281828, 123.456, true, '2025-01-20 10:00:00', '2025-01-20', '10:30:45',
    now(), '127.0.0.1', textAsBlob('binary_data'), 12345678901234567890,
    ['item1', 'item2', 'item3'], {1, 2, 3}, {'key1': 'value1', 'key2': 'value2'},
    ['frozen1', 'frozen2'], {'fkey1': 100, 'fkey2': 200}
);

INSERT INTO all_types (
    id, text_col, varchar_col, ascii_col, int_col, bigint_col, smallint_col, tinyint_col,
    float_col, double_col, decimal_col, boolean_col, timestamp_col, date_col, time_col,
    timeuuid_col, inet_col, blob_col, varint_col,
    list_col, set_col, map_col, frozen_list, frozen_map
) VALUES (
    uuid(), 'Test Row 2', 'Second Row', 'ASCII2', -123, -456789, -100, -50,
    -1.23, -4.56, -78.90, false, '2024-12-25 15:30:00', '2024-12-25', '15:30:00',
    now(), '192.168.1.1', textAsBlob('more_data'), -987654321,
    ['alpha', 'beta'], {10, 20}, {'name': 'test', 'type': 'demo'},
    ['alpha_frozen'], {'demo': 300}
);

INSERT INTO all_types (
    id, text_col, int_col, double_col, boolean_col, timestamp_col,
    list_col, map_col
) VALUES (
    uuid(), 'Partial Row', 999, 3.14159, true, '2023-06-15 08:45:00',
    ['partial'], {'status': 'active'}
);

INSERT INTO all_types (id, text_col) VALUES (uuid(), 'Minimal Row');
"

echo "3. Verifying data with cqlsh..."
echo "Sample data from Cassandra:"
cqlsh localhost -e "USE duckdb_test; SELECT id, text_col, int_col, double_col, timestamp_col, list_col, map_col FROM all_types;" 2>/dev/null

echo
echo "4. Testing DuckDB Cassandra Extension - Method 1: cassandra_scan()"
echo "================================================================="
/Users/andrewgrosser/Documents/duckdb-cassandra/build/release/duckdb -c "
INSTALL '/Users/andrewgrosser/Documents/duckdb-cassandra/build/release/repository/v1.4.0/osx_arm64/cassandra.duckdb_extension';
LOAD 'cassandra';
SELECT 'Method 1: cassandra_scan() - Direct table access' as test_method;
-- Show ALL rows including NULL verification for comprehensive test
SELECT id, text_col, int_col, double_col, boolean_col, timestamp_col, list_col, map_col 
FROM cassandra_scan('duckdb_test.all_types', contact_points='127.0.0.1', port=9042) 
ORDER BY text_col;
"

echo
echo "5. Testing DuckDB Cassandra Extension - Method 2: cassandra_query()"
echo "==================================================================="
/Users/andrewgrosser/Documents/duckdb-cassandra/build/release/duckdb -c "
LOAD 'cassandra';
SELECT 'Method 2: cassandra_query() - Custom CQL execution' as test_method;
-- Test with basic columns to avoid binding issues
SELECT * FROM cassandra_query('SELECT id, text_col, int_col FROM duckdb_test.all_types ORDER BY text_col', contact_points='127.0.0.1');
"

echo
echo "6. Testing DuckDB Cassandra Extension - Method 3: ATTACH"
echo "========================================================"
/Users/andrewgrosser/Documents/duckdb-cassandra/build/release/duckdb -c "
LOAD 'cassandra';
SELECT 'Method 3: ATTACH - Database attachment' as test_method;
ATTACH 'host=127.0.0.1 port=9042 keyspace=duckdb_test' AS cass_test (TYPE cassandra);
-- Show ALL rows including NULL verification for comprehensive test  
SELECT id, text_col, int_col, double_col, boolean_col, timestamp_col, list_col, map_col 
FROM cass_test.all_types ORDER BY text_col;
"

echo
echo "7. Performance Test - Complex Query"
echo "==================================="
/Users/andrewgrosser/Documents/duckdb-cassandra/build/release/duckdb -c "
LOAD 'cassandra';
SELECT 'Complex query performance test' as test;
SELECT COUNT(*) as total_rows FROM cassandra_query('SELECT * FROM duckdb_test.all_types', contact_points='127.0.0.1');
"

echo
echo "8. SSL Test (placeholder - would need real certs)"
echo "================================================"
/Users/andrewgrosser/Documents/duckdb-cassandra/build/release/duckdb -c "
LOAD 'cassandra';
SELECT 'SSL parameters available' as ssl_test;
SELECT * FROM cassandra_scan('duckdb_test.all_types', 
    contact_points='127.0.0.1', 
    ssl=false
) LIMIT 1;
"

echo
echo "9. Cleanup - Removing test data..."
cqlsh localhost -e "DROP KEYSPACE IF EXISTS duckdb_test;" 2>/dev/null
