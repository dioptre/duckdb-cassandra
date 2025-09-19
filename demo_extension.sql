-- DuckDB Cassandra Extension Demo
-- Note: This demonstrates what the extension will do once the build issues are resolved

-- Load the Cassandra extension
LOAD 'cassandra';

-- Attach the local Cassandra cluster with sfpla keyspace
ATTACH 'host=127.0.0.1 port=9042 keyspace=sfpla' AS cass (TYPE cassandra);

-- Show all tables in the attached Cassandra keyspace
SHOW TABLES FROM cass.sfpla;

-- Scan a table directly using cassandra_scan function
SELECT * FROM cassandra_scan('sfpla.events', 
    contact_points='127.0.0.1', 
    port=9042);

-- Execute a custom CQL query
SELECT * FROM cassandra_query('SELECT eid, created, ename, score FROM sfpla.events LIMIT 5');

-- Query with filters (when WHERE clause support is added)
-- SELECT * FROM cass.sfpla.events WHERE ename = 'test_event';

-- Show table schema information
DESCRIBE TABLE cass.sfpla.events;