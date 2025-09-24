# DuckDB Cassandra Extension

This extension allows [DuckDB](https://duckdb.org) to query data from Apache Cassandra clusters, it supports most C* databases - Cassandra, ScyllaDB and AstraDB (vectorless).

## Status

- ALPHA

## Getting Started

Look at https://github.com/dioptre/duckdb-cassandra/blob/main/test.sh for a comprehensive example. Certificates if required, need to be encoded and can be extractd from your bundle. See the test/ direcectory for examples of certificates that are included/required in the Secure Connect Bundle (SCB).

## Features

```sql
-- Install and load the extension
INSTALL 'cassandra';
LOAD 'cassandra';

-- Attach a Cassandra cluster
ATTACH 'host=127.0.0.1 port=9042 keyspace=my_keyspace' AS cassandra (TYPE cassandra);

-- Query tables
SELECT * FROM cassandra.my_keyspace.my_table;

-- Direct table scan
SELECT * FROM cassandra_scan('my_keyspace.my_table', 
    contact_points='127.0.0.1', 
    port=9042,
    filter='id > 1000');

-- Execute CQL queries
SELECT * FROM cassandra_query('SELECT * FROM my_keyspace.my_table WHERE token(id) > 0');
```

## Building

```bash
make
```

## License

[Apache License 2.0](LICENSE)
