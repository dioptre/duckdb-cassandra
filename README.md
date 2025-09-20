# DuckDB Cassandra Extension

This extension allows [DuckDB](https://duckdb.org) to query data from Apache Cassandra clusters.

## Status

- ALPHA

## Planned Features

Once complete, this extension will provide:

```sql
-- Install and load the extension
INSTALL 'cassandra';
LOAD 'cassandra';

-- Attach a Cassandra cluster
ATTACH 'host=127.0.0.1 port=9042 keyspace=my_keyspace' AS cassandra (TYPE cassandra);

-- Query tables (NOT YET)
-- SELECT * FROM cassandra.my_keyspace.my_table;

-- Direct table scan
SELECT * FROM cassandra_scan('my_keyspace.my_table', 
    contact_points='127.0.0.1', 
    port=9042,
    filter='id > 1000');

-- Execute CQL queries (NOT YET)
-- SELECT * FROM cassandra_query('SELECT * FROM my_keyspace.my_table WHERE token(id) > 0');
```

## Building

Currently, the extension will not build successfully due to missing DataStax driver integration. Once the driver is integrated:

```bash
make
```

## License

[Apache License 2.0](LICENSE)