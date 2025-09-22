# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

## Testing Astra connection

```
g++ -I../build/release/datastax-install/include -L../build/release/datastax-install/lib -L/opt/homebrew/lib astra_connection_test.cpp -lcassandra_static -luv -lssl -lcrypto -lz -o
      astra_test && ./astra_test
```