#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
namespace cassandra {

class CassandraScanFunction : public TableFunction {
public:
    CassandraScanFunction();
};

class CassandraQueryFunction : public TableFunction {
public:
    CassandraQueryFunction();
};

} // namespace cassandra
} // namespace duckdb