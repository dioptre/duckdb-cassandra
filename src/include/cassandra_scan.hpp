#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "cassandra_utils.hpp"

// Forward declaration
namespace duckdb { namespace cassandra { class CassandraClient; } }

namespace duckdb {
namespace cassandra {

struct CassandraScanBindData : public TableFunctionData {
    CassandraTableRef table_ref;
    CassandraConfig config;
    string filter_condition;
};

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