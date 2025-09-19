#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
namespace cassandra {

class CassandraAttachFunction : public TableFunction {
public:
    CassandraAttachFunction();
};

} // namespace cassandra
} // namespace duckdb