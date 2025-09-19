#pragma once

#include "duckdb.hpp"
#include <cassandra.h>
#include <unordered_map>
#include <string>

namespace duckdb {
namespace cassandra {

// Cassandra to DuckDB type mapping
class CassandraTypeMapper {
public:
    // Convert Cassandra CassValueType to DuckDB LogicalType
    static LogicalType CassValueTypeToDuckDBType(CassValueType cass_type);
    
    // Convert Cassandra type string to DuckDB LogicalType
    static LogicalType CassandraTypeStringToDuckDBType(const std::string& type_str);
    
    // Convert CassValue to DuckDB Value
    static Value CassValueToDuckDBValue(const CassValue* value, const LogicalType& target_type);
    
    // Get type name for debugging
    static std::string GetCassandraTypeName(CassValueType cass_type);
    
private:
    static const std::unordered_map<CassValueType, LogicalType> type_map_;
    static const std::unordered_map<std::string, LogicalType> type_string_map_;
};

// Column information from Cassandra schema
struct CassandraColumnInfo {
    std::string column_name;
    std::string type;
    std::string kind; // partition_key, clustering, regular, static
    int position;
    
    LogicalType GetDuckDBType() const {
        return CassandraTypeMapper::CassandraTypeStringToDuckDBType(type);
    }
};

} // namespace cassandra
} // namespace duckdb