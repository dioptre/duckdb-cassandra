#include "cassandra_types.hpp"
#include "duckdb/common/exception.hpp"
#include <iostream>

namespace duckdb {
namespace cassandra {

// Static type mapping from Cassandra CassValueType to DuckDB LogicalType
const std::unordered_map<CassValueType, LogicalType> CassandraTypeMapper::type_map_ = {
    // Basic types
    {CASS_VALUE_TYPE_ASCII, LogicalType::VARCHAR},
    {CASS_VALUE_TYPE_BIGINT, LogicalType::BIGINT},
    {CASS_VALUE_TYPE_BLOB, LogicalType::BLOB},
    {CASS_VALUE_TYPE_BOOLEAN, LogicalType::BOOLEAN},
    {CASS_VALUE_TYPE_COUNTER, LogicalType::BIGINT},
    {CASS_VALUE_TYPE_DECIMAL, LogicalType::DECIMAL(38, 10)}, // Default precision/scale
    {CASS_VALUE_TYPE_DOUBLE, LogicalType::DOUBLE},
    {CASS_VALUE_TYPE_FLOAT, LogicalType::FLOAT},
    {CASS_VALUE_TYPE_INT, LogicalType::INTEGER},
    {CASS_VALUE_TYPE_TEXT, LogicalType::VARCHAR},
    {CASS_VALUE_TYPE_TIMESTAMP, LogicalType::TIMESTAMP_TZ},
    {CASS_VALUE_TYPE_UUID, LogicalType::UUID},
    {CASS_VALUE_TYPE_VARCHAR, LogicalType::VARCHAR},
    {CASS_VALUE_TYPE_VARINT, LogicalType::HUGEINT},
    {CASS_VALUE_TYPE_TIMEUUID, LogicalType::UUID},
    {CASS_VALUE_TYPE_INET, LogicalType::VARCHAR}, // IP addresses as strings
    {CASS_VALUE_TYPE_DATE, LogicalType::DATE},
    {CASS_VALUE_TYPE_TIME, LogicalType::TIME},
    {CASS_VALUE_TYPE_SMALL_INT, LogicalType::SMALLINT},
    {CASS_VALUE_TYPE_TINY_INT, LogicalType::TINYINT},
    {CASS_VALUE_TYPE_DURATION, LogicalType::INTERVAL}, // Duration as interval
    
    // Collection types - map to JSON
    {CASS_VALUE_TYPE_LIST, LogicalType::JSON()},
    {CASS_VALUE_TYPE_MAP, LogicalType::JSON()},
    {CASS_VALUE_TYPE_SET, LogicalType::JSON()},
    {CASS_VALUE_TYPE_UDT, LogicalType::JSON()}, // User-defined types as JSON
    {CASS_VALUE_TYPE_TUPLE, LogicalType::JSON()}, // Tuples as JSON
    
    // Unknown/custom types
    {CASS_VALUE_TYPE_CUSTOM, LogicalType::VARCHAR},
    {CASS_VALUE_TYPE_UNKNOWN, LogicalType::VARCHAR}
};

// Static type mapping from Cassandra type strings to DuckDB LogicalType
const std::unordered_map<std::string, LogicalType> CassandraTypeMapper::type_string_map_ = {
    // Basic types
    {"ascii", LogicalType::VARCHAR},
    {"bigint", LogicalType::BIGINT},
    {"blob", LogicalType::BLOB},
    {"boolean", LogicalType::BOOLEAN},
    {"counter", LogicalType::BIGINT},
    {"decimal", LogicalType::DECIMAL(38, 10)},
    {"double", LogicalType::DOUBLE},
    {"float", LogicalType::FLOAT},
    {"int", LogicalType::INTEGER},
    {"text", LogicalType::VARCHAR},
    {"timestamp", LogicalType::TIMESTAMP_TZ},
    {"uuid", LogicalType::UUID},
    {"varchar", LogicalType::VARCHAR},
    {"varint", LogicalType::HUGEINT},
    {"timeuuid", LogicalType::UUID},
    {"inet", LogicalType::VARCHAR},
    {"date", LogicalType::DATE},
    {"time", LogicalType::TIME},
    {"smallint", LogicalType::SMALLINT},
    {"tinyint", LogicalType::TINYINT},
    {"duration", LogicalType::INTERVAL},
    
    // Collection types - JSON support
    {"list", LogicalType::JSON()},
    {"set", LogicalType::JSON()},
    {"map", LogicalType::JSON()},
    {"tuple", LogicalType::JSON()},
    {"frozen", LogicalType::JSON()} // Frozen collections
};

LogicalType CassandraTypeMapper::CassValueTypeToDuckDBType(CassValueType cass_type) {
    auto it = type_map_.find(cass_type);
    if (it != type_map_.end()) {
        return it->second;
    }
    
    std::cerr << "Warning: Unknown Cassandra type " << static_cast<int>(cass_type) 
              << ", mapping to VARCHAR" << std::endl;
    return LogicalType::VARCHAR;
}

LogicalType CassandraTypeMapper::CassandraTypeStringToDuckDBType(const std::string& type_str) {
    // Handle parameterized types (e.g., "list<text>", "map<text,int>")
    std::string base_type = type_str;
    size_t bracket_pos = type_str.find('<');
    if (bracket_pos != std::string::npos) {
        base_type = type_str.substr(0, bracket_pos);
    }
    
    auto it = type_string_map_.find(base_type);
    if (it != type_string_map_.end()) {
        return it->second;
    }
    
    std::cerr << "Warning: Unknown Cassandra type string '" << type_str 
              << "', mapping to VARCHAR" << std::endl;
    return LogicalType::VARCHAR;
}

Value CassandraTypeMapper::CassValueToDuckDBValue(const CassValue* value, const LogicalType& target_type) {
    if (cass_value_is_null(value)) {
        return Value(target_type);
    }
    
    CassValueType cass_type = cass_value_type(value);
    
    switch (cass_type) {
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        case CASS_VALUE_TYPE_VARCHAR: {
            const char* str_val;
            size_t str_len;
            cass_value_get_string(value, &str_val, &str_len);
            return Value(std::string(str_val, str_len));
        }
        
        case CASS_VALUE_TYPE_BIGINT:
        case CASS_VALUE_TYPE_COUNTER: {
            cass_int64_t int_val;
            cass_value_get_int64(value, &int_val);
            return Value::BIGINT(int_val);
        }
        
        case CASS_VALUE_TYPE_INT: {
            cass_int32_t int_val;
            cass_value_get_int32(value, &int_val);
            return Value::INTEGER(int_val);
        }
        
        case CASS_VALUE_TYPE_SMALL_INT: {
            cass_int16_t int_val;
            cass_value_get_int16(value, &int_val);
            return Value::SMALLINT(int_val);
        }
        
        case CASS_VALUE_TYPE_TINY_INT: {
            cass_int8_t int_val;
            cass_value_get_int8(value, &int_val);
            return Value::TINYINT(int_val);
        }
        
        case CASS_VALUE_TYPE_BOOLEAN: {
            cass_bool_t bool_val;
            cass_value_get_bool(value, &bool_val);
            return Value::BOOLEAN(bool_val == cass_true);
        }
        
        case CASS_VALUE_TYPE_FLOAT: {
            cass_float_t float_val;
            cass_value_get_float(value, &float_val);
            return Value::FLOAT(float_val);
        }
        
        case CASS_VALUE_TYPE_DOUBLE: {
            cass_double_t double_val;
            cass_value_get_double(value, &double_val);
            return Value::DOUBLE(double_val);
        }
        
        case CASS_VALUE_TYPE_TIMESTAMP: {
            cass_int64_t timestamp_ms;
            cass_value_get_int64(value, &timestamp_ms);
            // Convert milliseconds since epoch to DuckDB timestamp
            auto timestamp_us = timestamp_ms * 1000; // Convert to microseconds
            return Value::TIMESTAMP(timestamp_t(timestamp_us));
        }
        
        case CASS_VALUE_TYPE_DATE: {
            cass_uint32_t date_val;
            cass_value_get_uint32(value, &date_val);
            // Cassandra date is days since epoch (1970-01-01)
            // DuckDB date is also days since epoch
            return Value::DATE(date_t(date_val));
        }
        
        case CASS_VALUE_TYPE_TIME: {
            cass_int64_t time_val;
            cass_value_get_int64(value, &time_val);
            // Cassandra time is nanoseconds since midnight
            // Convert to DuckDB time (microseconds since midnight)
            auto time_us = time_val / 1000;
            return Value::TIME(dtime_t(time_us));
        }
        
        case CASS_VALUE_TYPE_UUID:
        case CASS_VALUE_TYPE_TIMEUUID: {
            CassUuid uuid_val;
            cass_value_get_uuid(value, &uuid_val);
            char uuid_str[CASS_UUID_STRING_LENGTH];
            cass_uuid_string(uuid_val, uuid_str);
            return Value::UUID(std::string(uuid_str));
        }
        
        case CASS_VALUE_TYPE_INET: {
            CassInet inet_val;
            cass_value_get_inet(value, &inet_val);
            char inet_str[CASS_INET_STRING_LENGTH];
            cass_inet_string(inet_val, inet_str);
            return Value(std::string(inet_str));
        }
        
        case CASS_VALUE_TYPE_BLOB: {
            const cass_byte_t* blob_val;
            size_t blob_size;
            cass_value_get_bytes(value, &blob_val, &blob_size);
            return Value::BLOB(blob_val, blob_size);
        }
        
        // Collection types - convert to JSON
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_SET:
        case CASS_VALUE_TYPE_MAP:
        case CASS_VALUE_TYPE_UDT:
        case CASS_VALUE_TYPE_TUPLE: {
            // TODO: Implement proper JSON serialization from CassValue
            // For now, return a JSON placeholder - this will be enhanced later
            return Value("{\"type\": \"" + GetCassandraTypeName(cass_type) + "\", \"data\": \"<collection>\"}");
        }
        
        default: {
            std::cerr << "Warning: Unsupported Cassandra value type " << static_cast<int>(cass_type)
                      << ", returning NULL" << std::endl;
            return Value(target_type);
        }
    }
}

std::string CassandraTypeMapper::GetCassandraTypeName(CassValueType cass_type) {
    switch (cass_type) {
        case CASS_VALUE_TYPE_ASCII: return "ascii";
        case CASS_VALUE_TYPE_BIGINT: return "bigint";
        case CASS_VALUE_TYPE_BLOB: return "blob";
        case CASS_VALUE_TYPE_BOOLEAN: return "boolean";
        case CASS_VALUE_TYPE_COUNTER: return "counter";
        case CASS_VALUE_TYPE_DECIMAL: return "decimal";
        case CASS_VALUE_TYPE_DOUBLE: return "double";
        case CASS_VALUE_TYPE_FLOAT: return "float";
        case CASS_VALUE_TYPE_INT: return "int";
        case CASS_VALUE_TYPE_TEXT: return "text";
        case CASS_VALUE_TYPE_TIMESTAMP: return "timestamp";
        case CASS_VALUE_TYPE_UUID: return "uuid";
        case CASS_VALUE_TYPE_VARCHAR: return "varchar";
        case CASS_VALUE_TYPE_VARINT: return "varint";
        case CASS_VALUE_TYPE_TIMEUUID: return "timeuuid";
        case CASS_VALUE_TYPE_INET: return "inet";
        case CASS_VALUE_TYPE_DATE: return "date";
        case CASS_VALUE_TYPE_TIME: return "time";
        case CASS_VALUE_TYPE_SMALL_INT: return "smallint";
        case CASS_VALUE_TYPE_TINY_INT: return "tinyint";
        case CASS_VALUE_TYPE_DURATION: return "duration";
        case CASS_VALUE_TYPE_LIST: return "list";
        case CASS_VALUE_TYPE_MAP: return "map";
        case CASS_VALUE_TYPE_SET: return "set";
        case CASS_VALUE_TYPE_UDT: return "udt";
        case CASS_VALUE_TYPE_TUPLE: return "tuple";
        default: return "unknown";
    }
}

} // namespace cassandra
} // namespace duckdb