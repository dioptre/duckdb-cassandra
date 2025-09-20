#include "cassandra_scan.hpp"
#include "cassandra_client.hpp"
#include "cassandra_utils.hpp"
#include "cassandra_types.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {
namespace cassandra {

// CassandraScanBindData is now defined in cassandra_scan.hpp

struct CassandraScanGlobalState : public GlobalTableFunctionState {
    unique_ptr<CassandraClient> client;
    CassIterator* result_iterator;
    const CassResult* result;
    bool finished;
    
    CassandraScanGlobalState() : result_iterator(nullptr), result(nullptr), finished(false) {}
    
    ~CassandraScanGlobalState() {
        if (result_iterator) {
            cass_iterator_free(result_iterator);
        }
        if (result) {
            cass_result_free(result);
        }
    }
};

static unique_ptr<FunctionData> CassandraScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
    
    auto bind_data = make_uniq<CassandraScanBindData>();
    
    if (input.inputs.empty()) {
        throw BinderException("cassandra_scan requires a table name");
    }
    
    auto table_name = StringValue::Get(input.inputs[0]);
    
    // Parse table name - format: keyspace.table
    auto dot_pos = table_name.find('.');
    if (dot_pos != string::npos) {
        bind_data->table_ref.keyspace_name = table_name.substr(0, dot_pos);
        bind_data->table_ref.table_name = table_name.substr(dot_pos + 1);
    } else {
        throw BinderException("Table name must include keyspace: keyspace.table");
    }
    
    // Set default configuration
    bind_data->config = CassandraConfig();
    
    // Parse named parameters
    for (auto &kv : input.named_parameters) {
        auto lower_key = StringUtil::Lower(kv.first);
        if (lower_key == "contact_points" || lower_key == "host") {
            bind_data->config.contact_points = StringValue::Get(kv.second);
        } else if (lower_key == "port") {
            bind_data->config.port = IntegerValue::Get(kv.second);
        } else if (lower_key == "username") {
            bind_data->config.username = StringValue::Get(kv.second);
        } else if (lower_key == "password") {
            bind_data->config.password = StringValue::Get(kv.second);
        } else if (lower_key == "ssl" || lower_key == "use_ssl") {
            bind_data->config.use_ssl = BooleanValue::Get(kv.second);
        } else if (lower_key == "certfile") {
            bind_data->config.cert_file_hex = StringValue::Get(kv.second);
            bind_data->config.use_ssl = true;
        } else if (lower_key == "userkey") {
            bind_data->config.user_key_hex = StringValue::Get(kv.second);
            bind_data->config.use_ssl = true;
        } else if (lower_key == "usercert") {
            bind_data->config.user_cert_hex = StringValue::Get(kv.second);
            bind_data->config.use_ssl = true;
        }
    }
    
    // Get schema by doing a LIMIT 1 query and extracting metadata
    try {
        auto client = make_uniq<CassandraClient>(bind_data->config);
        string schema_query = "SELECT * FROM " + bind_data->table_ref.keyspace_name + "." + bind_data->table_ref.table_name + " LIMIT 1";
        
        auto session = client->GetSession();
        CassStatement* statement = cass_statement_new(schema_query.c_str(), 0);
        CassFuture* result_future = cass_session_execute(session, statement);
        
        if (cass_future_error_code(result_future) == CASS_OK) {
            const CassResult* result = cass_future_get_result(result_future);
            size_t column_count = cass_result_column_count(result);
            
            for (size_t i = 0; i < column_count; i++) {
                const char* column_name;
                size_t name_length;
                cass_result_column_name(result, i, &column_name, &name_length);
                CassValueType column_type = cass_result_column_type(result, i);
                
                string col_name(column_name, name_length);
                names.push_back(col_name);
                
                // Map types properly
                LogicalType duckdb_type;
                switch (column_type) {
                    case CASS_VALUE_TYPE_UUID:
                    case CASS_VALUE_TYPE_TIMEUUID:
                        duckdb_type = LogicalType::UUID;
                        break;
                    case CASS_VALUE_TYPE_TIMESTAMP:
                        duckdb_type = LogicalType::TIMESTAMP_TZ;
                        break;
                    case CASS_VALUE_TYPE_DOUBLE:
                        duckdb_type = LogicalType::DOUBLE;
                        break;
                    case CASS_VALUE_TYPE_INT:
                        duckdb_type = LogicalType::INTEGER;
                        break;
                    case CASS_VALUE_TYPE_BIGINT:
                        duckdb_type = LogicalType::BIGINT;
                        break;
                    default:
                        duckdb_type = LogicalType::VARCHAR;
                        break;
                }
                
                return_types.push_back(duckdb_type);
            }
            
            cass_result_free(result);
        }
        
        cass_future_free(result_future);
        cass_statement_free(statement);
        
    } catch (const std::exception& e) {
        names.push_back("error");
        return_types.push_back(LogicalType::VARCHAR);
    }
    
    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> CassandraScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto bind_data = input.bind_data->Cast<CassandraScanBindData>();
    auto result = make_uniq<CassandraScanGlobalState>();
    
    result->client = make_uniq<CassandraClient>(bind_data.config);
    
    // Execute the actual data query
    string query = "SELECT * FROM " + bind_data.table_ref.keyspace_name + "." + bind_data.table_ref.table_name;
    
    auto session = result->client->GetSession();
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassFuture* query_future = cass_session_execute(session, statement);
    
    if (cass_future_error_code(query_future) == CASS_OK) {
        result->result = cass_future_get_result(query_future);
        result->result_iterator = cass_iterator_from_result(result->result);
        result->finished = false;
    } else {
        result->finished = true;
    }
    
    cass_future_free(query_future);
    cass_statement_free(statement);
    
    return std::move(result);
}

static void CassandraScanExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &gstate = data.global_state->Cast<CassandraScanGlobalState>();
    
    if (gstate.finished || !gstate.result_iterator) {
        output.SetCardinality(0);
        return;
    }
    
    idx_t row_count = 0;
    const idx_t chunk_size = STANDARD_VECTOR_SIZE;
    
    while (row_count < chunk_size && cass_iterator_next(gstate.result_iterator)) {
        const CassRow* row = cass_iterator_get_row(gstate.result_iterator);
        size_t column_count = cass_result_column_count(gstate.result);
        
        for (size_t col_idx = 0; col_idx < column_count && col_idx < output.ColumnCount(); col_idx++) {
            const CassValue* value = cass_row_get_column(row, col_idx);
            auto &vector = output.data[col_idx];
            auto validity = FlatVector::Validity(vector);
            
            if (cass_value_is_null(value)) {
                validity.SetInvalid(row_count);
            } else {
                validity.SetValid(row_count);
                
                LogicalType expected_type = vector.GetType();
                CassValueType value_type = cass_value_type(value);
                
                switch (expected_type.id()) {
                    case LogicalTypeId::UUID: {
                        auto data_ptr = FlatVector::GetData<hugeint_t>(vector);
                        CassUuid uuid_val;
                        cass_value_get_uuid(value, &uuid_val);
                        char uuid_str[CASS_UUID_STRING_LENGTH];
                        cass_uuid_string(uuid_val, uuid_str);
                        hugeint_t uuid_hugeint;
                        if (UUID::FromCString(uuid_str, strlen(uuid_str), uuid_hugeint)) {
                            data_ptr[row_count] = uuid_hugeint;
                        } else {
                            validity.SetInvalid(row_count);
                        }
                        break;
                    }
                    case LogicalTypeId::TIMESTAMP_TZ: {
                        auto data_ptr = FlatVector::GetData<timestamp_t>(vector);
                        cass_int64_t timestamp_ms;
                        cass_value_get_int64(value, &timestamp_ms);
                        data_ptr[row_count] = timestamp_t(timestamp_ms * 1000);
                        break;
                    }
                    case LogicalTypeId::DOUBLE: {
                        auto data_ptr = FlatVector::GetData<double>(vector);
                        cass_double_t double_val;
                        cass_value_get_double(value, &double_val);
                        data_ptr[row_count] = double_val;
                        break;
                    }
                    case LogicalTypeId::INTEGER: {
                        auto data_ptr = FlatVector::GetData<int32_t>(vector);
                        cass_int32_t int_val;
                        cass_value_get_int32(value, &int_val);
                        data_ptr[row_count] = int_val;
                        break;
                    }
                    case LogicalTypeId::BIGINT: {
                        auto data_ptr = FlatVector::GetData<int64_t>(vector);
                        cass_int64_t bigint_val;
                        cass_value_get_int64(value, &bigint_val);
                        data_ptr[row_count] = bigint_val;
                        break;
                    }
                    case LogicalTypeId::VARCHAR:
                    default: {
                        auto data_ptr = FlatVector::GetData<string_t>(vector);
                        string string_val;
                        
                        switch (value_type) {
                            case CASS_VALUE_TYPE_ASCII:
                            case CASS_VALUE_TYPE_TEXT:
                            case CASS_VALUE_TYPE_VARCHAR: {
                                const char* str_val;
                                size_t str_len;
                                cass_value_get_string(value, &str_val, &str_len);
                                string_val = string(str_val, str_len);
                                break;
                            }
                            case CASS_VALUE_TYPE_UUID:
                            case CASS_VALUE_TYPE_TIMEUUID: {
                                CassUuid uuid_val;
                                cass_value_get_uuid(value, &uuid_val);
                                char uuid_str[CASS_UUID_STRING_LENGTH];
                                cass_uuid_string(uuid_val, uuid_str);
                                string_val = string(uuid_str);
                                break;
                            }
                            case CASS_VALUE_TYPE_MAP: {
                                string_val = "{";
                                CassIterator* map_iterator = cass_iterator_from_map(value);
                                bool first = true;
                                while (cass_iterator_next(map_iterator)) {
                                    if (!first) string_val += ", ";
                                    first = false;
                                    
                                    const CassValue* key = cass_iterator_get_map_key(map_iterator);
                                    const CassValue* val = cass_iterator_get_map_value(map_iterator);
                                    
                                    const char* key_str;
                                    size_t key_len;
                                    cass_value_get_string(key, &key_str, &key_len);
                                    
                                    const char* val_str;
                                    size_t val_len;
                                    cass_value_get_string(val, &val_str, &val_len);
                                    
                                    string_val += "'" + string(key_str, key_len) + "': '" + string(val_str, val_len) + "'";
                                }
                                string_val += "}";
                                cass_iterator_free(map_iterator);
                                break;
                            }
                            default:
                                string_val = "<type:" + std::to_string(static_cast<int>(value_type)) + ">";
                                break;
                        }
                        
                        data_ptr[row_count] = StringVector::AddString(vector, string_val);
                        break;
                    }
                }
            }
        }
        
        row_count++;
    }
    
    if (row_count == 0) {
        gstate.finished = true;
    }
    
    output.SetCardinality(row_count);
}

CassandraScanFunction::CassandraScanFunction() 
    : TableFunction("cassandra_scan", {LogicalType::VARCHAR}, CassandraScanExecute, CassandraScanBind, 
                    CassandraScanInitGlobal, nullptr) {
    
    named_parameters["contact_points"] = LogicalType::VARCHAR;
    named_parameters["host"] = LogicalType::VARCHAR;
    named_parameters["port"] = LogicalType::INTEGER;
    named_parameters["username"] = LogicalType::VARCHAR;
    named_parameters["password"] = LogicalType::VARCHAR;
    named_parameters["ssl"] = LogicalType::BOOLEAN;
    named_parameters["use_ssl"] = LogicalType::BOOLEAN;
    named_parameters["certfile"] = LogicalType::VARCHAR;
    named_parameters["userkey"] = LogicalType::VARCHAR;
    named_parameters["usercert"] = LogicalType::VARCHAR;
}

// Custom query function implementation
static unique_ptr<FunctionData> CassandraQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<CassandraScanBindData>();
    
    if (input.inputs.empty()) {
        throw BinderException("cassandra_query requires a CQL query string");
    }
    
    auto query = StringValue::Get(input.inputs[0]);
    bind_data->filter_condition = query; // Store the custom query
    
    // Set default configuration
    bind_data->config = CassandraConfig();
    
    // Parse named parameters for connection
    for (auto &kv : input.named_parameters) {
        auto lower_key = StringUtil::Lower(kv.first);
        if (lower_key == "contact_points" || lower_key == "host") {
            bind_data->config.contact_points = StringValue::Get(kv.second);
        } else if (lower_key == "port") {
            bind_data->config.port = IntegerValue::Get(kv.second);
        }
    }
    
    // For custom queries, execute to get schema
    try {
        auto client = make_uniq<CassandraClient>(bind_data->config);
        auto session = client->GetSession();
        CassStatement* statement = cass_statement_new(query.c_str(), 0);
        CassFuture* result_future = cass_session_execute(session, statement);
        
        if (cass_future_error_code(result_future) == CASS_OK) {
            const CassResult* result = cass_future_get_result(result_future);
            size_t column_count = cass_result_column_count(result);
            
            for (size_t i = 0; i < column_count; i++) {
                const char* column_name;
                size_t name_length;
                cass_result_column_name(result, i, &column_name, &name_length);
                
                string col_name(column_name, name_length);
                names.push_back(col_name);
                return_types.push_back(LogicalType::VARCHAR);
            }
            
            cass_result_free(result);
        }
        
        cass_future_free(result_future);
        cass_statement_free(statement);
        
    } catch (const std::exception& e) {
        names.push_back("result");
        return_types.push_back(LogicalType::VARCHAR);
    }
    
    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> CassandraQueryInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto bind_data = input.bind_data->Cast<CassandraScanBindData>();
    auto result = make_uniq<CassandraScanGlobalState>();
    
    result->client = make_uniq<CassandraClient>(bind_data.config);
    
    // Execute the custom CQL query
    string query = bind_data.filter_condition;
    
    auto session = result->client->GetSession();
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassFuture* query_future = cass_session_execute(session, statement);
    
    if (cass_future_error_code(query_future) == CASS_OK) {
        result->result = cass_future_get_result(query_future);
        result->result_iterator = cass_iterator_from_result(result->result);
        result->finished = false;
    } else {
        result->finished = true;
    }
    
    cass_future_free(query_future);
    cass_statement_free(statement);
    
    return std::move(result);
}

CassandraQueryFunction::CassandraQueryFunction()
    : TableFunction("cassandra_query", {LogicalType::VARCHAR}, CassandraScanExecute, CassandraQueryBind,
                    CassandraQueryInitGlobal, nullptr) {
    
    named_parameters["contact_points"] = LogicalType::VARCHAR;
    named_parameters["host"] = LogicalType::VARCHAR;
    named_parameters["port"] = LogicalType::INTEGER;
    named_parameters["username"] = LogicalType::VARCHAR;
    named_parameters["password"] = LogicalType::VARCHAR;
}

} // namespace cassandra
} // namespace duckdb