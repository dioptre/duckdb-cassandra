#include "cassandra_scan.hpp"
#include "cassandra_client.hpp"
#include "cassandra_utils.hpp"
#include "cassandra_types.hpp"

namespace duckdb {
namespace cassandra {

struct CassandraScanBindData : public TableFunctionData {
    CassandraTableRef table_ref;
    CassandraConfig config;
    string filter_condition;
    vector<string> column_names;
    vector<LogicalType> column_types;
};

struct CassandraScanGlobalState : public GlobalTableFunctionState {
    unique_ptr<CassandraClient> client;
    CassIterator* result_iterator;
    const CassResult* result;
    bool finished;
    vector<LogicalType> column_types;
    
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
    
    // Parse table name - format: keyspace.table or just table
    auto dot_pos = table_name.find('.');
    if (dot_pos != string::npos) {
        bind_data->table_ref.keyspace_name = table_name.substr(0, dot_pos);
        bind_data->table_ref.table_name = table_name.substr(dot_pos + 1);
    } else {
        throw BinderException("Table name must include keyspace: keyspace.table");
    }
    
    // Set default configuration
    bind_data->config = CassandraConfig();
    
    // Parse named parameters for connection configuration
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
        } else if (lower_key == "keyspace") {
            bind_data->config.keyspace = StringValue::Get(kv.second);
        } else if (lower_key == "filter") {
            bind_data->filter_condition = StringValue::Get(kv.second);
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
    
    // Connect to Cassandra and get table schema
    auto client = make_uniq<CassandraClient>(bind_data->config);
    
    // Get real table schema using a simple approach
    try {
        auto client = make_uniq<CassandraClient>(bind_data->config);
        
        // Query system_schema.columns to get actual column names
        string schema_query = "SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = '" 
                            + bind_data->table_ref.keyspace_name + "' AND table_name = '" 
                            + bind_data->table_ref.table_name + "'";
        
        auto session = client->GetSession();
        CassStatement* statement = cass_statement_new(schema_query.c_str(), 0);
        CassFuture* result_future = cass_session_execute(session, statement);
        
        if (cass_future_error_code(result_future) == CASS_OK) {
            const CassResult* result = cass_future_get_result(result_future);
            CassIterator* rows = cass_iterator_from_result(result);
            
            while (cass_iterator_next(rows)) {
                const CassRow* row = cass_iterator_get_row(rows);
                
                // Get column name
                const CassValue* name_value = cass_row_get_column(row, 0);
                const char* name_str;
                size_t name_len;
                cass_value_get_string(name_value, &name_str, &name_len);
                string col_name(name_str, name_len);
                
                // For now, treat all columns as VARCHAR
                names.push_back(col_name);
                return_types.push_back(LogicalType::VARCHAR);
            }
            
            cass_iterator_free(rows);
            cass_result_free(result);
        }
        
        cass_future_free(result_future);
        cass_statement_free(statement);
        
        if (names.empty()) {
            // Fallback schema
            names.push_back("column1");
            return_types.push_back(LogicalType::VARCHAR);
        }
        
        std::cout << "Found " << names.size() << " columns for table: " 
                  << bind_data->table_ref.GetQualifiedName() << std::endl;
                  
    } catch (const std::exception& e) {
        std::cerr << "Error getting schema: " << e.what() << std::endl;
        names.push_back("error");
        return_types.push_back(LogicalType::VARCHAR);
    }
    
    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> CassandraScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto bind_data = input.bind_data->Cast<CassandraScanBindData>();
    auto result = make_uniq<CassandraScanGlobalState>();
    
    result->client = make_uniq<CassandraClient>(bind_data.config);
    result->column_types = {}; // Will be populated when we execute query
    
    // Build and execute CQL query
    string query = "SELECT * FROM " + bind_data.table_ref.keyspace_name + "." + bind_data.table_ref.table_name;
    
    if (!bind_data.filter_condition.empty()) {
        query += " WHERE " + bind_data.filter_condition;
    }
    
    // Execute the query using the client's session
    auto session = result->client->GetSession();
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassFuture* query_future = cass_session_execute(session, statement);
    
    if (cass_future_error_code(query_future) == CASS_OK) {
        result->result = cass_future_get_result(query_future);
        result->result_iterator = cass_iterator_from_result(result->result);
        result->finished = false;
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(query_future, &message, &message_length);
        cass_future_free(query_future);
        cass_statement_free(statement);
        throw InternalException("Failed to execute CQL query '%s': %s", query, std::string(message, message_length));
    }
    
    cass_future_free(query_future);
    cass_statement_free(statement);
    
    return std::move(result);
}

static void CassandraScanExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<CassandraScanBindData>();
    auto &gstate = data.global_state->Cast<CassandraScanGlobalState>();
    
    if (gstate.finished || !gstate.result_iterator) {
        output.SetCardinality(0);
        return;
    }
    
    idx_t row_count = 0;
    const idx_t chunk_size = STANDARD_VECTOR_SIZE;
    
    // Process rows from Cassandra result - simplified approach
    while (row_count < chunk_size && cass_iterator_next(gstate.result_iterator)) {
        const CassRow* row = cass_iterator_get_row(gstate.result_iterator);
        
        // Get the number of columns from the result
        size_t column_count = cass_result_column_count(gstate.result);
        
        // Convert each column value to string for simplicity
        for (size_t col_idx = 0; col_idx < column_count && col_idx < output.ColumnCount(); col_idx++) {
            const CassValue* value = cass_row_get_column(row, col_idx);
            
            auto &vector = output.data[col_idx];
            auto data_ptr = FlatVector::GetData<string_t>(vector);
            auto validity = FlatVector::Validity(vector);
            
            if (cass_value_is_null(value)) {
                validity.SetInvalid(row_count);
            } else {
                validity.SetValid(row_count);
                
                // Convert all values to strings for now
                string string_val;
                
                CassValueType value_type = cass_value_type(value);
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
                    case CASS_VALUE_TYPE_INT: {
                        cass_int32_t int_val;
                        cass_value_get_int32(value, &int_val);
                        string_val = std::to_string(int_val);
                        break;
                    }
                    case CASS_VALUE_TYPE_BIGINT: {
                        cass_int64_t bigint_val;
                        cass_value_get_int64(value, &bigint_val);
                        string_val = std::to_string(bigint_val);
                        break;
                    }
                    case CASS_VALUE_TYPE_DOUBLE: {
                        cass_double_t double_val;
                        cass_value_get_double(value, &double_val);
                        string_val = std::to_string(double_val);
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
                    default:
                        string_val = "<unsupported_type>";
                        break;
                }
                
                data_ptr[row_count] = StringVector::AddString(vector, string_val);
            }
        }
        
        row_count++;
    }
    
    // Check if we've finished iterating
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
    named_parameters["keyspace"] = LogicalType::VARCHAR;
    named_parameters["filter"] = LogicalType::VARCHAR;
    named_parameters["ssl"] = LogicalType::BOOLEAN;
    named_parameters["use_ssl"] = LogicalType::BOOLEAN;
    named_parameters["certfile"] = LogicalType::VARCHAR;
    named_parameters["userkey"] = LogicalType::VARCHAR;
    named_parameters["usercert"] = LogicalType::VARCHAR;
}

// Similar implementation for CassandraQueryFunction
CassandraQueryFunction::CassandraQueryFunction()
    : TableFunction("cassandra_query", {LogicalType::VARCHAR}, CassandraScanExecute, CassandraScanBind,
                    CassandraScanInitGlobal, nullptr) {
    
    named_parameters["contact_points"] = LogicalType::VARCHAR;
    named_parameters["host"] = LogicalType::VARCHAR;
    named_parameters["port"] = LogicalType::INTEGER;
    named_parameters["username"] = LogicalType::VARCHAR;
    named_parameters["password"] = LogicalType::VARCHAR;
    named_parameters["keyspace"] = LogicalType::VARCHAR;
}

} // namespace cassandra
} // namespace duckdb