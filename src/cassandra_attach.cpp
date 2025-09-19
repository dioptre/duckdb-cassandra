#include "cassandra_attach.hpp"
#include "cassandra_utils.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {
namespace cassandra {

struct CassandraAttachBindData : public TableFunctionData {
    string connection_string;
    string name;
    CassandraConfig config;
};

static unique_ptr<FunctionData> CassandraAttachBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<CassandraAttachBindData>();
    
    if (input.inputs.empty()) {
        throw BinderException("ATTACH requires a connection string");
    }
    
    bind_data->connection_string = StringValue::Get(input.inputs[0]);
    bind_data->config = CassandraConfig::FromConnectionString(bind_data->connection_string);
    
    // Parse named parameters
    for (auto &kv : input.named_parameters) {
        auto lower_key = StringUtil::Lower(kv.first);
        if (lower_key == "name") {
            bind_data->name = StringValue::Get(kv.second);
        }
    }
    
    if (bind_data->name.empty()) {
        bind_data->name = "cassandra";
    }
    
    // Return schema for attach result
    return_types.push_back(LogicalType::BOOLEAN);
    names.push_back("success");
    
    return std::move(bind_data);
}

static void CassandraAttachExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<CassandraAttachBindData>();
    
    // Create attach info
    AttachInfo info;
    info.name = bind_data.name;
    info.path = bind_data.connection_string;
    info.options["type"] = Value("cassandra");
    
    auto &db_manager = DatabaseManager::Get(context);
    AttachOptions options(info.options, AccessMode::AUTOMATIC);
    db_manager.AttachDatabase(context, info, options);
    
    // Return success
    output.SetCardinality(1);
    auto success_data = FlatVector::GetData<bool>(output.data[0]);
    success_data[0] = true;
}

CassandraAttachFunction::CassandraAttachFunction()
    : TableFunction("cassandra_attach", {LogicalType::VARCHAR}, CassandraAttachExecute, CassandraAttachBind) {
    
    named_parameters["name"] = LogicalType::VARCHAR;
}

} // namespace cassandra
} // namespace duckdb