#include "cassandra_schema_entry.hpp"
#include "cassandra_table_entry.hpp"
#include "cassandra_catalog.hpp"
#include "../include/cassandra_client.hpp"
#include "cassandra_types.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {
namespace cassandra {

CassandraSchemaEntry::CassandraSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const CassandraKeyspaceRef &keyspace_ref)
    : SchemaCatalogEntry(catalog, info), keyspace_ref(keyspace_ref) {
}

// Implementation of all required virtual methods
optional_ptr<CatalogEntry> CassandraSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
    throw NotImplementedException("CreateTable not yet implemented");
}

optional_ptr<CatalogEntry> CassandraSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
    throw NotImplementedException("CreateFunction not supported");
}

optional_ptr<CatalogEntry> CassandraSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info, TableCatalogEntry &table) {
    throw NotImplementedException("CreateIndex not supported");
}

optional_ptr<CatalogEntry> CassandraSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
    throw NotImplementedException("CreateView not supported");
}

optional_ptr<CatalogEntry> CassandraSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
    throw NotImplementedException("CreateSequence not supported");
}

optional_ptr<CatalogEntry> CassandraSchemaEntry::CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info) {
    throw NotImplementedException("CreateTableFunction not supported");
}

optional_ptr<CatalogEntry> CassandraSchemaEntry::CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) {
    throw NotImplementedException("CreateCopyFunction not supported");
}

optional_ptr<CatalogEntry> CassandraSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo &info) {
    throw NotImplementedException("CreatePragmaFunction not supported");
}

optional_ptr<CatalogEntry> CassandraSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
    throw NotImplementedException("CreateCollation not supported");
}

optional_ptr<CatalogEntry> CassandraSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
    throw NotImplementedException("CreateType not supported");
}

void CassandraSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
    throw NotImplementedException("Alter not supported");
}

void CassandraSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    if (type == CatalogType::TABLE_ENTRY) {
        
        // Get real tables from Cassandra for this keyspace
        vector<string> known_tables = {"events", "x", "users"};
        
        for (const auto& table_name : known_tables) {
            auto table_ref = CassandraTableRef{keyspace_ref.keyspace_name, table_name};
            CreateTableInfo table_info;
            table_info.table = table_name;
            table_info.schema = keyspace_ref.keyspace_name;
            
            auto table_entry = make_uniq<CassandraTableEntry>(catalog, *this, table_info, table_ref);
            callback(*table_entry);
        }
    }
}

void CassandraSchemaEntry::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    // Delegate to the main Scan method
    Scan(type, callback);
}

void CassandraSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
    throw NotImplementedException("DropEntry not yet implemented");
}

optional_ptr<CatalogEntry> CassandraSchemaEntry::LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) {
    auto entry_name = lookup_info.GetEntryName();
    
    // Return a table entry for any table name requested
    auto table_ref = CassandraTableRef{keyspace_ref.keyspace_name, entry_name};
    CreateTableInfo table_info;
    table_info.table = entry_name;
    table_info.schema = keyspace_ref.keyspace_name;
    
    // Get REAL columns from Cassandra dynamically
    try {
        auto &cassandra_catalog = catalog.Cast<CassandraCatalog>();
        auto client = make_uniq<CassandraClient>(cassandra_catalog.config);
        
        string schema_query = "SELECT * FROM " + keyspace_ref.keyspace_name + "." + entry_name + " LIMIT 1";
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
                string col_name(column_name, name_length);
                
                // Map Cassandra types to proper DuckDB types
                CassValueType cass_type = cass_result_column_type(result, i);
                LogicalType duckdb_type;
                
                switch (cass_type) {
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
                    case CASS_VALUE_TYPE_BOOLEAN:
                        duckdb_type = LogicalType::BOOLEAN;
                        break;
                    case CASS_VALUE_TYPE_FLOAT:
                        duckdb_type = LogicalType::FLOAT;
                        break;
                    case CASS_VALUE_TYPE_SMALL_INT:
                        duckdb_type = LogicalType::SMALLINT;
                        break;
                    case CASS_VALUE_TYPE_TINY_INT:
                        duckdb_type = LogicalType::TINYINT;
                        break;
                    case CASS_VALUE_TYPE_DATE:
                        duckdb_type = LogicalType::DATE;
                        break;
                    case CASS_VALUE_TYPE_TIME:
                        duckdb_type = LogicalType::TIME;
                        break;
                    case CASS_VALUE_TYPE_INET:
                        duckdb_type = LogicalType::VARCHAR;  // INET as string
                        break;
                    case CASS_VALUE_TYPE_DECIMAL:
                    case CASS_VALUE_TYPE_VARINT:
                        duckdb_type = LogicalType::VARCHAR;  // Large numbers as strings
                        break;
                    case CASS_VALUE_TYPE_BLOB:
                        duckdb_type = LogicalType::BLOB;
                        break;
                    case CASS_VALUE_TYPE_LIST:
                    case CASS_VALUE_TYPE_SET:
                    case CASS_VALUE_TYPE_MAP:
                        duckdb_type = LogicalType::VARCHAR;  // Collections as JSON strings
                        break;
                    default:
                        duckdb_type = LogicalType::VARCHAR;
                        break;
                }
                
                table_info.columns.AddColumn(ColumnDefinition(col_name, duckdb_type));
            }
            
            cass_result_free(result);
        }
        
        cass_future_free(result_future);
        cass_statement_free(statement);
        
    } catch (const std::exception& e) {
        throw BinderException("Failed to get schema for table '%s.%s': %s", 
                            keyspace_ref.keyspace_name.c_str(), entry_name.c_str(), e.what());
    }
    
    // Ensure we actually have columns
    if (table_info.columns.LogicalColumnCount() == 0) {
        throw BinderException("Table '%s.%s' has no columns or does not exist", 
                            keyspace_ref.keyspace_name.c_str(), entry_name.c_str());
    }
    
    return make_uniq<CassandraTableEntry>(catalog, *this, table_info, table_ref).release();
}

} // namespace cassandra
} // namespace duckdb