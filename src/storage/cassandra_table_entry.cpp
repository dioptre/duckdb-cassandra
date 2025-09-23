#include "cassandra_table_entry.hpp"
#include "cassandra_catalog.hpp"
#include "../include/cassandra_scan.hpp"

namespace duckdb {
namespace cassandra {

CassandraTableEntry::CassandraTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, const CassandraTableRef &table_ref)
    : TableCatalogEntry(catalog, schema, info), table_ref(table_ref) {
}

unique_ptr<BaseStatistics> CassandraTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
    // TODO: Return table statistics
    return nullptr;
}

TableFunction CassandraTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
    // Create bind data for this specific table
    auto cassandra_bind_data = make_uniq<CassandraScanBindData>();
    cassandra_bind_data->table_ref = table_ref;
    
    // Get the connection config and shared client from the catalog
    auto &cassandra_catalog = catalog.Cast<CassandraCatalog>();
    cassandra_bind_data->config = cassandra_catalog.config;
    
    // Reuse the catalog's connection to avoid creating new connections
    cassandra_bind_data->reused_connection = cassandra_catalog.GetSharedClient();
    
    bind_data = std::move(cassandra_bind_data);
    
    // Return the cassandra scan function
    return CassandraScanFunction();
}

TableStorageInfo CassandraTableEntry::GetStorageInfo(ClientContext &context) {
    TableStorageInfo info;
    info.cardinality = 0;
    info.index_info = vector<IndexInfo>();
    return info;
}

} // namespace cassandra
} // namespace duckdb