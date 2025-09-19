#include "cassandra_schema_entry.hpp"
#include "cassandra_table_entry.hpp"

namespace duckdb {
namespace cassandra {

CassandraSchemaEntry::CassandraSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const CassandraKeyspaceRef &keyspace_ref)
    : SchemaCatalogEntry(catalog, info), keyspace_ref(keyspace_ref) {
}

optional_ptr<CatalogEntry> CassandraSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
    // TODO: Implement table creation
    throw NotImplementedException("CreateTable not yet implemented in schema entry");
}

optional_ptr<CatalogEntry> CassandraSchemaEntry::GetTable(CatalogTransaction transaction, const string &name,
                                                           OnEntryNotFound if_not_found,
                                                           QueryErrorContext error_context) {
    // TODO: Check if table exists and return table entry
    auto table_ref = CassandraTableRef{keyspace_ref.keyspace_name, name};
    
    // For now, always return a basic table entry
    CreateTableInfo table_info;
    table_info.table = name;
    table_info.schema = keyspace_ref.keyspace_name;
    
    return make_uniq<CassandraTableEntry>(catalog, table_info, table_ref).release();
}

void CassandraSchemaEntry::DropTable(CatalogTransaction transaction, DropInfo &info) {
    // TODO: Implement table drop
    throw NotImplementedException("DropTable not yet implemented in schema entry");
}

} // namespace cassandra
} // namespace duckdb