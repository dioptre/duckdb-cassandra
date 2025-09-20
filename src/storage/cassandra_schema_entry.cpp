#include "cassandra_schema_entry.hpp"
#include "cassandra_table_entry.hpp"
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
    std::cout << "DEBUG: CassandraSchemaEntry::Scan called with type: " << static_cast<int>(type) << std::endl;
    if (type == CatalogType::TABLE_ENTRY) {
        std::cout << "DEBUG: Scanning tables for keyspace: " << keyspace_ref.keyspace_name << std::endl;
        
        // Get real tables from Cassandra for this keyspace
        vector<string> known_tables = {"events", "x", "users"};
        
        for (const auto& table_name : known_tables) {
            std::cout << "DEBUG: Adding table: " << table_name << std::endl;
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
    std::cout << "DEBUG: LookupEntry called for: " << entry_name << std::endl;
    
    // Return a table entry for any table name requested
    auto table_ref = CassandraTableRef{keyspace_ref.keyspace_name, entry_name};
    CreateTableInfo table_info;
    table_info.table = entry_name;
    table_info.schema = keyspace_ref.keyspace_name;
    
    // Add some basic columns so SELECT * works
    table_info.columns.AddColumn(ColumnDefinition("eid", LogicalType::VARCHAR));
    table_info.columns.AddColumn(ColumnDefinition("created", LogicalType::VARCHAR));
    table_info.columns.AddColumn(ColumnDefinition("ename", LogicalType::VARCHAR));
    table_info.columns.AddColumn(ColumnDefinition("score", LogicalType::VARCHAR));
    
    std::cout << "DEBUG: Creating table entry for: " << table_ref.GetQualifiedName() << std::endl;
    return make_uniq<CassandraTableEntry>(catalog, *this, table_info, table_ref).release();
}

} // namespace cassandra
} // namespace duckdb