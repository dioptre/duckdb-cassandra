#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "../include/cassandra_utils.hpp"

namespace duckdb {
namespace cassandra {

class CassandraSchemaEntry : public SchemaCatalogEntry {
public:
    CassandraSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const CassandraKeyspaceRef &keyspace_ref);

    // Required override methods from SchemaCatalogEntry
    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info, TableCatalogEntry &table) override;
    optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
    optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
    optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;

    void Alter(CatalogTransaction transaction, AlterInfo &info) override;
    void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
    void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
    void DropEntry(ClientContext &context, DropInfo &info) override;
    
    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

private:
    CassandraKeyspaceRef keyspace_ref;
};

} // namespace cassandra
} // namespace duckdb