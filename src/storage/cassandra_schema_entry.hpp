#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "../include/cassandra_utils.hpp"

namespace duckdb {
namespace cassandra {

class CassandraSchemaEntry : public SchemaCatalogEntry {
public:
    CassandraSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const CassandraKeyspaceRef &keyspace_ref);

    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
    
    optional_ptr<CatalogEntry> GetTable(CatalogTransaction transaction, const string &name,
                                        OnEntryNotFound if_not_found,
                                        QueryErrorContext error_context = QueryErrorContext()) override;

    void DropTable(CatalogTransaction transaction, DropInfo &info) override;

private:
    CassandraKeyspaceRef keyspace_ref;
};

} // namespace cassandra
} // namespace duckdb