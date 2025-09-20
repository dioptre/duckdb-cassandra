#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "../include/cassandra_utils.hpp"

namespace duckdb {
namespace cassandra {

class CassandraTableEntry : public TableCatalogEntry {
public:
    CassandraTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, const CassandraTableRef &table_ref);

    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
    
    TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
    CassandraTableRef table_ref;
};

} // namespace cassandra
} // namespace duckdb