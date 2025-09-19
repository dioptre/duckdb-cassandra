#include "cassandra_table_entry.hpp"
#include "../include/cassandra_scan.hpp"

namespace duckdb {
namespace cassandra {

CassandraTableEntry::CassandraTableEntry(Catalog &catalog, CreateTableInfo &info, const CassandraTableRef &table_ref)
    : TableCatalogEntry(catalog, info), table_ref(table_ref) {
}

unique_ptr<BaseStatistics> CassandraTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
    // TODO: Return table statistics
    return nullptr;
}

TableFunction CassandraTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
    // TODO: Return scan function for this table
    auto scan_function = CassandraScanFunction();
    return scan_function;
}

} // namespace cassandra
} // namespace duckdb