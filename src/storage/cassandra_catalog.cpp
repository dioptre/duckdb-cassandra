#include "cassandra_catalog.hpp"
#include "cassandra_schema_entry.hpp"
#include "../include/cassandra_client.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"

namespace duckdb {
namespace cassandra {

CassandraCatalog::CassandraCatalog(AttachedDatabase &db, const string &name, const string &path,
                                   AccessMode access_mode, const CassandraConfig &config)
    : Catalog(db), config(config) {
    
    client = make_uniq<CassandraClient>(config);
}

CassandraCatalog::~CassandraCatalog() = default;

void CassandraCatalog::Initialize(bool load_builtin) {
    
    // Create the default schema (keyspace) 
    if (!config.keyspace.empty()) {
        // The schema will be created on-demand via LookupSchema
    }
}

string CassandraCatalog::GetCatalogType() {
    return "cassandra";
}

optional_ptr<CatalogEntry> CassandraCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
    if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
        throw NotImplementedException("REPLACE not supported for CREATE KEYSPACE");
    }

    auto keyspace_ref = CassandraKeyspaceRef{info.schema};
    
    if (client->KeyspaceExists(info.schema)) {
        if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
            return nullptr;
        }
        throw CatalogException("Keyspace \"%s\" already exists", info.schema);
    }

    client->CreateKeyspace(info, keyspace_ref);
    return nullptr;
}

optional_ptr<SchemaCatalogEntry> CassandraCatalog::LookupSchema(CatalogTransaction transaction,
                                                                const EntryLookupInfo &schema_lookup,
                                                                OnEntryNotFound if_not_found) {
    auto schema_name = schema_lookup.GetEntryName();
    
    // For Cassandra, we should default to the configured keyspace if no schema specified
    if (schema_name.empty() || schema_name == "main") {
        schema_name = config.keyspace.empty() ? "sfpla" : config.keyspace;
    }
    
    auto keyspace_ref = client->GetKeyspace(schema_name);
    CreateSchemaInfo schema_info;
    schema_info.schema = schema_name;
    return make_uniq<CassandraSchemaEntry>(*this, schema_info, keyspace_ref).release();
}

void CassandraCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
    auto keyspaces = client->GetKeyspaces();
    for (const auto& ks : keyspaces) {
        CreateSchemaInfo schema_info;
        schema_info.schema = ks.keyspace_name;
        auto schema_entry = make_uniq<CassandraSchemaEntry>(*this, schema_info, ks);
        callback(*schema_entry);
    }
}

void CassandraCatalog::DropSchema(ClientContext &context, DropInfo &info) {
    if (!client->KeyspaceExists(info.name)) {
        if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
            return;
        }
        throw CatalogException("Keyspace \"%s\" does not exist", info.name);
    }

    client->DropKeyspace(info);
}

PhysicalOperator &CassandraCatalog::PlanCreateTableAs(ClientContext &context,
                                                       PhysicalPlanGenerator &planner,
                                                       LogicalCreateTable &op,
                                                       PhysicalOperator &plan) {
    throw NotImplementedException("CREATE TABLE AS not supported for Cassandra");
}

PhysicalOperator &CassandraCatalog::PlanInsert(ClientContext &context,
                                                PhysicalPlanGenerator &planner,
                                                LogicalInsert &op,
                                                optional_ptr<PhysicalOperator> plan) {
    throw NotImplementedException("INSERT not yet supported for Cassandra");
}

PhysicalOperator &CassandraCatalog::PlanDelete(ClientContext &context,
                                                PhysicalPlanGenerator &planner,
                                                LogicalDelete &op,
                                                PhysicalOperator &plan) {
    throw NotImplementedException("DELETE not supported for Cassandra");
}

PhysicalOperator &CassandraCatalog::PlanUpdate(ClientContext &context,
                                                PhysicalPlanGenerator &planner,
                                                LogicalUpdate &op,
                                                PhysicalOperator &plan) {
    throw NotImplementedException("UPDATE not supported for Cassandra");
}

unique_ptr<LogicalOperator> CassandraCatalog::BindCreateIndex(Binder &binder,
                                                              CreateStatement &stmt,
                                                              TableCatalogEntry &table,
                                                              unique_ptr<LogicalOperator> plan) {
    throw NotImplementedException("CREATE INDEX not supported for Cassandra");
}

DatabaseSize CassandraCatalog::GetDatabaseSize(ClientContext &context) {
    DatabaseSize result;
    result.total_blocks = 0;
    result.block_size = 0;
    result.free_blocks = 0;
    result.used_blocks = 0;
    result.bytes = 0;
    return result;
}

vector<MetadataBlockInfo> CassandraCatalog::GetMetadataInfo(ClientContext &context) {
    return vector<MetadataBlockInfo>();
}

bool CassandraCatalog::InMemory() {
    return false;
}

string CassandraCatalog::GetDBPath() {
    return GetName();
}

} // namespace cassandra
} // namespace duckdb