#include "cassandra_catalog.hpp"
#include "cassandra_schema_entry.hpp"
#include "../include/cassandra_client.hpp"

namespace duckdb {
namespace cassandra {

CassandraCatalog::CassandraCatalog(AttachedDatabase &db, const string &name, const string &path,
                                   AccessMode access_mode, const CassandraConfig &config)
    : Catalog(db, name, path, access_mode), config(config) {
    
    client = make_uniq<CassandraClient>(config);
}

CassandraCatalog::~CassandraCatalog() = default;

void CassandraCatalog::Initialize(bool load_builtin) {
    // Initialize the schema set with available keyspaces
    // schema_set = make_uniq<CassandraSchemaSet>(*this);
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

optional_ptr<CatalogEntry> CassandraCatalog::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
    // TODO: Implement table creation
    throw NotImplementedException("CreateTable not yet implemented");
}

void CassandraCatalog::DropSchema(CatalogTransaction transaction, DropInfo &info) {
    if (!client->KeyspaceExists(info.name)) {
        if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
            return;
        }
        throw CatalogException("Keyspace \"%s\" does not exist", info.name);
    }

    client->DropKeyspace(info);
}

void CassandraCatalog::DropTable(CatalogTransaction transaction, DropInfo &info) {
    // TODO: Implement table drop
    client->DropTable(info);
}

optional_ptr<CatalogEntry> CassandraCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                        OnEntryNotFound if_not_found,
                                                        QueryErrorContext error_context) {
    // TODO: Return schema entry if exists
    if (!client->KeyspaceExists(schema_name)) {
        if (if_not_found == OnEntryNotFound::RETURN_NULL) {
            return nullptr;
        }
        throw CatalogException("Keyspace \"%s\" does not exist", schema_name);
    }

    // For now, return a basic schema entry
    auto keyspace_ref = client->GetKeyspace(schema_name);
    return make_uniq<CassandraSchemaEntry>(*this, CreateSchemaInfo{schema_name}, keyspace_ref).release();
}

// Additional required pure virtual method implementations
optional_ptr<SchemaCatalogEntry> CassandraCatalog::LookupSchema(CatalogTransaction transaction, const string &schema_name,
                                                                 OnEntryNotFound if_not_found,
                                                                 QueryErrorContext error_context) {
    // For now, just call GetSchema
    return GetSchema(transaction, schema_name, if_not_found, error_context);
}

void CassandraCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
    // TODO: Implement schema scanning
    // For now, just scan the configured keyspace
    if (!config.keyspace.empty()) {
        auto keyspace_ref = client->GetKeyspace(config.keyspace);
        CreateSchemaInfo schema_info{config.keyspace};
        auto schema_entry = CassandraSchemaEntry(*this, schema_info, keyspace_ref);
        callback(schema_entry);
    }
}

PhysicalOperator &CassandraCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, 
                                                       LogicalCreateTable &op, unique_ptr<PhysicalOperator> plan) {
    throw NotImplementedException("CREATE TABLE AS not implemented for Cassandra");
}

PhysicalOperator &CassandraCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, 
                                                LogicalInsert &op, unique_ptr<PhysicalOperator> plan) {
    throw NotImplementedException("INSERT not implemented for Cassandra");
}

PhysicalOperator &CassandraCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, 
                                                LogicalDelete &op, unique_ptr<PhysicalOperator> plan) {
    throw NotImplementedException("DELETE not implemented for Cassandra");
}

PhysicalOperator &CassandraCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, 
                                                LogicalUpdate &op, unique_ptr<PhysicalOperator> plan) {
    throw NotImplementedException("UPDATE not implemented for Cassandra");
}

DatabaseSize CassandraCatalog::GetDatabaseSize(ClientContext &context) {
    return DatabaseSize{0, 0}; // Return empty size for now
}

bool CassandraCatalog::InMemory() {
    return false; // Cassandra is not in-memory
}

string CassandraCatalog::GetDBPath() {
    return path; // Return the connection string
}

void CassandraCatalog::DropSchema(ClientContext &context, DropInfo &info) {
    // Delegate to transaction-based drop
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
    DropSchema(transaction, info);
}

} // namespace cassandra
} // namespace duckdb