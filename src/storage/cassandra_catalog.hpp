#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "../include/cassandra_utils.hpp"

namespace duckdb {
namespace cassandra {

class CassandraCatalog : public Catalog {
public:
    CassandraCatalog(AttachedDatabase &db, const string &name, const string &path,
                     AccessMode access_mode, const CassandraConfig &config);
    ~CassandraCatalog() override;

    void Initialize(bool load_builtin) override;
    string GetCatalogType() override;

    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
    
    void DropSchema(CatalogTransaction transaction, DropInfo &info) override;
    void DropTable(CatalogTransaction transaction, DropInfo &info) override;

    optional_ptr<CatalogEntry> GetSchema(CatalogTransaction transaction, const string &schema_name,
                                         OnEntryNotFound if_not_found,
                                         QueryErrorContext error_context = QueryErrorContext()) override;

    // Additional required pure virtual methods
    optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const string &schema_name,
                                                   OnEntryNotFound if_not_found,
                                                   QueryErrorContext error_context = QueryErrorContext()) override;
    void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
    PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op, unique_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op, unique_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op, unique_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op, unique_ptr<PhysicalOperator> plan) override;
    DatabaseSize GetDatabaseSize(ClientContext &context) override;
    bool InMemory() override;
    string GetDBPath() override;
    void DropSchema(ClientContext &context, DropInfo &info) override;

private:
    CassandraConfig config;
    unique_ptr<class CassandraClient> client;
    unique_ptr<class CassandraSchemaSet> schema_set;
};

} // namespace cassandra
} // namespace duckdb