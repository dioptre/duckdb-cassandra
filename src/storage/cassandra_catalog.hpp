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
    
    optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction,
                                                  const EntryLookupInfo &schema_lookup,
                                                  OnEntryNotFound if_not_found) override;

    void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

    void DropSchema(ClientContext &context, DropInfo &info) override;

    PhysicalOperator &PlanCreateTableAs(ClientContext &context,
                                        PhysicalPlanGenerator &planner,
                                        LogicalCreateTable &op,
                                        PhysicalOperator &plan) override;
    PhysicalOperator &PlanInsert(ClientContext &context,
                                 PhysicalPlanGenerator &planner,
                                 LogicalInsert &op,
                                 optional_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanDelete(ClientContext &context,
                                 PhysicalPlanGenerator &planner,
                                 LogicalDelete &op,
                                 PhysicalOperator &plan) override;
    PhysicalOperator &PlanUpdate(ClientContext &context,
                                 PhysicalPlanGenerator &planner,
                                 LogicalUpdate &op,
                                 PhysicalOperator &plan) override;
    
    unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder,
                                                CreateStatement &stmt,
                                                TableCatalogEntry &table,
                                                unique_ptr<LogicalOperator> plan) override;

    DatabaseSize GetDatabaseSize(ClientContext &context) override;
    vector<MetadataBlockInfo> GetMetadataInfo(ClientContext &context) override;

    bool InMemory() override;
    string GetDBPath() override;

private:
    CassandraConfig config;
    unique_ptr<class CassandraClient> client;
};

} // namespace cassandra
} // namespace duckdb