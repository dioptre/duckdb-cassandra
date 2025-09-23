#pragma once

#include "cassandra_utils.hpp"
#include "duckdb.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"

#include <iostream>
#include <string>
#include <vector>

// DataStax C++ driver headers
#include <cassandra.h>

namespace duckdb {
namespace cassandra {

class CassandraClient {
public:
    explicit CassandraClient(const CassandraConfig &config);
    ~CassandraClient();

public:
    vector<CassandraKeyspaceRef> GetKeyspaces();
    vector<CassandraTableRef> GetTables(const string &keyspace_name);
    
    CassandraKeyspaceRef GetKeyspace(const string &keyspace_name);
    CassandraTableRef GetTable(const string &keyspace_name, const string &table_name);
    
    bool KeyspaceExists(const string &keyspace_name);
    bool TableExists(const string &keyspace_name, const string &table_name);
    
    void CreateKeyspace(const CreateSchemaInfo &info, const CassandraKeyspaceRef &keyspace_ref);
    void CreateTable(const CreateTableInfo &info, const CassandraTableRef &table_ref);
    
    void DropKeyspace(const DropInfo &info);
    void DropTable(const DropInfo &info);
    
    void GetTableInfo(const string &keyspace_name,
                      const string &table_name,
                      ColumnList &res_columns,
                      vector<unique_ptr<Constraint>> &res_constraints);
    
    // Execute CQL query and return results
    unique_ptr<QueryResult> ExecuteQuery(const string &query);
    
    // Get access to the session for direct query execution (with connection recovery)
    CassSession* GetSession();
    
    // Connection recovery methods
    bool IsConnected() const;
    void ResetConnection();

private:
    CassandraConfig config;
    // DataStax C++ driver objects
    CassCluster* cluster;
    CassSession* session;
    
    void Connect();
    void Disconnect();
    
    // Connection state tracking
    mutable std::mutex connection_mutex;
    mutable bool connection_valid;
};

} // namespace cassandra
} // namespace duckdb