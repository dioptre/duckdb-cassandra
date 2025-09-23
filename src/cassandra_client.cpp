#include "cassandra_client.hpp"
#include "cassandra_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/constraint.hpp"
#include <cassandra.h>
#include <iterator>

namespace duckdb {
namespace cassandra {

CassandraClient::CassandraClient(const CassandraConfig &config) : config(config), cluster(nullptr), session(nullptr) {
    // Initialize DataStax C++ driver
    cluster = cass_cluster_new();
    session = cass_session_new();
    
    Connect();
}

CassandraClient::~CassandraClient() {
    Disconnect();
    
    // Free DataStax C++ driver resources
    if (session) {
        cass_session_free(session);
    }
    if (cluster) {
        cass_cluster_free(cluster);
    }
}

void CassandraClient::Connect() {
    if (config.use_astra) {
        // Manual Astra configuration (no SCB needed)
        std::cout << "Configuring Astra connection manually..." << std::endl;
        
        // Set Astra host and port
        cass_cluster_set_contact_points(cluster, config.astra_host.c_str());
        cass_cluster_set_port(cluster, config.astra_port);
        
        // Set Astra credentials (clientId + clientSecret)
        cass_cluster_set_credentials(cluster, config.client_id.c_str(), config.client_secret.c_str());
        
        // Configure SSL with Astra certificates
        CassSsl* ssl = cass_ssl_new();
        
        // Handle CA certificate - prefer base64 if available
        std::string ca_cert;
        if (!config.astra_ca_cert_b64.empty()) {
            ca_cert = config.DecodeBase64ToString(config.astra_ca_cert_b64);
        } else if (!config.astra_ca_cert.empty()) {
            ca_cert = config.astra_ca_cert;
            // Replace \n with actual newlines if needed
            size_t pos = 0;
            while ((pos = ca_cert.find("\\n", pos)) != std::string::npos) {
                ca_cert.replace(pos, 2, "\n");
                pos += 1;
            }
        }
        
        if (!ca_cert.empty()) {
            cass_ssl_add_trusted_cert(ssl, ca_cert.c_str());
        } 

        // Handle client certificate and key - prefer base64 if available
        std::string client_cert, private_key;
        bool has_client_certs = false;
        
        if (!config.astra_client_cert_b64.empty() && !config.astra_client_key_b64.empty()) {
            client_cert = config.DecodeBase64ToString(config.astra_client_cert_b64);
            private_key = config.DecodeBase64ToString(config.astra_client_key_b64);
            has_client_certs = true;
        } else if (!config.astra_client_cert.empty() && !config.astra_client_key.empty()) {
            client_cert = config.astra_client_cert;
            private_key = config.astra_client_key;
            
            // Replace \n with actual newlines if needed
            size_t pos = 0;
            while ((pos = client_cert.find("\\n", pos)) != std::string::npos) {
                client_cert.replace(pos, 2, "\n");
                pos += 1;
            }
            pos = 0;
            while ((pos = private_key.find("\\n", pos)) != std::string::npos) {
                private_key.replace(pos, 2, "\n");
                pos += 1;
            }
            has_client_certs = true;
        }
        
        if (has_client_certs) {
            cass_ssl_set_cert(ssl, client_cert.c_str());
            cass_ssl_set_private_key(ssl, private_key.c_str(), "");
        }
        
        cass_cluster_set_ssl(cluster, ssl);
        cass_ssl_free(ssl);
    } else {
        // Traditional Cassandra configuration
        cass_cluster_set_contact_points(cluster, config.contact_points.c_str());
        cass_cluster_set_port(cluster, config.port);
        
        // Set credentials if provided
        if (!config.username.empty()) {
            cass_cluster_set_credentials(cluster, config.username.c_str(), config.password.c_str());
        }
    }
    
    // Configure SSL if certificates are provided
    if (config.HasSSLConfig()) {
        std::cout << "Configuring SSL/TLS connection..." << std::endl;
        
        // Create SSL context
        CassSsl* ssl = cass_ssl_new();
        
        // Set verification mode
        if (config.verify_peer_cert) {
            cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_PEER_CERT);
        } else {
            cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_NONE);
        }
        
        // Add CA certificate if provided
        if (!config.ca_cert_hex.empty()) {
            std::string ca_cert_pem = config.DecodeHexToString(config.ca_cert_hex);
            cass_ssl_add_trusted_cert(ssl, ca_cert_pem.c_str());
        }
        
        // Add client certificate and key if provided
        if (!config.user_cert_hex.empty() && !config.user_key_hex.empty()) {
            std::string user_cert_pem = config.DecodeHexToString(config.user_cert_hex);
            std::string user_key_pem = config.DecodeHexToString(config.user_key_hex);
            
            cass_ssl_set_cert(ssl, user_cert_pem.c_str());
            cass_ssl_set_private_key(ssl, user_key_pem.c_str(), ""); // No password for now
        }
        
        // Apply SSL to cluster
        cass_cluster_set_ssl(cluster, ssl);
        cass_ssl_free(ssl);
        
        std::cout << "SSL/TLS configuration applied" << std::endl;
    }
    
    // Connect to cluster
    CassFuture* connect_future = cass_session_connect(session, cluster);
    
    // Check for connection errors
    if (cass_future_error_code(connect_future) != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(connect_future, &message, &message_length);
        cass_future_free(connect_future);
        throw ConnectionException("Failed to connect to Cassandra: %s", std::string(message, message_length));
    }
    
    cass_future_free(connect_future);
    std::cout << "Successfully connected to Cassandra cluster: " << config.contact_points 
              << ":" << config.port << std::endl;
}

void CassandraClient::Disconnect() {
    if (session) {
        CassFuture* close_future = cass_session_close(session);
        cass_future_wait(close_future);
        cass_future_free(close_future);
    }
}

vector<CassandraKeyspaceRef> CassandraClient::GetKeyspaces() {
    vector<CassandraKeyspaceRef> keyspaces;
    
    // Query system.schema_keyspaces table
    const char* query = "SELECT keyspace_name FROM system_schema.keyspaces";
    
    CassStatement* statement = cass_statement_new(query, 0);
    CassFuture* result_future = cass_session_execute(session, statement);
    
    if (cass_future_error_code(result_future) == CASS_OK) {
        const CassResult* result = cass_future_get_result(result_future);
        CassIterator* rows = cass_iterator_from_result(result);
        
        while (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            const CassValue* keyspace_name_value = cass_row_get_column(row, 0);
            
            const char* keyspace_name;
            size_t keyspace_name_length;
            cass_value_get_string(keyspace_name_value, &keyspace_name, &keyspace_name_length);
            
            CassandraKeyspaceRef keyspace_ref;
            keyspace_ref.keyspace_name = std::string(keyspace_name, keyspace_name_length);
            keyspaces.push_back(keyspace_ref);
        }
        
        cass_iterator_free(rows);
        cass_result_free(result);
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(result_future, &message, &message_length);
        std::cerr << "Error querying keyspaces: " << std::string(message, message_length) << std::endl;
        
        // Fallback to dummy data
        CassandraKeyspaceRef system_keyspace;
        system_keyspace.keyspace_name = "system";
        keyspaces.push_back(system_keyspace);
        
        if (!config.keyspace.empty()) {
            CassandraKeyspaceRef config_keyspace;
            config_keyspace.keyspace_name = config.keyspace;
            keyspaces.push_back(config_keyspace);
        }
    }
    
    cass_future_free(result_future);
    cass_statement_free(statement);
    
    return keyspaces;
}

vector<CassandraTableRef> CassandraClient::GetTables(const string &keyspace_name) {
    vector<CassandraTableRef> tables;
    
    // Query system.schema_tables table
    const char* query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?";
    
    CassStatement* statement = cass_statement_new(query, 1);
    cass_statement_bind_string(statement, 0, keyspace_name.c_str());
    
    CassFuture* result_future = cass_session_execute(session, statement);
    
    if (cass_future_error_code(result_future) == CASS_OK) {
        const CassResult* result = cass_future_get_result(result_future);
        CassIterator* rows = cass_iterator_from_result(result);
        
        while (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            const CassValue* table_name_value = cass_row_get_column(row, 0);
            
            const char* table_name;
            size_t table_name_length;
            cass_value_get_string(table_name_value, &table_name, &table_name_length);
            
            CassandraTableRef table_ref;
            table_ref.keyspace_name = keyspace_name;
            table_ref.table_name = std::string(table_name, table_name_length);
            tables.push_back(table_ref);
        }
        
        cass_iterator_free(rows);
        cass_result_free(result);
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(result_future, &message, &message_length);
        std::cerr << "Error querying tables: " << std::string(message, message_length) << std::endl;
    }
    
    cass_future_free(result_future);
    cass_statement_free(statement);
    
    return tables;
}

CassandraKeyspaceRef CassandraClient::GetKeyspace(const string &keyspace_name) {
    CassandraKeyspaceRef keyspace_ref;
    keyspace_ref.keyspace_name = keyspace_name;
    return keyspace_ref;
}

CassandraTableRef CassandraClient::GetTable(const string &keyspace_name, const string &table_name) {
    CassandraTableRef table_ref;
    table_ref.keyspace_name = keyspace_name;
    table_ref.table_name = table_name;
    return table_ref;
}

bool CassandraClient::KeyspaceExists(const string &keyspace_name) {
    // TODO: Check if keyspace exists
    return !keyspace_name.empty();
}

bool CassandraClient::TableExists(const string &keyspace_name, const string &table_name) {
    // TODO: Check if table exists
    return !keyspace_name.empty() && !table_name.empty();
}

void CassandraClient::CreateKeyspace(const CreateSchemaInfo &info, const CassandraKeyspaceRef &keyspace_ref) {
    // TODO: Execute CREATE KEYSPACE statement
    throw NotImplementedException("CreateKeyspace not yet implemented");
}

void CassandraClient::CreateTable(const CreateTableInfo &info, const CassandraTableRef &table_ref) {
    // TODO: Execute CREATE TABLE statement
    throw NotImplementedException("CreateTable not yet implemented");
}

void CassandraClient::DropKeyspace(const DropInfo &info) {
    // TODO: Execute DROP KEYSPACE statement
    throw NotImplementedException("DropKeyspace not yet implemented");
}

void CassandraClient::DropTable(const DropInfo &info) {
    // TODO: Execute DROP TABLE statement
    throw NotImplementedException("DropTable not yet implemented");
}

void CassandraClient::GetTableInfo(const string &keyspace_name,
                                   const string &table_name,
                                   ColumnList &res_columns,
                                   vector<unique_ptr<Constraint>> &res_constraints) {
    
    // Query system_schema.columns to get column information
    const char* query = 
        "SELECT column_name, type, kind, position "
        "FROM system_schema.columns "
        "WHERE keyspace_name = ? AND table_name = ? "
        "ORDER BY position ASC";
    
    CassStatement* statement = cass_statement_new(query, 2);
    cass_statement_bind_string(statement, 0, keyspace_name.c_str());
    cass_statement_bind_string(statement, 1, table_name.c_str());
    
    CassFuture* result_future = cass_session_execute(session, statement);
    
    if (cass_future_error_code(result_future) == CASS_OK) {
        const CassResult* result = cass_future_get_result(result_future);
        CassIterator* rows = cass_iterator_from_result(result);
        
        vector<CassandraColumnInfo> columns;
        
        while (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            
            // Extract column information
            CassandraColumnInfo col_info;
            
            // Column name
            const CassValue* name_value = cass_row_get_column(row, 0);
            const char* name_str;
            size_t name_len;
            cass_value_get_string(name_value, &name_str, &name_len);
            col_info.column_name = std::string(name_str, name_len);
            
            // Type
            const CassValue* type_value = cass_row_get_column(row, 1);
            const char* type_str;
            size_t type_len;
            cass_value_get_string(type_value, &type_str, &type_len);
            col_info.type = std::string(type_str, type_len);
            
            // Kind (partition_key, clustering, regular, static)
            const CassValue* kind_value = cass_row_get_column(row, 2);
            const char* kind_str;
            size_t kind_len;
            cass_value_get_string(kind_value, &kind_str, &kind_len);
            col_info.kind = std::string(kind_str, kind_len);
            
            // Position
            const CassValue* pos_value = cass_row_get_column(row, 3);
            cass_int32_t position;
            cass_value_get_int32(pos_value, &position);
            col_info.position = position;
            
            columns.push_back(col_info);
        }
        
        // Convert to DuckDB ColumnList
        for (const auto& col : columns) {
            auto duckdb_type = col.GetDuckDBType();
            auto column_def = ColumnDefinition(col.column_name, duckdb_type);
            res_columns.AddColumn(std::move(column_def));
        }
        
        // TODO: Add primary key constraint based on partition and clustering keys
        // This requires finding the correct DuckDB constraint header
        // vector<string> primary_key_columns;
        // for (const auto& col : columns) {
        //     if (col.kind == "partition_key" || col.kind == "clustering") {
        //         primary_key_columns.push_back(col.column_name);
        //     }
        // }
        
        cass_iterator_free(rows);
        cass_result_free(result);
        
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(result_future, &message, &message_length);
        std::cerr << "Error querying table schema: " << std::string(message, message_length) << std::endl;
        throw InternalException("Failed to get table schema for %s.%s: %s", 
                               keyspace_name, table_name, std::string(message, message_length));
    }
    
    cass_future_free(result_future);
    cass_statement_free(statement);
}

unique_ptr<QueryResult> CassandraClient::ExecuteQuery(const string &query) {
    // TODO: Execute CQL query and return results
    throw NotImplementedException("ExecuteQuery not yet implemented");
}

} // namespace cassandra
} // namespace duckdb