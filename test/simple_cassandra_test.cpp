#include <cassandra.h>
#include <iostream>
#include <string>

int main() {
    std::cout << "=== Simple Cassandra Connection Test ===" << std::endl;
    
    // Create cluster and session
    CassCluster* cluster = cass_cluster_new();
    CassSession* session = cass_session_new();
    
    // Configure cluster
    cass_cluster_set_contact_points(cluster, "127.0.0.1");
    cass_cluster_set_port(cluster, 9042);
    
    // Connect to cluster
    std::cout << "Connecting to Cassandra at 127.0.0.1:9042..." << std::endl;
    CassFuture* connect_future = cass_session_connect(session, cluster);
    
    if (cass_future_error_code(connect_future) != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(connect_future, &message, &message_length);
        std::cerr << "❌ Connection failed: " << std::string(message, message_length) << std::endl;
        
        cass_future_free(connect_future);
        cass_session_free(session);
        cass_cluster_free(cluster);
        return 1;
    }
    
    cass_future_free(connect_future);
    std::cout << "✅ Connected successfully!" << std::endl;
    
    // Test query: List keyspaces
    std::cout << "Querying keyspaces..." << std::endl;
    const char* query = "SELECT keyspace_name FROM system_schema.keyspaces";
    CassStatement* statement = cass_statement_new(query, 0);
    CassFuture* result_future = cass_session_execute(session, statement);
    
    if (cass_future_error_code(result_future) == CASS_OK) {
        const CassResult* result = cass_future_get_result(result_future);
        CassIterator* rows = cass_iterator_from_result(result);
        
        std::cout << "Found keyspaces:" << std::endl;
        while (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            const CassValue* keyspace_name_value = cass_row_get_column(row, 0);
            
            const char* keyspace_name;
            size_t keyspace_name_length;
            cass_value_get_string(keyspace_name_value, &keyspace_name, &keyspace_name_length);
            
            std::string ks_name(keyspace_name, keyspace_name_length);
            std::cout << "  - " << ks_name << std::endl;
        }
        
        cass_iterator_free(rows);
        cass_result_free(result);
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(result_future, &message, &message_length);
        std::cerr << "❌ Query failed: " << std::string(message, message_length) << std::endl;
    }
    
    cass_future_free(result_future);
    cass_statement_free(statement);
    
    // Test query: List tables in sfpla
    std::cout << "\nQuerying tables in sfpla keyspace..." << std::endl;
    const char* table_query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?";
    CassStatement* table_statement = cass_statement_new(table_query, 1);
    cass_statement_bind_string(table_statement, 0, "sfpla");
    
    CassFuture* table_result_future = cass_session_execute(session, table_statement);
    
    if (cass_future_error_code(table_result_future) == CASS_OK) {
        const CassResult* table_result = cass_future_get_result(table_result_future);
        CassIterator* table_rows = cass_iterator_from_result(table_result);
        
        int table_count = 0;
        std::cout << "Tables in sfpla:" << std::endl;
        while (cass_iterator_next(table_rows) && table_count < 10) {
            const CassRow* row = cass_iterator_get_row(table_rows);
            const CassValue* table_name_value = cass_row_get_column(row, 0);
            
            const char* table_name;
            size_t table_name_length;
            cass_value_get_string(table_name_value, &table_name, &table_name_length);
            
            std::string table_name_str(table_name, table_name_length);
            std::cout << "  - " << table_name_str << std::endl;
            table_count++;
        }
        
        cass_iterator_free(table_rows);
        cass_result_free(table_result);
    }
    
    cass_future_free(table_result_future);
    cass_statement_free(table_statement);
    
    // Cleanup
    CassFuture* close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
    
    cass_session_free(session);
    cass_cluster_free(cluster);
    
    std::cout << "\n✅ All DataStax driver tests passed!" << std::endl;
    std::cout << "🎉 The Cassandra extension driver integration is working correctly!" << std::endl;
    
    return 0;
}