#include <cassandra.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string>
#include <filesystem>

// Function to create a temporary SCB from parameters
std::string create_virtual_scb(const std::string& host, int port, const std::string& keyspace, 
                               const std::string& dc, const std::string& ca_cert, 
                               const std::string& client_cert, const std::string& client_key) {
    // Create temporary directory
    std::string temp_dir = "/tmp/virtual_scb_" + std::to_string(time(nullptr));
    std::filesystem::create_directory(temp_dir);
    
    // Write config.json
    std::ofstream config_file(temp_dir + "/config.json");
    config_file << "{\n";
    config_file << "  \"host\": \"" << host << "\",\n";
    config_file << "  \"port\": " << port << ",\n";
    config_file << "  \"cql_port\": " << port << ",\n";
    config_file << "  \"keyspace\": \"" << keyspace << "\",\n";
    config_file << "  \"localDC\": \"" << dc << "\",\n";
    config_file << "  \"caCertLocation\": \"./ca.crt\",\n";
    config_file << "  \"keyLocation\": \"./key\",\n";
    config_file << "  \"certLocation\": \"./cert\"\n";
    config_file << "}";
    config_file.close();
    
    // Write certificates (using existing bundle files for now)
    std::ifstream ca_src("/Users/andrewgrosser/Documents/duckdb-cassandra/test/bundle/ca.crt");
    std::ofstream ca_dst(temp_dir + "/ca.crt");
    ca_dst << ca_src.rdbuf();
    ca_src.close(); ca_dst.close();
    
    std::ifstream cert_src("/Users/andrewgrosser/Documents/duckdb-cassandra/test/bundle/cert");
    std::ofstream cert_dst(temp_dir + "/cert");
    cert_dst << cert_src.rdbuf();
    cert_src.close(); cert_dst.close();
    
    std::ifstream key_src("/Users/andrewgrosser/Documents/duckdb-cassandra/test/bundle/key");
    std::ofstream key_dst(temp_dir + "/key");
    key_dst << key_src.rdbuf();
    key_src.close(); key_dst.close();
    
    return temp_dir;
}

int main(int argc, char* argv[]) {
    CassCluster* cluster = cass_cluster_new();
    CassSession* session = cass_session_new();

    // Parameters that would come from CLI
    std::string astra_host = "d150140c-a487-44af-bf29-202e09205631-us-east1.db.astra.datastax.com";
    int astra_port = 29042;
    std::string keyspace = "movies";
    std::string local_dc = "us-east1";
    std::string client_id = "pZsmEXXItazOtLdNNqUduAjU";
    std::string client_secret = "bb.ATqd52qWDUaKmo83,JE2-PjE9OQyE1L6wCNpSa8P76Qc1BNPKvu.ROfp.tDoSRGOL8vEwNLF-8kMbWRYcrR+hmOlkve7+xIpBF-Xf5DCtHUX7wwL-D7y0ti2yeZBh";
    
    // Try manual configuration instead of SCB
    printf("Configuring Astra manually from CLI parameters...\n");
    
    // Set contact points and port directly
    cass_cluster_set_contact_points(cluster, astra_host.c_str());
    cass_cluster_set_port(cluster, astra_port);
    
    // Set local datacenter
    if (!local_dc.empty()) {
        // This might require a different approach - let's see if it works without
    }
    
    // Configure SSL manually with file contents
    CassSsl* ssl = cass_ssl_new();
    
    // Read and add CA certificate
    std::ifstream ca_file("/Users/andrewgrosser/Documents/duckdb-cassandra/test/bundle/ca.crt");
    std::string ca_cert((std::istreambuf_iterator<char>(ca_file)), std::istreambuf_iterator<char>());
    cass_ssl_add_trusted_cert(ssl, ca_cert.c_str());
    
    // Read and set client certificate
    std::ifstream cert_file("/Users/andrewgrosser/Documents/duckdb-cassandra/test/bundle/cert");
    std::string client_cert((std::istreambuf_iterator<char>(cert_file)), std::istreambuf_iterator<char>());
    cass_ssl_set_cert(ssl, client_cert.c_str());
    
    // Read and set private key
    std::ifstream key_file("/Users/andrewgrosser/Documents/duckdb-cassandra/test/bundle/key");
    std::string private_key((std::istreambuf_iterator<char>(key_file)), std::istreambuf_iterator<char>());
    cass_ssl_set_private_key(ssl, private_key.c_str(), "");
    
    // Apply SSL to cluster
    cass_cluster_set_ssl(cluster, ssl);
    cass_ssl_free(ssl);

    // Set credentials
    cass_cluster_set_credentials(cluster, client_id.c_str(), client_secret.c_str());

    // Increase the connection timeout
    cass_cluster_set_connect_timeout(cluster, 10000);

    CassFuture* connect_future = cass_session_connect(session, cluster);

    if (cass_future_error_code(connect_future) == CASS_OK) {
        printf("✅ Connected to Astra successfully!\n");

        // First, check what keyspaces exist
        const char* keyspace_query = "SELECT keyspace_name FROM system_schema.keyspaces";
        CassStatement* ks_statement = cass_statement_new(keyspace_query, 0);
        CassFuture* ks_result_future = cass_session_execute(session, ks_statement);
        
        if (cass_future_error_code(ks_result_future) == CASS_OK) {
            const CassResult* ks_result = cass_future_get_result(ks_result_future);
            CassIterator* ks_iterator = cass_iterator_from_result(ks_result);
            printf("Available keyspaces:\n");
            while (cass_iterator_next(ks_iterator)) {
                const CassRow* row = cass_iterator_get_row(ks_iterator);
                const CassValue* value = cass_row_get_column(row, 0);
                const char* keyspace_name;
                size_t keyspace_length;
                cass_value_get_string(value, &keyspace_name, &keyspace_length);
                printf("  - %.*s\n", (int)keyspace_length, keyspace_name);
            }
            cass_iterator_free(ks_iterator);
            cass_result_free(ks_result);
        }
        cass_statement_free(ks_statement);
        cass_future_free(ks_result_future);
        
        // Check what tables exist in movies keyspace
        const char* table_query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name='movies'";
        CassStatement* table_statement = cass_statement_new(table_query, 0);
        CassFuture* table_result_future = cass_session_execute(session, table_statement);
        
        if (cass_future_error_code(table_result_future) == CASS_OK) {
            const CassResult* table_result = cass_future_get_result(table_result_future);
            CassIterator* table_iterator = cass_iterator_from_result(table_result);
            printf("Available tables in 'movies' keyspace:\n");
            while (cass_iterator_next(table_iterator)) {
                const CassRow* row = cass_iterator_get_row(table_iterator);
                const CassValue* value = cass_row_get_column(row, 0);
                const char* table_name;
                size_t table_length;
                cass_value_get_string(value, &table_name, &table_length);
                printf("  - %.*s\n", (int)table_length, table_name);
            }
            cass_iterator_free(table_iterator);
            cass_result_free(table_result);
        }
        cass_statement_free(table_statement);
        cass_future_free(table_result_future);
        
        // Test basic query with all_types table
        const char* query = "SELECT * FROM movies.all_types LIMIT 3";
        CassStatement* statement = cass_statement_new(query, 0);

        CassFuture* result_future = cass_session_execute(session, statement);

        if (cass_future_error_code(result_future) == CASS_OK) {
            printf("✅ Query executed successfully!\n");
            
            const CassResult* result = cass_future_get_result(result_future);
            size_t column_count = cass_result_column_count(result);
            printf("Columns: %zu\n", column_count);
            
            CassIterator* iterator = cass_iterator_from_result(result);
            int row_count = 0;
            while (cass_iterator_next(iterator) && row_count < 3) {
                const CassRow* row = cass_iterator_get_row(iterator);
                printf("Row %d: Found data\n", row_count + 1);
                row_count++;
            }
            
            cass_iterator_free(iterator);
            cass_result_free(result);
        } else {
            const char* message;
            size_t message_length;
            cass_future_error_message(result_future, &message, &message_length);
            fprintf(stderr, "❌ Unable to run query: '%.*s'\n", (int)message_length, message);
        }

        cass_statement_free(statement);
        cass_future_free(result_future);
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(connect_future, &message, &message_length);
        fprintf(stderr, "❌ Unable to connect: '%.*s'\n", (int)message_length, message);
    }

    cass_future_free(connect_future);
    cass_cluster_free(cluster);
    cass_session_free(session);

    return 0;
}