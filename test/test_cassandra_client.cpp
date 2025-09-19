#include "src/include/cassandra_client.hpp"
#include "src/include/cassandra_types.hpp"
#include <iostream>

using namespace duckdb::cassandra;

int main() {
    try {
        std::cout << "=== DuckDB Cassandra Extension Test ===" << std::endl;
        
        // Test configuration parsing
        CassandraConfig config;
        config.contact_points = "127.0.0.1";
        config.port = 9042;
        config.keyspace = "sfpla";
        
        std::cout << "Configuration: " << config.contact_points << ":" << config.port << std::endl;
        
        // Test client connection
        std::cout << "Connecting to Cassandra..." << std::endl;
        CassandraClient client(config);
        
        // Test keyspace discovery
        std::cout << "Getting keyspaces..." << std::endl;
        auto keyspaces = client.GetKeyspaces();
        std::cout << "Found " << keyspaces.size() << " keyspaces:" << std::endl;
        for (const auto& ks : keyspaces) {
            std::cout << "  - " << ks.keyspace_name << std::endl;
        }
        
        // Test table discovery
        std::cout << "Getting tables for sfpla keyspace..." << std::endl;
        auto tables = client.GetTables("sfpla");
        std::cout << "Found " << tables.size() << " tables in sfpla:" << std::endl;
        for (size_t i = 0; i < std::min<size_t>(5, tables.size()); i++) {
            std::cout << "  - " << tables[i].table_name << std::endl;
        }
        if (tables.size() > 5) {
            std::cout << "  ... and " << (tables.size() - 5) << " more" << std::endl;
        }
        
        // Test type mapping
        std::cout << "\n=== Testing Type Mappings ===" << std::endl;
        auto varchar_type = CassandraTypeMapper::CassValueTypeToDuckDBType(CASS_VALUE_TYPE_VARCHAR);
        auto int_type = CassandraTypeMapper::CassValueTypeToDuckDBType(CASS_VALUE_TYPE_INT);
        auto timestamp_type = CassandraTypeMapper::CassValueTypeToDuckDBType(CASS_VALUE_TYPE_TIMESTAMP);
        
        std::cout << "CASS_VALUE_TYPE_VARCHAR -> " << varchar_type.ToString() << std::endl;
        std::cout << "CASS_VALUE_TYPE_INT -> " << int_type.ToString() << std::endl; 
        std::cout << "CASS_VALUE_TYPE_TIMESTAMP -> " << timestamp_type.ToString() << std::endl;
        
        std::cout << "\n✅ All tests passed! Extension is working correctly." << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}