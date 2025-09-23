#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include <string>
#include <vector>

namespace duckdb {
namespace cassandra {

struct CassandraConfig {
    std::string contact_points = "127.0.0.1";
    int port = 9042;
    std::string username;
    std::string password;
    std::string keyspace;
    std::string consistency = "ONE";
    
    // SSL/TLS Configuration
    bool use_ssl = false;
    std::string cert_file_hex;      // Hex-encoded certificate
    std::string user_key_hex;       // Hex-encoded private key  
    std::string user_cert_hex;      // Hex-encoded user certificate
    std::string ca_cert_hex;        // Hex-encoded CA certificate
    bool verify_peer_cert = true;
    
    // Base64 encoded SSL certificates (alternative format)
    std::string certfile_b64;       // Base64 encoded CA certificate
    std::string userkey_b64;        // Base64 encoded private key  
    std::string usercert_b64;       // Base64 encoded user certificate
    
    // Astra authentication (manual configuration, no SCB needed)
    bool use_astra = false;        // Flag to enable Astra mode
    std::string client_id;         // Astra client ID
    std::string client_secret;     // Astra client secret  
    std::string astra_host;        // Astra host (from config.json)
    int astra_port = 29042;        // Astra CQL port (from config.json)
    std::string astra_dc;          // Astra datacenter (localDC)
    
    // Astra SSL certificates (as strings, not hex)
    std::string astra_ca_cert;     // CA certificate content
    std::string astra_client_cert; // Client certificate content
    std::string astra_client_key;  // Private key content
    
    // Base64 encoded Astra SSL certificates (alternative format)
    std::string astra_ca_cert_b64;     // Base64 encoded CA certificate
    std::string astra_client_cert_b64; // Base64 encoded client certificate
    std::string astra_client_key_b64;  // Base64 encoded private key
    
    static CassandraConfig FromConnectionString(const std::string& connection_string);
    
    // Helper methods for SSL
    std::string DecodeHexToString(const std::string& hex) const;
    std::string DecodeBase64ToString(const std::string& base64) const;
    bool HasSSLConfig() const;
};

struct CassandraKeyspaceRef {
    std::string keyspace_name;
};

struct CassandraTableRef {
    std::string keyspace_name;
    std::string table_name;
    
    std::string GetQualifiedName() const {
        return keyspace_name + "." + table_name;
    }
};

// Forward declarations for storage extension functions
unique_ptr<Catalog> CassandraAttachCatalog(optional_ptr<StorageExtensionInfo> storage_info,
                                           ClientContext &context, AttachedDatabase &db, const string &name,
                                           AttachInfo &info, AttachOptions &options);

unique_ptr<TransactionManager> CassandraCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                 AttachedDatabase &db, Catalog &catalog);

class CassandraStorageExtension : public StorageExtension {
public:
    CassandraStorageExtension() {
        attach = CassandraAttachCatalog;
        create_transaction_manager = CassandraCreateTransactionManager;
        storage_info = nullptr;
    }
};

} // namespace cassandra  
} // namespace duckdb