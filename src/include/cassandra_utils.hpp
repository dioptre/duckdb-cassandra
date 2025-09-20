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
    
    static CassandraConfig FromConnectionString(const std::string& connection_string);
    
    // Helper methods for SSL
    std::string DecodeHexToString(const std::string& hex) const;
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