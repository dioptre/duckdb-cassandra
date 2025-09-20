#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"

#include "cassandra_attach.hpp"
#include "cassandra_client.hpp"
#include "cassandra_extension.hpp"
#include "cassandra_scan.hpp"
#include "cassandra_settings.hpp"
#include "cassandra_utils.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include <iostream>

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {

    // Register table functions
    cassandra::CassandraScanFunction cassandra_scan_function;
    loader.RegisterFunction(cassandra_scan_function);

    cassandra::CassandraQueryFunction cassandra_query_function;
    loader.RegisterFunction(cassandra_query_function);

    // Don't register ATTACH function - let storage extension handle it
    // cassandra::CassandraAttachFunction cassandra_attach_function;
    // loader.RegisterFunction(cassandra_attach_function);

    auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
    std::cout << "DEBUG: Registering cassandra storage extension" << std::endl;
    auto storage_ext = make_uniq<cassandra::CassandraStorageExtension>();
    std::cout << "DEBUG: Storage extension created, attach function: " << (storage_ext->attach ? "SET" : "NULL") << std::endl;
    config.storage_extensions["cassandra"] = std::move(storage_ext);
    config.AddExtensionOption("cassandra_contact_points",
                              "Comma-separated list of Cassandra contact points",
                              LogicalType::VARCHAR,
                              Value("127.0.0.1"),
                              cassandra::CassandraSettings::SetContactPoints);
    
    config.AddExtensionOption("cassandra_port",
                              "Cassandra port number",
                              LogicalType::INTEGER,
                              Value(9042),
                              cassandra::CassandraSettings::SetPort);
    
    config.AddExtensionOption("cassandra_username",
                              "Cassandra username for authentication",
                              LogicalType::VARCHAR,
                              Value(""),
                              cassandra::CassandraSettings::SetUsername);
    
    config.AddExtensionOption("cassandra_password",
                              "Cassandra password for authentication",
                              LogicalType::VARCHAR,
                              Value(""),
                              cassandra::CassandraSettings::SetPassword);
    
    config.AddExtensionOption("cassandra_keyspace",
                              "Default Cassandra keyspace",
                              LogicalType::VARCHAR,
                              Value(""),
                              cassandra::CassandraSettings::SetKeyspace);
    
    config.AddExtensionOption("cassandra_consistency",
                              "Default consistency level",
                              LogicalType::VARCHAR,
                              Value("ONE"),
                              cassandra::CassandraSettings::SetConsistency);
    
    config.AddExtensionOption("cassandra_use_ssl",
                              "Enable SSL/TLS connections",
                              LogicalType::BOOLEAN,
                              Value(false),
                              cassandra::CassandraSettings::SetUseSSL);
    
    config.AddExtensionOption("cassandra_cert_file_hex",
                              "SSL certificate file in hex format",
                              LogicalType::VARCHAR,
                              Value(""),
                              cassandra::CassandraSettings::SetCertFileHex);
    
    config.AddExtensionOption("cassandra_user_key_hex",
                              "SSL private key file in hex format",
                              LogicalType::VARCHAR,
                              Value(""),
                              cassandra::CassandraSettings::SetUserKeyHex);
    
    config.AddExtensionOption("cassandra_user_cert_hex",
                              "SSL user certificate file in hex format",
                              LogicalType::VARCHAR,
                              Value(""),
                              cassandra::CassandraSettings::SetUserCertHex);
}

void CassandraExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string CassandraExtension::Name() {
    return "cassandra";
}

std::string CassandraExtension::Version() const {
#ifdef EXT_VERSION_CASSANDRA
    return EXT_VERSION_CASSANDRA;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(cassandra, loader) {
    duckdb::LoadInternal(loader);
}

}