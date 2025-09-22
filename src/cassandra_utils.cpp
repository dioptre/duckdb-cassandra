#include "cassandra_utils.hpp"
#include "storage/cassandra_catalog.hpp"
#include "storage/cassandra_transaction.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include <sstream>

namespace duckdb {
namespace cassandra {

CassandraConfig CassandraConfig::FromConnectionString(const std::string& connection_string) {
    CassandraConfig config;
    
    // Parse connection string format: host=127.0.0.1 port=9042 keyspace=test username=user password=pass certfile=hex userkey=hex usercert=hex
    std::stringstream ss(connection_string);
    std::string token;
    
    while (std::getline(ss, token, ' ')) {
        auto pos = token.find('=');
        if (pos != std::string::npos) {
            std::string key = token.substr(0, pos);
            std::string value = token.substr(pos + 1);
            
            if (key == "host" || key == "contact_points") {
                config.contact_points = value;
            } else if (key == "port") {
                config.port = std::stoi(value);
            } else if (key == "keyspace") {
                config.keyspace = value;
            } else if (key == "username") {
                config.username = value;
            } else if (key == "password") {
                config.password = value;
            } else if (key == "consistency") {
                config.consistency = value;
            } else if (key == "ssl" || key == "use_ssl") {
                config.use_ssl = (value == "true" || value == "1" || value == "on");
            } else if (key == "certfile") {
                config.cert_file_hex = value;
                config.use_ssl = true;
            } else if (key == "userkey") {
                config.user_key_hex = value;
                config.use_ssl = true;
            } else if (key == "usercert") {
                config.user_cert_hex = value;
                config.use_ssl = true;
            } else if (key == "cacert") {
                config.ca_cert_hex = value;
                config.use_ssl = true;
            } else if (key == "verify_peer") {
                config.verify_peer_cert = (value == "true" || value == "1" || value == "on");
            } else if (key == "client_id") {
                config.client_id = value;
                config.use_astra = true;
            } else if (key == "client_secret") {
                config.client_secret = value;
                config.use_astra = true;
            } else if (key == "astra_host") {
                config.astra_host = value;
            } else if (key == "astra_port") {
                config.astra_port = std::stoi(value);
            } else if (key == "astra_dc") {
                config.astra_dc = value;
            } else if (key == "astra_ca_cert") {
                config.astra_ca_cert = value;
            } else if (key == "astra_client_cert") {
                config.astra_client_cert = value;
            } else if (key == "astra_client_key") {
                config.astra_client_key = value;
            }
        }
    }
    
    return config;
}

std::string CassandraConfig::DecodeHexToString(const std::string& hex) const {
    std::string result;
    for (size_t i = 0; i < hex.length(); i += 2) {
        std::string byte_str = hex.substr(i, 2);
        char byte = static_cast<char>(std::stoul(byte_str, nullptr, 16));
        result += byte;
    }
    return result;
}

bool CassandraConfig::HasSSLConfig() const {
    return use_ssl && (!cert_file_hex.empty() || !user_cert_hex.empty());
}

unique_ptr<Catalog> CassandraAttachCatalog(optional_ptr<StorageExtensionInfo> storage_info,
                                           ClientContext &context, AttachedDatabase &db, const string &name,
                                           AttachInfo &info, AttachOptions &options) {
    auto config = CassandraConfig::FromConnectionString(info.path);
    auto catalog = make_uniq<CassandraCatalog>(db, name, info.path, options.access_mode, config);
    return catalog;
}

unique_ptr<TransactionManager> CassandraCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                        AttachedDatabase &db,
                                                                        Catalog &catalog) {
    return make_uniq<CassandraTransactionManager>(db);
}

} // namespace cassandra
} // namespace duckdb