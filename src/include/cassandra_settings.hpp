#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace cassandra {

struct CassandraSettings {
    static void SetContactPoints(ClientContext &context, SetScope scope, Value &parameter);
    static void SetPort(ClientContext &context, SetScope scope, Value &parameter);
    static void SetUsername(ClientContext &context, SetScope scope, Value &parameter);
    static void SetPassword(ClientContext &context, SetScope scope, Value &parameter);
    static void SetKeyspace(ClientContext &context, SetScope scope, Value &parameter);
    static void SetConsistency(ClientContext &context, SetScope scope, Value &parameter);
    
    // SSL/TLS settings
    static void SetUseSSL(ClientContext &context, SetScope scope, Value &parameter);
    static void SetCertFileHex(ClientContext &context, SetScope scope, Value &parameter);
    static void SetUserKeyHex(ClientContext &context, SetScope scope, Value &parameter);
    static void SetUserCertHex(ClientContext &context, SetScope scope, Value &parameter);

    static std::string GetContactPoints(ClientContext &context);
    static int GetPort(ClientContext &context);
    static std::string GetUsername(ClientContext &context);
    static std::string GetPassword(ClientContext &context);
    static std::string GetKeyspace(ClientContext &context);
    static std::string GetConsistency(ClientContext &context);
    
    // SSL/TLS getters
    static bool GetUseSSL(ClientContext &context);
    static std::string GetCertFileHex(ClientContext &context);
    static std::string GetUserKeyHex(ClientContext &context);
    static std::string GetUserCertHex(ClientContext &context);
};

} // namespace cassandra
} // namespace duckdb