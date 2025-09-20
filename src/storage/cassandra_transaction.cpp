#include "cassandra_transaction.hpp"

namespace duckdb {
namespace cassandra {

CassandraTransactionManager::CassandraTransactionManager(AttachedDatabase &db) 
    : TransactionManager(db) {
}

Transaction &CassandraTransactionManager::StartTransaction(ClientContext &context) {
    // Create a simple transaction for Cassandra
    auto transaction = make_uniq<Transaction>(*this, context);
    auto &result = *transaction;
    lock_guard<mutex> l(transaction_lock);
    // Store transaction using a simple counter as key
    static transaction_t next_transaction_id = 1;
    transactions[next_transaction_id++] = std::move(transaction);
    return result;
}

ErrorData CassandraTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
    return ErrorData();
}

void CassandraTransactionManager::RollbackTransaction(Transaction &transaction) {
    // No-op for Cassandra
}

void CassandraTransactionManager::Checkpoint(ClientContext &context, bool force) {
    // No-op for Cassandra
}

} // namespace cassandra
} // namespace duckdb