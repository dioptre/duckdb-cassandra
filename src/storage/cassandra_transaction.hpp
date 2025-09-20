#pragma once

#include "duckdb.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {
namespace cassandra {

class CassandraTransactionManager : public TransactionManager {
public:
    CassandraTransactionManager(AttachedDatabase &db);
    
    Transaction &StartTransaction(ClientContext &context) override;
    ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
    void RollbackTransaction(Transaction &transaction) override;
    void Checkpoint(ClientContext &context, bool force = false) override;
    
private:
    mutex transaction_lock;
    unordered_map<transaction_t, unique_ptr<Transaction>> transactions;
};

} // namespace cassandra
} // namespace duckdb