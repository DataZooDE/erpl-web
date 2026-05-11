#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include "business_central_catalog.hpp"

namespace erpl_web {

// -----------------------------------------------------------------------
// BcTransaction — no-op transaction that creates BcTableEntry on demand
// -----------------------------------------------------------------------

class BcTransaction : public duckdb::Transaction {
public:
    BcTransaction(BcCatalog &bc_catalog, duckdb::TransactionManager &manager, duckdb::ClientContext &context);
    ~BcTransaction() override;

    void Start();
    void Commit();
    void Rollback();

    duckdb::optional_ptr<duckdb::CatalogEntry> GetCatalogEntry(const std::string &table_name);

    static BcTransaction &Get(duckdb::ClientContext &context, duckdb::Catalog &catalog);

private:
    BcCatalog &bc_catalog_;
    duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> catalog_entries_;
};

// -----------------------------------------------------------------------
// BcTransactionManager
// -----------------------------------------------------------------------

class BcTransactionManager : public duckdb::TransactionManager {
public:
    BcTransactionManager(duckdb::AttachedDatabase &db, BcCatalog &bc_catalog);

    duckdb::Transaction &StartTransaction(duckdb::ClientContext &context) override;
    duckdb::ErrorData CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) override;
    void RollbackTransaction(duckdb::Transaction &transaction) override;
    void Checkpoint(duckdb::ClientContext &context, bool force = false) override;

private:
    BcCatalog &bc_catalog_;
    std::mutex transaction_lock_;
    duckdb::reference_map_t<duckdb::Transaction, duckdb::unique_ptr<BcTransaction>> transactions_;
};

// -----------------------------------------------------------------------
// BcStorageExtension
// -----------------------------------------------------------------------

class BcStorageExtension : public duckdb::StorageExtension {
public:
    BcStorageExtension();
};

duckdb::unique_ptr<BcStorageExtension> CreateBcStorageExtension();

} // namespace erpl_web
