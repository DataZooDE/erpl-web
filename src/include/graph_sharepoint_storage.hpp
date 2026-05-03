#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

#include "graph_sharepoint_catalog.hpp"

namespace erpl_web {

// -------------------------------------------------------------------------------------------------

class SharePointTransaction : public duckdb::Transaction {
public:
	SharePointTransaction(SharePointCatalog &sp_catalog,
	                      duckdb::TransactionManager &manager,
	                      duckdb::ClientContext &context);
	~SharePointTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	static SharePointTransaction &Get(duckdb::ClientContext &context, duckdb::Catalog &catalog);

private:
	SharePointCatalog &sp_catalog_;
};

// -------------------------------------------------------------------------------------------------

class SharePointTransactionManager : public duckdb::TransactionManager {
public:
	SharePointTransactionManager(duckdb::AttachedDatabase &db, SharePointCatalog &sp_catalog);

	duckdb::Transaction &StartTransaction(duckdb::ClientContext &context) override;
	duckdb::ErrorData CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) override;
	void RollbackTransaction(duckdb::Transaction &transaction) override;
	void Checkpoint(duckdb::ClientContext &context, bool force = false) override;

private:
	SharePointCatalog &sp_catalog_;
	std::mutex transaction_lock_;
	duckdb::reference_map_t<duckdb::Transaction, duckdb::unique_ptr<SharePointTransaction>> transactions_;
};

// -------------------------------------------------------------------------------------------------

class SharePointStorageExtension : public duckdb::StorageExtension {
public:
	SharePointStorageExtension();
};

duckdb::unique_ptr<SharePointStorageExtension> CreateSharePointStorageExtension();

} // namespace erpl_web
