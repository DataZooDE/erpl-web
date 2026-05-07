#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

#include "graph_excel_catalog.hpp"

namespace erpl_web {

// -------------------------------------------------------------------------------------------------

class ExcelTransaction : public duckdb::Transaction {
public:
	ExcelTransaction(ExcelCatalog &catalog,
	                 duckdb::TransactionManager &manager,
	                 duckdb::ClientContext &context);
	~ExcelTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	static ExcelTransaction &Get(duckdb::ClientContext &context, duckdb::Catalog &catalog);
};

// -------------------------------------------------------------------------------------------------

class ExcelTransactionManager : public duckdb::TransactionManager {
public:
	ExcelTransactionManager(duckdb::AttachedDatabase &db, ExcelCatalog &catalog);

	duckdb::Transaction &StartTransaction(duckdb::ClientContext &context) override;
	duckdb::ErrorData CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) override;
	void RollbackTransaction(duckdb::Transaction &transaction) override;
	void Checkpoint(duckdb::ClientContext &context, bool force = false) override;

private:
	ExcelCatalog &catalog_;
	std::mutex transaction_lock_;
	duckdb::reference_map_t<duckdb::Transaction, duckdb::unique_ptr<ExcelTransaction>> transactions_;
};

// -------------------------------------------------------------------------------------------------

class ExcelStorageExtension : public duckdb::StorageExtension {
public:
	ExcelStorageExtension();
};

duckdb::unique_ptr<ExcelStorageExtension> CreateExcelStorageExtension();

} // namespace erpl_web
