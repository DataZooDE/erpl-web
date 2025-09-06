#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "odata_catalog.hpp"
#include "odata_storage.hpp"

namespace erpl_web {

class ODataTransaction; // forward declaration
class ODataCatalog; // forward declaration

ODataTransaction &GetODataTransaction(duckdb::CatalogTransaction transaction);

// -------------------------------------------------------------------------------------------------
class ODataTransaction : public duckdb::Transaction {
public:
	ODataTransaction(ODataCatalog &odata_catalog, duckdb::TransactionManager &manager, duckdb::ClientContext &context);
	~ODataTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	duckdb::optional_ptr<duckdb::CatalogEntry> GetCatalogEntry(const std::string &table_name);
	void DropEntry(duckdb::CatalogType type, const std::string &table_name, bool cascade);
	void ClearTableEntry(const std::string &table_name);

	static ODataTransaction &Get(duckdb::ClientContext &context, duckdb::Catalog &catalog);

private:
	ODataCatalog &odata_catalog;
	duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> catalog_entries;
};

// -------------------------------------------------------------------------------------------------

class ODataTransactionManager : public duckdb::TransactionManager {
public:
	ODataTransactionManager(duckdb::AttachedDatabase &db_p, ODataCatalog &odata_catalog);

	duckdb::Transaction &StartTransaction(duckdb::ClientContext &context) override;
	duckdb::ErrorData CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) override;
	void RollbackTransaction(duckdb::Transaction &transaction) override;

	void Checkpoint(duckdb::ClientContext &context, bool force = false) override;

private:
	ODataCatalog &odata_catalog;
	std::mutex transaction_lock;
	duckdb::reference_map_t<duckdb::Transaction, duckdb::unique_ptr<ODataTransaction>> transactions;
};

} // namespace erpl_web
