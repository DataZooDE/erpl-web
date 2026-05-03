#include "graph_sharepoint_catalog.hpp"
#include "graph_sharepoint_client.hpp"
#include "tracing.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "yyjson.hpp"
#include <algorithm>
#include <unordered_set>

using namespace duckdb_yyjson;
using namespace duckdb;

namespace erpl_web {

// -------------------------------------------------------------------------------------------------
// Internal scan types (file-scope)
// -------------------------------------------------------------------------------------------------

struct SharePointListScanBindData : public duckdb::TableFunctionData {
	std::string site_id;
	std::string list_id;
	std::shared_ptr<HttpAuthParams> auth_params;
	std::vector<std::string> column_names;
	std::string json_response;  // fetched lazily on first scan call
	bool done = false;
};

static void SharePointListScan(duckdb::ClientContext &context,
                                duckdb::TableFunctionInput &data,
                                duckdb::DataChunk &output)
{
	auto &bind_data = data.bind_data->CastNoConst<SharePointListScanBindData>();

	if (bind_data.done) {
		output.SetCardinality(0);
		return;
	}

	// Lazy-fetch items on first invocation
	if (bind_data.json_response.empty()) {
		GraphSharePointClient client(bind_data.auth_params);
		bind_data.json_response = client.GetListItems(bind_data.site_id, bind_data.list_id);
	}

	yyjson_doc *doc = yyjson_read(bind_data.json_response.c_str(), bind_data.json_response.length(), 0);
	if (!doc) {
		throw InvalidInputException("SharePointListScan: failed to parse Graph API response");
	}

	yyjson_val *root = yyjson_doc_get_root(doc);
	yyjson_val *value_arr = yyjson_obj_get(root, "value");

	if (!value_arr || !yyjson_is_arr(value_arr)) {
		yyjson_doc_free(doc);
		bind_data.done = true;
		output.SetCardinality(0);
		return;
	}

	size_t count = yyjson_arr_size(value_arr);
	if (count > STANDARD_VECTOR_SIZE) {
		count = STANDARD_VECTOR_SIZE;
	}

	output.SetCardinality(count);

	size_t idx, max;
	yyjson_val *item;
	idx_t row = 0;

	yyjson_arr_foreach(value_arr, idx, max, item) {
		if (row >= count) { break; }

		yyjson_val *fields_obj = yyjson_obj_get(item, "fields");

		for (size_t col = 0; col < bind_data.column_names.size(); col++) {
			const std::string &col_name = bind_data.column_names[col];

			if (col_name == "id") {
				yyjson_val *id_val = yyjson_obj_get(item, "id");
				output.SetValue(col, row, id_val && yyjson_is_str(id_val) ?
				    Value(yyjson_get_str(id_val)) : Value());
			} else if (fields_obj) {
				yyjson_val *field_val = yyjson_obj_get(fields_obj, col_name.c_str());
				if (field_val) {
					if (yyjson_is_str(field_val)) {
						output.SetValue(col, row, Value(yyjson_get_str(field_val)));
					} else if (yyjson_is_num(field_val)) {
						output.SetValue(col, row, Value(std::to_string(yyjson_get_num(field_val))));
					} else if (yyjson_is_bool(field_val)) {
						output.SetValue(col, row, Value(yyjson_get_bool(field_val) ? "true" : "false"));
					} else if (yyjson_is_null(field_val)) {
						output.SetValue(col, row, Value());
					} else {
						// Complex object — serialize as JSON string
						size_t json_len;
						char *json_str = yyjson_val_write(field_val, 0, &json_len);
						if (json_str) {
							output.SetValue(col, row, Value(std::string(json_str, json_len)));
							free(json_str);
						} else {
							output.SetValue(col, row, Value());
						}
					}
				} else {
					output.SetValue(col, row, Value());
				}
			} else {
				output.SetValue(col, row, Value());
			}
		}

		row++;
	}

	yyjson_doc_free(doc);
	bind_data.done = true;
}

// -------------------------------------------------------------------------------------------------
// SharePointSchemaEntry Implementation
// -------------------------------------------------------------------------------------------------

SharePointSchemaEntry::SharePointSchemaEntry(duckdb::Catalog &catalog, duckdb::CreateSchemaInfo &info)
    : duckdb::SchemaCatalogEntry(catalog, info), tables_loaded(false)
{
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointSchemaEntry::CreateTable(duckdb::CatalogTransaction transaction, duckdb::BoundCreateTableInfo &info) {
	throw duckdb::NotImplementedException("Creating tables in SharePoint schemas is not supported");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointSchemaEntry::CreateFunction(duckdb::CatalogTransaction transaction, duckdb::CreateFunctionInfo &info) {
	throw duckdb::NotImplementedException("Creating functions in SharePoint schemas is not supported");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointSchemaEntry::CreateIndex(duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo &info, duckdb::TableCatalogEntry &table) {
	throw duckdb::NotImplementedException("Creating indexes in SharePoint schemas is not supported");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointSchemaEntry::CreateView(duckdb::CatalogTransaction transaction, duckdb::CreateViewInfo &info) {
	throw duckdb::NotImplementedException("Creating views in SharePoint schemas is not supported");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointSchemaEntry::CreateSequence(duckdb::CatalogTransaction transaction, duckdb::CreateSequenceInfo &info) {
	throw duckdb::NotImplementedException("Creating sequences in SharePoint schemas is not supported");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointSchemaEntry::CreateTableFunction(duckdb::CatalogTransaction transaction, duckdb::CreateTableFunctionInfo &info) {
	throw duckdb::NotImplementedException("Creating table functions in SharePoint schemas is not supported");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointSchemaEntry::CreateCopyFunction(duckdb::CatalogTransaction transaction, duckdb::CreateCopyFunctionInfo &info) {
	throw duckdb::NotImplementedException("Creating copy functions in SharePoint schemas is not supported");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointSchemaEntry::CreatePragmaFunction(duckdb::CatalogTransaction transaction, duckdb::CreatePragmaFunctionInfo &info) {
	throw duckdb::NotImplementedException("Creating pragma functions in SharePoint schemas is not supported");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointSchemaEntry::CreateCollation(duckdb::CatalogTransaction transaction, duckdb::CreateCollationInfo &info) {
	throw duckdb::NotImplementedException("Creating collations in SharePoint schemas is not supported");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointSchemaEntry::CreateType(duckdb::CatalogTransaction transaction, duckdb::CreateTypeInfo &info) {
	throw duckdb::NotImplementedException("Creating types in SharePoint schemas is not supported");
}

void SharePointSchemaEntry::Alter(duckdb::CatalogTransaction transaction, duckdb::AlterInfo &info) {
	throw duckdb::NotImplementedException("Altering SharePoint schemas is not supported");
}

void SharePointSchemaEntry::Scan(duckdb::ClientContext &context, duckdb::CatalogType type, const std::function<void(duckdb::CatalogEntry &)> &callback) {
	if (type == duckdb::CatalogType::TABLE_ENTRY) {
		std::lock_guard<std::mutex> lock(tables_mutex);
		if (!tables_loaded) {
			LoadTables();
			tables_loaded = true;
		}
		for (auto &table_pair : table_entries) {
			callback(*table_pair.second);
		}
	}
}

void SharePointSchemaEntry::Scan(duckdb::CatalogType type, const std::function<void(duckdb::CatalogEntry &)> &callback) {
	if (type == duckdb::CatalogType::TABLE_ENTRY) {
		std::lock_guard<std::mutex> lock(tables_mutex);
		if (!tables_loaded) {
			LoadTables();
			tables_loaded = true;
		}
		for (auto &table_pair : table_entries) {
			callback(*table_pair.second);
		}
	}
}

void SharePointSchemaEntry::DropEntry(duckdb::ClientContext &context, duckdb::DropInfo &info) {
	throw duckdb::NotImplementedException("Dropping entries from SharePoint schemas is not supported");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointSchemaEntry::GetEntry(duckdb::CatalogTransaction transaction, duckdb::CatalogType type, const std::string &name) {
	if (type == duckdb::CatalogType::TABLE_ENTRY) {
		std::lock_guard<std::mutex> lock(tables_mutex);
		if (!tables_loaded) {
			LoadTables();
			tables_loaded = true;
		}
		auto it = table_entries.find(name);
		if (it != table_entries.end()) {
			return it->second.get();
		}
	}
	return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointSchemaEntry::LookupEntry(duckdb::CatalogTransaction transaction, const duckdb::EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() == duckdb::CatalogType::TABLE_ENTRY) {
		std::lock_guard<std::mutex> lock(tables_mutex);
		if (!tables_loaded) {
			LoadTables();
			tables_loaded = true;
		}
		auto it = table_entries.find(lookup_info.GetEntryName());
		if (it != table_entries.end()) {
			return it->second.get();
		}
	}
	return nullptr;
}

void SharePointSchemaEntry::LoadTables() {
	// Must only be called while holding tables_mutex
	auto &sp_catalog = static_cast<SharePointCatalog &>(catalog);
	const std::string &site_id = sp_catalog.GetSiteId();
	auto auth_params = sp_catalog.GetAuthParams();
	const std::string &secret_name = sp_catalog.GetSecretName();

	try {
		GraphSharePointClient sp_client(auth_params);

		// Fetch all lists for the site
		const std::string lists_json = sp_client.ListLists(site_id);

		yyjson_doc *lists_doc = yyjson_read(lists_json.c_str(), lists_json.length(), 0);
		if (!lists_doc) {
			ERPL_TRACE_WARN("SHAREPOINT_CATALOG", "Failed to parse lists JSON for site: " + site_id);
			return;
		}

		yyjson_val *lists_root = yyjson_doc_get_root(lists_doc);
		yyjson_val *lists_arr = yyjson_obj_get(lists_root, "value");

		if (!lists_arr || !yyjson_is_arr(lists_arr)) {
			yyjson_doc_free(lists_doc);
			return;
		}

		table_entries.clear();

		size_t list_idx, list_max;
		yyjson_val *list_item;

		yyjson_arr_foreach(lists_arr, list_idx, list_max, list_item) {
			// Extract list id and displayName
			yyjson_val *list_id_val = yyjson_obj_get(list_item, "id");
			yyjson_val *display_name_val = yyjson_obj_get(list_item, "displayName");

			if (!list_id_val || !yyjson_is_str(list_id_val)) { continue; }

			std::string list_id = yyjson_get_str(list_id_val);
			std::string table_key = list_id;  // fall back to ID as key

			if (display_name_val && yyjson_is_str(display_name_val)) {
				std::string display_name = yyjson_get_str(display_name_val);
				if (!display_name.empty()) {
					table_key = display_name;
				}
			}

			// Fetch columns for this list to build the schema
			std::string cols_json;
			try {
				cols_json = sp_client.GetListColumns(site_id, list_id);
			} catch (const std::exception &e) {
				ERPL_TRACE_WARN("SHAREPOINT_CATALOG", "Failed to fetch columns for list " + list_id + ": " + std::string(e.what()));
				continue;
			}

			yyjson_doc *cols_doc = yyjson_read(cols_json.c_str(), cols_json.length(), 0);
			if (!cols_doc) { continue; }

			yyjson_val *cols_root = yyjson_doc_get_root(cols_doc);
			yyjson_val *cols_arr = yyjson_obj_get(cols_root, "value");

			duckdb::CreateTableInfo table_info;
			table_info.table = table_key;
			table_info.schema = name;

			// Always include id as first column
			table_info.columns.AddColumn(duckdb::ColumnDefinition("id", duckdb::LogicalType::VARCHAR));
			std::vector<std::string> column_names = {"id"};
			std::unordered_set<std::string> seen_names = {"id"};

			if (cols_arr && yyjson_is_arr(cols_arr)) {
				size_t col_idx, col_max;
				yyjson_val *col;

				yyjson_arr_foreach(cols_arr, col_idx, col_max, col) {
					yyjson_val *name_val = yyjson_obj_get(col, "name");
					if (!name_val || !yyjson_is_str(name_val)) { continue; }

					std::string col_name = yyjson_get_str(name_val);

					// Skip internal/system columns
					if (col_name.empty() || col_name[0] == '_') { continue; }
					if (col_name == "Edit" || col_name == "LinkTitle" ||
					    col_name == "LinkTitleNoMenu" || col_name == "DocIcon" ||
					    col_name == "ItemChildCount" || col_name == "FolderChildCount" ||
					    col_name == "AppAuthor" || col_name == "AppEditor") {
						continue;
					}

					// Skip case-insensitive duplicates (e.g. "ID" collides with "id")
					std::string lower_name = col_name;
					std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
					if (!seen_names.insert(lower_name).second) { continue; }

					table_info.columns.AddColumn(duckdb::ColumnDefinition(col_name, duckdb::LogicalType::VARCHAR));
					column_names.push_back(col_name);
				}
			}

			yyjson_doc_free(cols_doc);

			auto table_entry = duckdb::make_uniq<SharePointTableEntry>(
			    catalog, *this, table_info,
			    site_id, list_id, secret_name, auth_params);

			table_entries[table_key] = std::move(table_entry);
		}

		yyjson_doc_free(lists_doc);

	} catch (const std::exception &e) {
		ERPL_TRACE_ERROR("SHAREPOINT_CATALOG", "Failed to load tables for site " + site_id + ": " + std::string(e.what()));
		table_entries.clear();
	}
}

// -------------------------------------------------------------------------------------------------
// SharePointTableEntry Implementation
// -------------------------------------------------------------------------------------------------

SharePointTableEntry::SharePointTableEntry(duckdb::Catalog &catalog,
                                           duckdb::SchemaCatalogEntry &schema,
                                           duckdb::CreateTableInfo &info,
                                           const std::string &site_id,
                                           const std::string &list_id,
                                           const std::string &secret_name,
                                           std::shared_ptr<HttpAuthParams> auth_params)
    : duckdb::TableCatalogEntry(catalog, schema, info),
      site_id_(site_id),
      list_id_(list_id),
      secret_name_(secret_name),
      auth_params_(auth_params)
{
	// Populate column_names from CreateTableInfo columns
	for (const auto &col : info.columns.Logical()) {
		column_names_.push_back(col.Name());
	}
}

duckdb::unique_ptr<duckdb::BaseStatistics> SharePointTableEntry::GetStatistics(duckdb::ClientContext &context, duckdb::column_t column_id) {
	return nullptr;
}

duckdb::TableFunction SharePointTableEntry::GetScanFunction(duckdb::ClientContext &context, duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	auto scan_bind_data = duckdb::make_uniq<SharePointListScanBindData>();
	scan_bind_data->site_id = site_id_;
	scan_bind_data->list_id = list_id_;
	scan_bind_data->auth_params = auth_params_;
	scan_bind_data->column_names = column_names_;

	bind_data = std::move(scan_bind_data);

	duckdb::TableFunction table_function("sharepoint_list_scan", {}, SharePointListScan, nullptr, nullptr);
	return table_function;
}

duckdb::TableStorageInfo SharePointTableEntry::GetStorageInfo(duckdb::ClientContext &context) {
	return duckdb::TableStorageInfo();
}

void SharePointTableEntry::BindUpdateConstraints(duckdb::Binder &binder, duckdb::LogicalGet &get, duckdb::LogicalProjection &proj, duckdb::LogicalUpdate &update, duckdb::ClientContext &context) {
	throw duckdb::NotImplementedException("Updates are not supported on SharePoint tables");
}

// -------------------------------------------------------------------------------------------------
// SharePointCatalog Implementation
// -------------------------------------------------------------------------------------------------

SharePointCatalog::SharePointCatalog(duckdb::AttachedDatabase &db,
                                     const std::string &site_name_or_id,
                                     const std::string &secret_name,
                                     std::shared_ptr<HttpAuthParams> auth_params)
    : duckdb::Catalog(db),
      secret_name_(secret_name),
      auth_params_(auth_params),
      path_(site_name_or_id)
{
	// Resolve site name/ID to a canonical site ID via the Graph API
	GraphSharePointClient sp_client(auth_params_);
	site_id_ = sp_client.ResolveSiteId(site_name_or_id);

	duckdb::CreateSchemaInfo schema_info;
	schema_info.schema = "main";
	main_schema_ = duckdb::make_uniq<SharePointSchemaEntry>(*this, schema_info);
}

duckdb::string SharePointCatalog::GetCatalogType() {
	return "sharepoint";
}

void SharePointCatalog::Initialize(bool load_builtin) {
	// Nothing to initialize
}

void SharePointCatalog::Initialize(duckdb::optional_ptr<duckdb::ClientContext> context, bool load_builtin) {
	// Nothing to initialize
}

void SharePointCatalog::FinalizeLoad(duckdb::optional_ptr<duckdb::ClientContext> context) {
	// Nothing to finalize
}

duckdb::string SharePointCatalog::GetDBPath() {
	return path_;
}

bool SharePointCatalog::InMemory() {
	return true;
}

bool SharePointCatalog::SupportsTimeTravel() const {
	return false;
}

duckdb::string SharePointCatalog::GetDefaultSchema() const {
	return "main";
}

duckdb::DatabaseSize SharePointCatalog::GetDatabaseSize(duckdb::ClientContext &context) {
	return duckdb::DatabaseSize();
}

duckdb::vector<duckdb::MetadataBlockInfo> SharePointCatalog::GetMetadataInfo(duckdb::ClientContext &context) {
	return duckdb::vector<duckdb::MetadataBlockInfo>();
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> SharePointCatalog::LookupSchema(duckdb::CatalogTransaction transaction, const duckdb::EntryLookupInfo &lookup_info, duckdb::OnEntryNotFound if_not_found) {
	if (lookup_info.GetEntryName() == "main") {
		return main_schema_.get();
	}
	if (if_not_found == duckdb::OnEntryNotFound::THROW_EXCEPTION) {
		throw duckdb::CatalogException("Schema \"%s\" not found", lookup_info.GetEntryName());
	}
	return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SharePointCatalog::CreateSchema(duckdb::CatalogTransaction transaction, duckdb::CreateSchemaInfo &info) {
	throw duckdb::NotImplementedException("Creating schemas in SharePoint catalogs is not supported");
}

void SharePointCatalog::ScanSchemas(duckdb::ClientContext &context, std::function<void(duckdb::SchemaCatalogEntry &)> callback) {
	callback(*main_schema_);
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> SharePointCatalog::GetSchema(duckdb::CatalogTransaction transaction, const std::string &schema_name, duckdb::OnEntryNotFound if_not_found, duckdb::QueryErrorContext error_context) {
	if (schema_name == "main") {
		return main_schema_.get();
	}
	if (if_not_found == duckdb::OnEntryNotFound::THROW_EXCEPTION) {
		throw duckdb::CatalogException("Schema \"%s\" not found", schema_name);
	}
	return nullptr;
}

duckdb::PhysicalOperator &SharePointCatalog::PlanCreateTableAs(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalCreateTable &op, duckdb::PhysicalOperator &plan) {
	throw duckdb::NotImplementedException("CREATE TABLE AS is not supported on SharePoint catalogs");
}

duckdb::PhysicalOperator &SharePointCatalog::PlanInsert(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalInsert &op, duckdb::optional_ptr<duckdb::PhysicalOperator> plan) {
	throw duckdb::NotImplementedException("INSERT is not supported on SharePoint catalogs");
}

duckdb::PhysicalOperator &SharePointCatalog::PlanDelete(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalDelete &op, duckdb::PhysicalOperator &plan) {
	throw duckdb::NotImplementedException("DELETE is not supported on SharePoint catalogs");
}

duckdb::PhysicalOperator &SharePointCatalog::PlanUpdate(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner, duckdb::LogicalUpdate &op, duckdb::PhysicalOperator &plan) {
	throw duckdb::NotImplementedException("UPDATE is not supported on SharePoint catalogs");
}

duckdb::unique_ptr<duckdb::LogicalOperator> SharePointCatalog::BindCreateIndex(duckdb::Binder &binder, duckdb::CreateStatement &stmt, duckdb::TableCatalogEntry &table, duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
	throw duckdb::NotImplementedException("CREATE INDEX is not supported on SharePoint catalogs");
}

void SharePointCatalog::DropSchema(duckdb::ClientContext &context, duckdb::DropInfo &info) {
	throw duckdb::NotImplementedException("DROP SCHEMA is not supported on SharePoint catalogs");
}

SharePointSchemaEntry &SharePointCatalog::GetMainSchema() {
	return *main_schema_;
}

const std::string &SharePointCatalog::GetSiteId() const {
	return site_id_;
}

const std::string &SharePointCatalog::GetSecretName() const {
	return secret_name_;
}

std::shared_ptr<HttpAuthParams> SharePointCatalog::GetAuthParams() const {
	return auth_params_;
}

} // namespace erpl_web
