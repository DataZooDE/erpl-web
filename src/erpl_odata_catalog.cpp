#include "erpl_odata_catalog.hpp"
#include "erpl_odata_read_functions.hpp"
#include "erpl_odata_attach_functions.hpp"
#include "erpl_odata_client.hpp"
#include "erpl_tracing.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace erpl_web {


// -------------------------------------------------------------------------------------------------

ODataSchemaEntry::ODataSchemaEntry(duckdb::Catalog &catalog, duckdb::CreateSchemaInfo &info) 
    : duckdb::SchemaCatalogEntry(catalog, info)
{ }

void ODataSchemaEntry::Scan(duckdb::ClientContext &context, duckdb::CatalogType type, const std::function<void(duckdb::CatalogEntry &)> &callback)
{
    ERPL_TRACE_DEBUG("ODATA_CATALOG", "Scanning OData schema for type: " + std::to_string(static_cast<int>(type)));
    
    auto &odata_catalog = catalog.Cast<ODataCatalog>();
    if (type != duckdb::CatalogType::TABLE_ENTRY) {
        ERPL_TRACE_DEBUG("ODATA_CATALOG", "Skipping non-table catalog type");
        return;
    }

    auto table_names = odata_catalog.GetTableNames();
    ERPL_TRACE_INFO("ODATA_CATALOG", "Found " + std::to_string(table_names.size()) + " tables in OData schema");
    
	for (auto &entry_name : table_names) {
		ERPL_TRACE_DEBUG("ODATA_CATALOG", "Processing table entry: " + entry_name);
		callback(*GetEntry(GetCatalogTransaction(context), type, entry_name));
	}
}

void ODataSchemaEntry::Scan(duckdb::CatalogType type, const std::function<void(duckdb::CatalogEntry &)> &callback)
{
    throw InternalException("Scan");
}

void ODataSchemaEntry::DropEntry(duckdb::ClientContext &context, duckdb::DropInfo &info)
{
    // TODO: Implement this
}

duckdb::optional_ptr<duckdb::CatalogEntry> ODataSchemaEntry::GetEntry(duckdb::CatalogTransaction transaction, duckdb::CatalogType type, const std::string &name)
{
    ERPL_TRACE_DEBUG("ODATA_CATALOG", "Getting catalog entry for: " + name + " (type: " + std::to_string(static_cast<int>(type)) + ")");
    
    auto &odata_transaction = GetODataTransaction(transaction); 

    switch (type) {
        case duckdb::CatalogType::TABLE_ENTRY:
            ERPL_TRACE_DEBUG("ODATA_CATALOG", "Retrieving table entry: " + name);
            return odata_transaction.GetCatalogEntry(name);
        default:
            ERPL_TRACE_WARN("ODATA_CATALOG", "Unsupported catalog type: " + std::to_string(static_cast<int>(type)));
            return nullptr;
	}
}

duckdb::optional_ptr<duckdb::CatalogEntry> ODataSchemaEntry::LookupEntry(duckdb::CatalogTransaction transaction, const duckdb::EntryLookupInfo &lookup_info)
{
    return GetEntry(transaction, lookup_info.GetCatalogType(), lookup_info.GetEntryName());
}

#pragma region NotRelevantImplementations

duckdb::optional_ptr<duckdb::CatalogEntry> ODataSchemaEntry::CreateTable(duckdb::CatalogTransaction transaction, duckdb::BoundCreateTableInfo &info)
{
    throw duckdb::BinderException("OData does not support CREATING Tables");
}

duckdb::optional_ptr<duckdb::CatalogEntry> ODataSchemaEntry::CreateFunction(duckdb::CatalogTransaction transaction, duckdb::CreateFunctionInfo &info)
{
    throw duckdb::BinderException("OData does not support CREATING Functions");
}

duckdb::optional_ptr<duckdb::CatalogEntry> ODataSchemaEntry::CreateIndex(duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo &info, duckdb::TableCatalogEntry &table)
{
    throw duckdb::BinderException("OData does not support CREATING Indexes");
}

duckdb::optional_ptr<duckdb::CatalogEntry> ODataSchemaEntry::CreateView(duckdb::CatalogTransaction transaction, duckdb::CreateViewInfo &info)
{
    throw duckdb::BinderException("OData does not support CREATING Views");
}

duckdb::optional_ptr<duckdb::CatalogEntry> ODataSchemaEntry::CreateSequence(duckdb::CatalogTransaction transaction, duckdb::CreateSequenceInfo &info)
{
    throw duckdb::BinderException("OData does not support CREATING Sequences");
}

duckdb::optional_ptr<duckdb::CatalogEntry> ODataSchemaEntry::CreateTableFunction(duckdb::CatalogTransaction transaction, duckdb::CreateTableFunctionInfo &info)
{
    throw duckdb::BinderException("OData does not support CREATING Table Functions");
}

duckdb::optional_ptr<duckdb::CatalogEntry> ODataSchemaEntry::CreateCopyFunction(duckdb::CatalogTransaction transaction, duckdb::CreateCopyFunctionInfo &info)
{
    throw duckdb::BinderException("OData does not support CREATING Copy Functions");
}

duckdb::optional_ptr<duckdb::CatalogEntry> ODataSchemaEntry::CreatePragmaFunction(duckdb::CatalogTransaction transaction, duckdb::CreatePragmaFunctionInfo &info)
{
    throw duckdb::BinderException("OData does not support CREATING Pragma Functions");
}

duckdb::optional_ptr<duckdb::CatalogEntry> ODataSchemaEntry::CreateCollation(duckdb::CatalogTransaction transaction, duckdb::CreateCollationInfo &info)
{
    throw duckdb::BinderException("OData does not support CREATING Collations");
}

duckdb::optional_ptr<duckdb::CatalogEntry> ODataSchemaEntry::CreateType(duckdb::CatalogTransaction transaction, duckdb::CreateTypeInfo &info)
{
    throw duckdb::BinderException("OData does not support CREATING Types");
}

void ODataSchemaEntry::Alter(duckdb::CatalogTransaction transaction, duckdb::AlterInfo &info)
{
    throw duckdb::BinderException("OData does not support ALTERING");
}

#pragma endregion

// -------------------------------------------------------------------------------------------------

ODataTableEntry::ODataTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, duckdb::CreateTableInfo &info)
    : duckdb::TableCatalogEntry(catalog, schema, info)
{ }

unique_ptr<duckdb::BaseStatistics> ODataTableEntry::GetStatistics(duckdb::ClientContext &context, duckdb::column_t column_id)
{
    return nullptr;
}

TableFunction ODataTableEntry::GetScanFunction(duckdb::ClientContext &context, unique_ptr<duckdb::FunctionData> &bind_data)
{
    auto &odata_catalog = catalog.Cast<ODataCatalog>();

    auto base_url = odata_catalog.ServiceUrl();
    auto entity_set_url = HttpUrl::MergeWithBaseUrlIfRelative(base_url, name);
    auto auth_params = HttpAuthParams::FromDuckDbSecrets(context, entity_set_url.ToString());
    bind_data = std::move(ODataReadBindData::FromEntitySetRoot(entity_set_url, auth_params));
    
    auto result = CreateODataReadFunction();
    return result.functions[0];
}

TableStorageInfo ODataTableEntry::GetStorageInfo(duckdb::ClientContext &context)
{
    TableStorageInfo result;
    return result;
}

void ODataTableEntry::BindUpdateConstraints(duckdb::Binder &binder, duckdb::LogicalGet &get, duckdb::LogicalProjection &proj, duckdb::LogicalUpdate &update, duckdb::ClientContext &context)
{ }

// -------------------------------------------------------------------------------------------------

ODataCatalog::ODataCatalog(duckdb::AttachedDatabase &db, 
                           const std::string &url, 
                           std::shared_ptr<HttpAuthParams> auth_params, 
                           const std::string &ignore_pattern)
    : duckdb::Catalog(db), 
      service_client(std::make_shared<HttpClient>(), HttpUrl(url), auth_params),
      ignore_pattern(ignore_pattern)
{ 
    // Initialize the main schema
    duckdb::CreateSchemaInfo schema_info;
    schema_info.schema = "main";
    main_schema = duckdb::make_uniq<ODataSchemaEntry>(*this, schema_info);
}

ODataCatalog::~ODataCatalog()
{
    // Cleanup if needed
}
    
std::string ODataCatalog::GetCatalogType()
{
    return "odata";
}

void ODataCatalog::Initialize(bool load_builtin)
{
    ERPL_TRACE_INFO("ODATA_CATALOG", "Initializing OData catalog (load_builtin: " + std::string(load_builtin ? "true" : "false") + ")");
    
    if (load_builtin) {
        ERPL_TRACE_DEBUG("ODATA_CATALOG", "Loading built-in OData catalog entries");
        // Load built-in entries if needed
    }
    
    ERPL_TRACE_INFO("ODATA_CATALOG", "OData catalog initialization completed");
}

void ODataCatalog::Initialize(duckdb::optional_ptr<duckdb::ClientContext> context, bool load_builtin)
{
    Initialize(load_builtin);
}

void ODataCatalog::FinalizeLoad(duckdb::optional_ptr<duckdb::ClientContext> context)
{
    // No additional finalization needed for OData
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> ODataCatalog::GetSchema(duckdb::CatalogTransaction transaction,
                                                                         const std::string &schema_name,
                                                                         duckdb::OnEntryNotFound if_not_found,
                                                                         duckdb::QueryErrorContext error_context)
{
    ERPL_TRACE_DEBUG("ODATA_CATALOG", "Getting schema: " + schema_name);
    
    if (schema_name == DEFAULT_SCHEMA || schema_name == INVALID_SCHEMA) {
        if (!main_schema) {
            throw duckdb::InternalException("OData catalog main schema not initialized");
        }
		return main_schema.get();
	}
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw duckdb::BinderException("We don't support seperation into mutliple schemas and map all entity sets to the same schema - \"%s\"", schema_name);
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> ODataCatalog::LookupSchema(duckdb::CatalogTransaction transaction, const duckdb::EntryLookupInfo &lookup_info, duckdb::OnEntryNotFound if_not_found)
{
    auto schema_name = lookup_info.GetEntryName();
    return GetSchema(transaction, schema_name, if_not_found);
}

void ODataCatalog::ScanSchemas(duckdb::ClientContext &context, std::function<void(duckdb::SchemaCatalogEntry &)> callback)
{ 
    if (!main_schema) {
        throw duckdb::InternalException("OData catalog main schema not initialized");
    }
    callback(*main_schema);
}

duckdb::optional_ptr<duckdb::CatalogEntry> ODataCatalog::CreateSchema(duckdb::CatalogTransaction transaction, duckdb::CreateSchemaInfo &info)
{
    throw duckdb::BinderException("OData does not support CREATING Schemas");
}

void ODataCatalog::DropSchema(duckdb::ClientContext &context, duckdb::DropInfo &info)
{
    throw duckdb::BinderException("OData does not support DROPPING Schemas");
}

duckdb::DatabaseSize ODataCatalog::GetDatabaseSize(duckdb::ClientContext &context)
{
    DatabaseSize result;
    
    result.total_blocks = 0;
	result.block_size = 0;
	result.free_blocks = 0;
	result.used_blocks = 0;
	result.bytes = 0;
	result.wal_size = idx_t(-1);

    throw duckdb::BinderException("OData does not support getting the DATABASE SIZE");

    return result;
}

bool ODataCatalog::InMemory()
{
    return false;
}

std::string ODataCatalog::GetDBPath()
{
    return "";
}

bool ODataCatalog::SupportsTimeTravel() const
{
    return false;
}

std::string ODataCatalog::GetDefaultSchema() const
{
    return "main";
}

duckdb::unique_ptr<duckdb::LogicalOperator> ODataCatalog::BindCreateIndex(duckdb::Binder &binder, duckdb::CreateStatement &stmt, duckdb::TableCatalogEntry &table, duckdb::unique_ptr<duckdb::LogicalOperator> plan)
{
    throw duckdb::BinderException("OData does not support CREATE INDEX");
}

HttpUrl ODataCatalog::ServiceUrl()
{
    return service_client.Url();
}

ODataSchemaEntry &ODataCatalog::GetMainSchema()
{
    if (!main_schema) {
        throw duckdb::InternalException("OData catalog main schema not initialized");
    }
    return *main_schema;
}

std::vector<std::string> ODataCatalog::GetTableNames()
{
    auto svc_response = service_client.Get();
    std::vector<std::string> result;
    for (auto &entity_set_ref : svc_response->EntitySets()) {
        if (ODataAttachBindData::MatchPattern(entity_set_ref.name, ignore_pattern)) {
            continue;
        }

        result.push_back(entity_set_ref.name);
    }
    return result;
}

std::optional<ODataEntitySetReference> ODataCatalog::GetEntitySetReference(const std::string &table_name)
{
    auto svc_response = service_client.Get();
    auto entity_set_refs = svc_response->EntitySets();
    auto entity_set_ref = std::find_if(entity_set_refs.begin(), entity_set_refs.end(), [&table_name](const ODataEntitySetReference &entity_set_ref) {
        return entity_set_ref.name == table_name;
    });

    if (entity_set_ref == entity_set_refs.end()) {
        return std::nullopt;
    }
    return *entity_set_ref;
}

void ODataCatalog::GetTableInfo(const std::string &table_name, duckdb::ColumnList &columns, std::vector<duckdb::unique_ptr<duckdb::Constraint>> &constraints)
{
    auto svc_response = service_client.Get();
    auto entity_set_ref = GetEntitySetReference(table_name);
    if (!entity_set_ref) {
        throw duckdb::BinderException("Table \"%s\" not found", table_name);
    }

    auto entity_set_url = HttpUrl::MergeWithBaseUrlIfRelative(service_client.Url(), entity_set_ref->url);
    auto entity_set_client = ODataEntitySetClient(
        std::dynamic_pointer_cast<HttpClient>(service_client.GetHttpClient()), 
        entity_set_url,
        service_client.AuthParams()
    );

    auto result_names = entity_set_client.GetResultNames();
    auto result_types = entity_set_client.GetResultTypes();

    for (idx_t i = 0; i < result_names.size(); i++) {
        ColumnDefinition column(result_names[i], result_types[i]);
        columns.AddColumn(std::move(column));
    }
}

#pragma region NotRelevantImplementations

duckdb::PhysicalOperator &ODataCatalog::PlanCreateTableAs(duckdb::ClientContext &context,
                                                         duckdb::PhysicalPlanGenerator &planner,
                                                         duckdb::LogicalCreateTable &op,
                                                         duckdb::PhysicalOperator &plan)
{
    throw duckdb::BinderException("OData does not support CREATING Tables");
}

duckdb::PhysicalOperator &ODataCatalog::PlanInsert(duckdb::ClientContext &context,
                                                   duckdb::PhysicalPlanGenerator &planner,
                                                   duckdb::LogicalInsert &op,
                                                   duckdb::optional_ptr<duckdb::PhysicalOperator> plan)
{
    throw duckdb::BinderException("OData does not support INSERTING into Tables");
}

duckdb::PhysicalOperator &ODataCatalog::PlanDelete(duckdb::ClientContext &context,
                                                   duckdb::PhysicalPlanGenerator &planner,
                                                   duckdb::LogicalDelete &op,
                                                   duckdb::PhysicalOperator &plan)
{
    throw duckdb::BinderException("OData does not support DELETING from Tables");
}

duckdb::PhysicalOperator &ODataCatalog::PlanUpdate(duckdb::ClientContext &context,
                                                   duckdb::PhysicalPlanGenerator &planner,
                                                   duckdb::LogicalUpdate &op,
                                                   duckdb::PhysicalOperator &plan)
{
    throw duckdb::BinderException("OData does not support UPDATING Tables");
}

duckdb::vector<duckdb::MetadataBlockInfo> ODataCatalog::GetMetadataInfo(duckdb::ClientContext &context)
{
    throw duckdb::BinderException("OData does not support getting the METADATA INFO");
}

#pragma endregion

} // namespace erpl_web