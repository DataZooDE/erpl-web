#include "erpl_odata_storage.hpp"
#include "erpl_odata_catalog.hpp"

#include "erpl_odata_transaction_manager.hpp"

namespace erpl_web {

// -------------------------------------------------------------------------------------------------

static duckdb::unique_ptr<duckdb::Catalog> ODataAttach(duckdb::StorageExtensionInfo *storage_info, 
                                                       duckdb::ClientContext &context,
                                                       duckdb::AttachedDatabase &db, 
                                                       const std::string &name, 
                                                       duckdb::AttachInfo &info,
                                                       duckdb::AccessMode access_mode) 
{
    // name = trippin
    // info.name = trippin
    // info.path = https://services.odata.org/TripPinRESTierService
    // access_mode = duckdb::AccessMode::READ_ONLY

    //options.access_mode = access_mode;

    if (access_mode != duckdb::AccessMode::READ_ONLY) {
        throw duckdb::BinderException("ODATA storage extension does not support write access");
    }

    return duckdb::make_uniq<ODataCatalog>(db, info.path);
}

static duckdb::unique_ptr<duckdb::TransactionManager> ODataCreateTransactionManager(duckdb::StorageExtensionInfo *storage_info,
                                                                                  duckdb::AttachedDatabase &db, 
                                                                                  duckdb::Catalog &catalog) 
{
    auto &odata_catalog = catalog.Cast<ODataCatalog>();
	return duckdb::make_uniq<ODataTransactionManager>(db, odata_catalog);
}

ODataStorageExtension::ODataStorageExtension()
{ 
    attach = ODataAttach;
	create_transaction_manager = ODataCreateTransactionManager;
}

duckdb::unique_ptr<ODataStorageExtension> CreateODataStorageExtension()
{
    return duckdb::make_uniq<ODataStorageExtension>();
}

} // namespace erpl_web