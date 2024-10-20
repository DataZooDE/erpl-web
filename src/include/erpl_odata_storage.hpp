#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/transaction/transaction_manager.hpp"


namespace erpl_web {


// -------------------------------------------------------------------------------------------------

class ODataStorageExtension : public duckdb::StorageExtension
{
public:
    ODataStorageExtension();
};

duckdb::unique_ptr<ODataStorageExtension> CreateODataStorageExtension();

} // namespace erpl_web