#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

namespace erpl_web {

// Storage Extension for Delta Sharing (Phase 2 - currently non-functional)
// ATTACH functionality is not yet implemented
// Use delta_share_show_shares/schemas/tables() functions instead for discovery
class DeltaShareStorageExtension : public duckdb::StorageExtension {
public:
	DeltaShareStorageExtension();
};

duckdb::unique_ptr<DeltaShareStorageExtension> CreateDeltaShareStorageExtension();

} // namespace erpl_web
