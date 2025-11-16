#include "delta_share_storage.hpp"

namespace erpl_web {

// ATTACH is not yet implemented for Delta Sharing
// Use delta_share_show_shares/schemas/tables() table functions for discovery instead
static duckdb::unique_ptr<duckdb::Catalog> DeltaShareAttach(
	duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
	duckdb::ClientContext& context,
	duckdb::AttachedDatabase& db,
	const std::string& name,
	duckdb::AttachInfo& info,
	duckdb::AttachOptions& options) {
	// Phase 2: Implement full catalog support
	return nullptr;
}

DeltaShareStorageExtension::DeltaShareStorageExtension() {
	attach = DeltaShareAttach;
	create_transaction_manager = nullptr;
}

duckdb::unique_ptr<DeltaShareStorageExtension> CreateDeltaShareStorageExtension() {
	return duckdb::make_uniq<DeltaShareStorageExtension>();
}

} // namespace erpl_web
