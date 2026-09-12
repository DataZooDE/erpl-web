#pragma once

#include "duckdb/function/table_function.hpp"
#include <vector>
#include <optional>

namespace erpl_web {

/**
 * Per-execution scan state for the SAC catalog functions (GitHub #195).
 *
 * The cursor used to live on the bind data, which DuckDB reuses across executions of a
 * bound plan. Nothing reset it, so a second EXECUTE resumed past the end of the vector and
 * returned no rows - silently, the #75 class.
 *
 * Only the CURSOR belongs here. The items themselves are fetched once at bind and never
 * mutated, so they stay on the bind data and are shared; copying them per execution would
 * be waste dressed up as a fix. Same reasoning as bc_describe / crm_describe in #191.
 */
class SacScanGlobalState : public duckdb::GlobalTableFunctionState {
public:
    size_t current_index = 0;
    bool finished = false;
};

inline duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SacScanInitGlobalState(
    duckdb::ClientContext &, duckdb::TableFunctionInitInput &) {
    return duckdb::make_uniq<SacScanGlobalState>();
}


/**
 * Generic bind data template for SAC catalog table functions
 *
 * Consolidates common pattern across all SAC catalog functions:
 * - Vector-based iteration (for listing functions)
 * - Current position tracking
 * - Finished flag for scan completion
 *
 * Usage:
 *   Using SacGenericBindData<SacModel> for sac_show_models
 *   Using SacGenericBindData<SacStory> for sac_show_stories
 */
template <typename ItemType>
class SacGenericBindData : public duckdb::TableFunctionData {
public:
    std::vector<ItemType> items;       // Collection of items (models, stories, etc.)

    // NOT the scan cursor. Per-execution position lives on SacScanGlobalState above; a
    // cursor here is shared across executions of a bound plan and made the second EXECUTE
    // return nothing (GitHub #195). Kept only so existing constructions still compile.
    size_t current_index = 0;
    bool finished = false;

    SacGenericBindData() = default;
    virtual ~SacGenericBindData() = default;
};

/**
 * Single-item bind data template for SAC catalog table functions
 *
 * Used when returning a single item with associated metadata
 * - Single item access
 * - Found flag (item was successfully retrieved)
 * - Current position tracking (for single-row output)
 * - Finished flag for scan completion
 *
 * Usage:
 *   Using SacSingleItemBindData<SacModel> for sac_describe_model
 *   Using SacSingleItemBindData<SacStory> for sac_describe_story
 */
template <typename ItemType>
class SacSingleItemBindData : public duckdb::TableFunctionData {
public:
    ItemType item;                     // Single item to return
    bool item_found = false;           // Whether item was found/retrieved
    size_t current_index = 0;          // Current iteration position (0 or 1 for single row)
    bool finished = false;             // Scan completion flag

    SacSingleItemBindData() = default;
    virtual ~SacSingleItemBindData() = default;
};

/**
 * Model-with-details bind data template
 *
 * Used when returning a model or story with additional metadata (dimensions, measures)
 * Specialization for cases requiring extra data alongside main item
 *
 * Usage:
 *   Using SacItemWithDetailsBindData<SacModel> for sac_describe_model
 *   with additional dimensions/measures vectors
 */
template <typename ItemType>
class SacItemWithDetailsBindData : public duckdb::TableFunctionData {
public:
    ItemType item;                     // Main item (model, story)
    std::vector<std::string> details;  // Additional details (dimensions, measures, etc.)
    bool item_found = false;           // Whether item was found/retrieved
    size_t current_index = 0;          // Current iteration position
    bool finished = false;             // Scan completion flag

    SacItemWithDetailsBindData() = default;
    virtual ~SacItemWithDetailsBindData() = default;
};

} // namespace erpl_web
