#pragma once

#include "duckdb/function/table_function.hpp"
#include <vector>
#include <optional>

namespace erpl_web {

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
    // The scan position is NOT here: it lives on ScanRowCursorState, per execution, so a
    // bound plan returns every row on each EXECUTE (GitHub #202). See scan_row_cursor.hpp.

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
    // The scan position lives on ScanRowCursorState, per execution - see
    // scan_row_cursor.hpp and GitHub #202.

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
    // The scan position lives on ScanRowCursorState, per execution - see
    // scan_row_cursor.hpp and GitHub #202.

    SacItemWithDetailsBindData() = default;
    virtual ~SacItemWithDetailsBindData() = default;
};

} // namespace erpl_web
