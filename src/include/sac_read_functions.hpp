#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "odata_read_functions.hpp"
#include <memory>
#include <vector>
#include <string>

namespace erpl_web {

/**
 * SAC Read Functions
 * Provide SQL table functions for reading data directly from SAC models and stories
 *
 * These functions use OData v4 APIs to query SAC planning models, analytics models,
 * and support predicate pushdown for efficient filtering and limiting.
 */

/**
 * sac_read_planning_data(model_id, [secret], [top], [skip])
 *
 * Read data from a SAC planning model
 *
 * Parameters:
 * - model_id: The technical ID of the planning model
 * - secret: (optional) Secret name containing SAC credentials (default: "sac")
 * - top: (optional) Maximum number of rows to return
 * - skip: (optional) Number of rows to skip (for pagination)
 *
 * Returns: All columns from the planning model with automatic schema detection
 *
 * Example:
 *   SELECT * FROM sac_read_planning_data(
 *     'REVENUE_MODEL',
 *     secret := 'my_sac',
 *     top := 1000
 *   );
 */
duckdb::TableFunctionSet CreateSacReadPlanningDataFunction();

/**
 * sac_read_analytical(model_id, [secret], [top], [skip], [dimensions], [measures])
 *
 * Read data from a SAC analytics model with optional dimension and measure filtering
 *
 * Parameters:
 * - model_id: The technical ID of the analytics model
 * - secret: (optional) Secret name containing SAC credentials (default: "sac")
 * - top: (optional) Maximum number of rows to return
 * - skip: (optional) Number of rows to skip
 * - dimensions: (optional) Comma-separated list of dimensions to include
 * - measures: (optional) Comma-separated list of measures to include
 *
 * Returns: Selected dimensions and measures from the analytics model
 *
 * Example:
 *   SELECT * FROM sac_read_analytical(
 *     'SALES_CUBE',
 *     dimensions := 'Territory,Product,Quarter',
 *     measures := 'SalesAmount,UnitsShipped',
 *     top := 5000
 *   );
 */
duckdb::TableFunctionSet CreateSacReadAnalyticalFunction();

/**
 * sac_read_story_data(story_id, [secret])
 *
 * Extract underlying data from a SAC story/dashboard
 *
 * Parameters:
 * - story_id: The technical ID of the story
 * - secret: (optional) Secret name containing SAC credentials (default: "sac")
 *
 * Returns: The data used in the story's visualizations
 *
 * Example:
 *   SELECT * FROM sac_read_story_data('EXECUTIVE_DASHBOARD_001');
 */
duckdb::TableFunctionSet CreateSacReadStoryDataFunction();

} // namespace erpl_web
