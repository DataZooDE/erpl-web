# Implementation Plan: OData-ODP Replication for ERPL

## Overview

Target a reliable, idempotent, delta-capable ingestion from SAP ODP via OData v2 into DuckDB. Follow the agent-agnostic TDD loop in `.ai/development-workflow.md` using `make test_debug` (SQL) and the C++ test binary where applicable.

## Implementation Checklist

### **Phase 1 — Service Registration & Metadata Discovery**

#### Core Infrastructure
- [x] **ODP Functions Registration**: Basic function registration in extension
- [x] **ODP Replication Manager**: Class structure defined in header
- [x] **Basic Data Structures**: `ODPSourceInfo`, `ODPEntityInfo`, `ODPDeltaState` defined
- [x] **Function Stubs**: `erpl_odp_odata_register`, `erpl_odp_odata_entities`, `erpl_odp_odata_status`
- [x] **Compilation Success**: All ODP functions compile and register successfully

#### Implementation Status
- [x] **Function Registration**: Functions registered in extension and callable from SQL
- [x] **Parameter Validation**: Basic URL and secret validation implemented
- [x] **Mock Responses**: Functions return mock data for testing
- [x] **Basic Function Stubs**: `erpl_odp_odata_initial_load`, `erpl_odp_odata_delta_pull`, `erpl_odp_odata_resubscribe` compile as templates
- [ ] **Metadata Discovery**: `$metadata` parsing and schema extraction
- [ ] **Delta Capability Detection**: Identify `DeltaLinksOf<EntitySet>` entity sets
- [ ] **Termination Function Detection**: Identify `TerminateDeltasFor<EntitySet>` functions
- [ ] **ODP Service Detection**: Entity set naming pattern recognition
- [ ] **State Persistence**: Store source info in `erpl_odp_odata_sources` table
- [ ] **Schema JSON Storage**: Parse and store EDM metadata as JSON

#### Tests Needed
- [ ] **Unit Tests**: Metadata parsing, ODP service detection, delta entity detection
- [ ] **Integration Tests**: Secret validation, endpoint accessibility, error handling
- [ ] **Fixtures**: OData v2 metadata XML, service catalog responses

### 🔄 **Phase 2 — Initial Load with Change Tracking** [NOT IMPLEMENTED]

#### Core Functionality
- [ ] **Initial Load Function**: `erpl_odp_odata_initial_load(source_id)`
- [ ] **Change Tracking Setup**: `Prefer: odata.track-changes` header implementation
- [ ] **Pagination Handling**: Process OData pages with `__next` links
- [ ] **Delta Token Extraction**: Parse `__delta` links from responses
- [ ] **Target Table Creation**: Generate DuckDB tables from schema
- [ ] **Data Ingestion**: Insert all rows from initial load
- [ ] **State Persistence**: Save delta token and sync time

#### Implementation Requirements
- [ ] **HTTP Headers**: `Prefer: odata.track-changes`, `Prefer: odata.maxpagesize=15000`
- [ ] **JSON Response Parsing**: Handle `{"d": {"results": [...]}}` structure
- [ ] **Preference Validation**: Verify `preference-applied: odata.track-changes` header
- [ ] **Error Handling**: 401/403 responses, preference not applied scenarios
- [ ] **Idempotency**: Re-running with same token should not duplicate data

#### Tests Needed
- [ ] **SQL Tests**: Table creation, data ingestion, idempotent behavior
- [ ] **Fixture Tests**: Paged responses, delta link extraction, preference validation
- [ ] **Error Tests**: Authentication failures, preference not applied, network errors

### ❌ **Phase 3 — Delta Pull (Incremental Apply)** [NOT IMPLEMENTED]

#### Core Functionality
- [ ] **Delta Pull Function**: `erpl_odp_odata_delta_pull(source_id)`
- [ ] **Delta Token Usage**: Call delta links with `!deltatoken={token}`
- [ ] **Change Processing**: Handle Insert/Update/Delete operations
- [ ] **Transactional Updates**: Atomic token advancement and data changes
- [ ] **Conflict Resolution**: Handle duplicate keys and change indicators

#### Implementation Requirements
- [ ] **URL Construction**: Proper delta token format with exclamation mark
- [ ] **Change Semantics**: Parse OData change indicators (I/U/D)
- [ ] **Upsert Logic**: Update existing records or insert new ones
- [ ] **Delete Handling**: Process tombstone records and soft deletes
- [ ] **State Management**: Update delta token only on successful completion

#### Tests Needed
- [ ] **SQL Tests**: Upsert/delete logic, transactional token advancement
- [ ] **Fixture Tests**: Mixed I/U/D delta pages, conflict scenarios
- [ ] **Error Tests**: Partial failures, token expiry, network interruptions

### ❌ **Phase 4 — Delta Token Discovery & Management** [NOT IMPLEMENTED]

#### Core Functionality
- [ ] **Token Discovery Function**: `erpl_odp_odata_discover_tokens(source_id)`
- [ ] **Delta Links Query**: Query `DeltaLinksOf<EntitySet>` entity set
- [ ] **Token Selection**: Identify latest token based on `CreatedAt` timestamp
- [ ] **Fallback Strategy**: Graceful degradation when delta capabilities unavailable

#### Implementation Requirements
- [ ] **Entity Set Discovery**: Query delta links entity set for available tokens
- [ ] **Timestamp Parsing**: Parse `CreatedAt` fields for token selection
- [ ] **Latest Token Logic**: Select most recent token for processing
- [ ] **Capability Detection**: Determine if delta links entity exists

#### Tests Needed
- [ ] **Fixture Tests**: Delta links entity responses, token selection logic
- [ ] **Error Tests**: 404 responses from delta links entity
- [ ] **Fallback Tests**: Scenarios where delta capabilities unavailable

### ❌ **Phase 5 — Subscription Lifecycle Management** [NOT IMPLEMENTED]

#### Core Functionality
- [ ] **Subscription Termination**: `erpl_odp_odata_terminate_subscription(source_id)`
- [ ] **Subscription Status**: `erpl_odp_odata_subscription_status(source_id)`
- [ **Enhanced Resubscribe**: `erpl_odp_odata_resubscribe(source_id)` with cleanup

#### Implementation Requirements
- [ ] **Termination Function Calls**: Execute `TerminateDeltasFor<EntitySet>` function
- [ ] **Subscription Health Monitoring**: Track subscription establishment and activity
- [ ] **Cleanup Logic**: Proper cleanup before establishing new subscriptions
- [ ] **Status Reporting**: Health indicators and last activity timestamps

#### Tests Needed
- [ ] **Termination Tests**: Function calls, error handling for missing functions
- [ ] **Status Tests**: Subscription health reporting, activity tracking
- [ ] **Resubscribe Tests**: Cleanup and re-establishment scenarios

### ❌ **Phase 6 — Recovery & Resubscribe** [NOT IMPLEMENTED]

#### Core Functionality
- [ ] **Token Expiry Detection**: Handle HTTP 410/404 responses
- [ ] **Resubscribe Logic**: Reset tokens and replace data safely
- [ ] **Conflict Resolution**: Detect and resolve subscription conflicts
- [ ] **Error Classification**: Surface actionable error messages

#### Implementation Requirements
- [ ] **Expiry Detection**: Identify token expiry and service errors
- [ ] **Safe Data Replacement**: Truncate+reload or shadow-swap strategies
- [ ] **Conflict Detection**: Identify conflicting subscriptions
- [ ] **Remediation Instructions**: Clear guidance for error resolution

#### Tests Needed
- [ ] **Expiry Tests**: Simulated expiry responses, error classification
- [ ] **Conflict Tests**: Subscription conflict scenarios and resolution
- [ ] **Recovery Tests**: Resubscribe behavior and data consistency

### ❌ **Phase 7 — Service Discovery & Catalog** [NOT IMPLEMENTED]

#### Core Functionality
- [ ] **Service Discovery**: `erpl_odp_odata_discover_services(url, secret_name)`
- [ ] **Service Catalog**: `erpl_odp_odata_service_catalog()`
- [ **Entity Set Discovery**: Report delta support and termination functions

#### Implementation Requirements
- [ ] **Service Pattern Detection**: Identify ODP services by entity set patterns
- [ ] **Catalog Management**: Maintain service metadata and capabilities
- [ ] **Discovery API**: Query SAP Gateway service catalog
- [ ] **Capability Reporting**: Delta support and termination function availability

#### Tests Needed
- [ ] **Service Tests**: Pattern detection, catalog management
- [ ] **Discovery Tests**: Service catalog API integration
- [ ] **Metadata Tests**: Various ODP service configurations

### ❌ **Phase 8 — Observability & Limits** [NOT IMPLEMENTED]

#### Core Functionality
- [ ] **Performance Counters**: Last run start/end, rows read/applied, pages processed
- [ ] **Error Tracking**: Last error details and timestamps
- [ **Operational Limits**: Single subscription per user, no parallel pulls
- [ **Rate Limiting**: Respect SAP Gateway constraints

#### Implementation Requirements
- [ ] **Metrics Collection**: Runtime performance and error statistics
- [ ] **Limit Enforcement**: Prevent parallel operations and conflicts
- [ ] **Rate Limiting**: Respect server-side pagination and rate limits
- [ **Documentation**: Operational guidance and troubleshooting

#### Tests Needed
- [ ] **Metrics Tests**: Counter accuracy, error tracking
- [ ] **Limit Tests**: Parallel operation prevention, rate limit handling
- [ ] **Performance Tests**: End-to-end performance benchmarks

## Enhanced API Surface Status

### **Implemented Functions**
- [x] `erpl_odp_odata_register(url, secret_name) -> source_id` - Basic registration
- [x] `erpl_odp_odata_entities(source_id) -> setof(entity_name, has_delta, has_termination_function)` - Mock entities
- [x] `erpl_odp_odata_status(source_id)` - Mock status reporting

### ❌ **Functions to Implement**
- [ ] `erpl_odp_odata_discover_services(url, secret_name) -> setof(service_id, description, entity_sets, delta_capabilities)`
- [ ] `erpl_odp_odata_discover_tokens(source_id) -> setof(delta_token, created_at, is_latest)`
- [ ] `erpl_odp_odata_initial_load(source_id)`
- [ ] `erpl_odp_odata_delta_pull(source_id)`
- [ ] `erpl_odp_odata_resubscribe(source_id)`
- [ ] `erpl_odp_odata_terminate_subscription(source_id)`
- [ ] `erpl_odp_odata_subscription_status(source_id) -> setof(status, last_activity, delta_token, subscription_health)`
- [ ] `erpl_odp_odata_service_catalog() -> setof(service_id, url, capabilities, last_check)`

## Enhanced Data & State Tables Status

### ❌ **Tables to Implement**
- [ ] `erpl_odp_odata_sources(source_id, service_url, entity_name, schema_json, is_delta_enabled, has_termination_function, secret_name, created_at)`
- [ ] `erpl_odp_odata_delta_state(source_id, delta_token, last_sync_time, last_stats_json, subscription_established_at)`
- [ ] `erpl_odp_odata_service_catalog(service_id, service_url, description, entity_sets, delta_capabilities, last_discovery, secret_name)`
- [ ] `erpl_odp_odata_subscription_log(source_id, action, timestamp, status, details)`
- [ ] Target data tables per source (derived from entity_name)

## Current Implementation Summary

### ✅ **What's Already Built**
1. **Basic Function Framework**: ODP functions are registered and callable from SQL
2. **Parameter Validation**: URL and secret validation with proper error messages
3. **Mock Responses**: Functions return test data for development and testing
4. **Class Structure**: ODP classes defined with proper inheritance from existing OData infrastructure
5. **Extension Integration**: Functions properly integrated into the DuckDB extension

### 🔄 **What's Partially Implemented**
1. **Function Stubs**: Basic function signatures and return types defined
2. **Data Structures**: ODP-specific structs defined but not yet used
3. **Error Handling**: Basic validation errors but no OData-specific error handling

### ❌ **What's Missing**
1. **OData Integration**: No actual OData client calls or HTTP requests
2. **Metadata Parsing**: No `$metadata` XML parsing or schema extraction
3. **Delta Token Management**: No delta token discovery or usage
4. **Data Persistence**: No DuckDB table creation or data storage
5. **Subscription Management**: No change tracking or subscription lifecycle
6. **Service Discovery**: No SAP Gateway service catalog integration
7. **Error Recovery**: No token expiry handling or resubscribe logic

## Next Steps Priority

1. **High Priority**: Implement metadata discovery and schema parsing
2. **Medium Priority**: Build initial load with change tracking
3. **Low Priority**: Add service discovery and catalog management

## Testing Status

### ✅ **Test Infrastructure**
- [x] Functions can be called from SQL
- [x] Basic parameter validation tested
- [x] Mock responses working

### ❌ **Tests Needed**
- [ ] Unit tests for OData parsing and ODP detection
- [ ] Integration tests with OData fixtures
- [ ] Error scenario testing
- [ ] Performance and limit testing

## Key Insights from Theobald Implementation

- **Delta Token Discovery**: Query `DeltaLinksOf<EntitySet>` entity set to find available tokens with timestamps
- **Subscription Management**: Use `TerminateDeltasFor<EntitySet>` function for cleanup
- **Service Detection**: Identify ODP services by entity set naming patterns (`EntityOf*`, `FactsOf*`)
- **Preference Validation**: Verify `preference-applied` header contains `odata.track-changes`
- **Fallback Strategy**: Gracefully degrade to full extraction when delta capabilities unavailable
- **URL Construction**: Use exclamation mark for delta tokens (`!deltatoken=`) not question mark
- **JSON Parsing**: Handle OData v2 response structure with `{"d": {"results": [...]}}` wrapper

## Concrete Implementation Patterns

### OData v2 URL Construction
- **Base Pattern**: `{gateway_host}/sap/opu/odata/sap/{SERVICE_NAME}/`
- **Entity Set**: `{base_url}/{EntitySet}?$format=json`
- **Delta Token**: `{base_url}/{EntitySet}!deltatoken={token}&$format=json`
- **Metadata**: `{base_url}/$metadata`
- **Service Catalog**: `/sap/opu/odata/IWFND/CATALOGSERVICE;v=2/ServiceCollection?search={pattern}&$format=json&$expand=EntitySets`

### HTTP Headers and Preferences
- **OData Version**: `DataServiceVersion: 2.0`, `MaxDataServiceVersion: 2.0`
- **Change Tracking**: `Prefer: odata.track-changes`
- **Page Size**: `Prefer: odata.maxpagesize=15000` (Theobald default)
- **Accept**: `application/json` for data, `application/xml` for metadata
- **Verification**: Check `preference-applied: odata.track-changes` response header

### JSON Response Structure
- **Root Wrapper**: `{"d": {...}}` (OData v2 standard)
- **Results**: `{"d": {"results": [...]}}`
- **Pagination**: `{"d": {"results": [...], "__next": "next_url"}}`
- **Delta Links**: `{"d": {"results": [...], "__delta": "delta_url"}}`

### Performance and Package Sizing
- **Default Package Size**: 15,000 rows (Theobald's proven default)
- **Configurable**: via `Prefer: odata.maxpagesize={size}` header
- **Sequential Processing**: handle pages one at a time to manage memory
- **Server Limits**: respect SAP Gateway pagination constraints

## Authentication & Secrets

- Use DuckDB secrets for HTTP auth against the Gateway host:
  - `http_basic` with `username`/`password` scoped to the base URL (e.g., `https://localhost:50000`).
  - `http_bearer` with an OAuth2 access token scoped to the base URL.
- S/4HANA/BW/4HANA guidance:
  - Trial/on-prem: HTTPS + Basic auth is simplest for local testing.
  - Production: configure OAuth2 on Gateway (`OA2C_CONFIG`) or use S/4HANA Cloud communication arrangements; obtain tokens from the platform token endpoint and store as `http_bearer` secret.
- Registration stores `secret_name`; all requests resolve and apply the secret at runtime, with credentials redacted in logs.

## Work Breakdown (commit discipline)

- **Commit 1** (tests only): metadata parsing tests, ODP service detection, and enhanced state schema.
- **Commit 2** (feat): registration + metadata discovery + service detection to green.
- **Commit 3** (tests only): initial load + delta + token discovery tests with fixtures.
- **Commit 4** (feat): initial load + delta + token discovery to green.
- **Commit 5** (tests only): subscription management + termination tests.
- **Commit 6** (feat): subscription lifecycle management to green.
- **Commit 7** (feat): service discovery + catalog + recovery/resubscribe + observability.

## Integration with Existing ERPL Web Architecture

- **Leverage existing components**:
  - HTTP client with authentication via DuckDB secrets
  - OData EDM parsing and content handling
  - Error handling and tracing infrastructure
  - Table function framework for SQL interface
- **Extend where needed**:
  - OData client with ODP-specific delta token handling
  - HTTP request headers for change tracking preferences
  - State persistence in DuckDB tables
  - Subscription lifecycle management
  - **URL construction**: handle exclamation mark delta tokens correctly
  - **JSON parsing**: adapt existing parsers for OData v2 response structure

## Implementation Notes

### URL Construction
- **Delta tokens**: Use exclamation mark format `!deltatoken={token}` (OData v2 standard)
- **Query parameters**: Always include `$format=json` for JSON responses
- **Base URL handling**: Ensure proper trailing slash handling in URL construction

### HTTP Headers
- **OData version**: Set `DataServiceVersion: 2.0` and `MaxDataServiceVersion: 2.0`
- **Change tracking**: Use `Prefer: odata.track-changes` header
- **Page size**: Default to 15,000 rows with `Prefer: odata.maxpagesize=15000`

### JSON Response Handling
- **Response wrapper**: Parse `{"d": {...}}` structure correctly
- **Results array**: Extract from `{"d": {"results": [...]}}`
- **Pagination**: Handle `__next` and `__delta` links
- **Error responses**: Parse OData error format for meaningful error messages

### Performance Considerations
- **Package size**: Default to 15,000 rows (Theobald's proven default)
- **Sequential processing**: Handle pages one at a time to manage memory
- **Caching**: Leverage existing HTTP caching for metadata and repeated requests

## Risks & Mitigations

- **Token staleness**: surface clear remediation and provide `resubscribe`.
- **Parallelism hazards**: enforce single-run per source via advisory locks or state flags.
- **Schema drift**: detect and require an explicit migration path before applying deltas.
- **Delta capability missing**: graceful fallback to full extraction with clear user notification.
- **Subscription conflicts**: detect and resolve through proper cleanup and re-establishment.
- **URL construction errors**: validate delta token format (`!deltatoken=` not `?deltatoken=`)
- **JSON parsing failures**: handle OData v2 response wrapper `{"d": {...}}` correctly

## Quantitative Acceptance (per feature increment)

- Tests: 100% of new tests pass locally and in CI.
- Coverage: non-decreasing for touched modules; target ≥80% for new logic.
- Performance: reasonable end-to-end on representative pages; avoid O(n²) operations.
- **Performance benchmarks**:
  - Handle package sizes up to 15,000 rows efficiently
  - Process delta tokens with minimal latency
  - Support concurrent operations on different sources
