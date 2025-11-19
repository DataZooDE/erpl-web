<a name="top"></a>

[![License: BSL 1.1](https://img.shields.io/badge/License-BSL%201.1-blue.svg)](LICENSE)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.4.2+-green.svg)](https://duckdb.org)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

# ERPL Web — DuckDB HTTP + OData + Delta Sharing + SAP Datasphere + SAP Analytics Cloud Extension

ERPL Web is a production-grade DuckDB extension that lets you call HTTP/REST APIs, query OData v2/v4 services, read Delta Sharing tables, and work with SAP Datasphere and SAP Analytics Cloud assets directly from SQL. It brings secure OAuth2, DuckDB Secrets integration, predicate pushdown, robust tracing, and smart caching into a single, easy-to-use package.

- SEO topics: DuckDB HTTP client, DuckDB REST, DuckDB OData v2/v4, DuckDB Delta Sharing, DuckDB Delta Lake, Databricks Delta, DuckDB SAP Datasphere, DuckDB SAP Analytics Cloud, OAuth2 for DuckDB, OData ATTACH, Delta Sharing ATTACH, query APIs from SQL.

## ✨ Highlights

- HTTP from SQL: GET, HEAD, POST, PUT, PATCH, DELETE with headers, auth, and body
- OData v2/v4: Universal reader with automatic version handling and pushdown
- Delta Sharing: Query Databricks, SAP, and other Delta Sharing protocol-compliant services via Parquet files
- SAP Datasphere: List spaces/assets, describe assets, and read relational/analytical data
- SAP Analytics Cloud: Query models, stories, discover dimensions/measures with automatic schema
- OAuth2 + Secrets: Secure flows with refresh, client credentials, and secret providers
- Tracing: Deep, configurable tracing for debugging and performance tuning
- Caching + Resilience: Response caching, metadata caching, and retry logic
- Charset handling: Automatic UTF‑8/ISO‑8859‑1/‑15/Windows‑1252 detection and conversion

---

## 🚀 Install

You can either install it from the DuckDB community repository

```sql
INSTALL erpl_web FROM community;
LOAD erpl_web;
```

or using our source

```sql
-- Install (DuckDB requires -unsigned when installing from a custom URL)
INSTALL 'erpl_web' FROM 'http://get.erpl.io';
LOAD 'erpl_web';
```

or build from source (developers):

```bash
make debug
```

---

## ⚡ Quick Start

### HTTP in one line

```sql
SELECT content
FROM http_get('https://httpbun.com/ip');

SELECT content::JSON->>'ip' AS ip_address
FROM http_get('https://httpbun.com/ip');
```

Add headers and auth:

```sql
SELECT status, content
FROM http_get(
  'https://api.example.com/data',
  headers:={'Authorization':'Bearer token123','X-Trace':'on'}
);
```

### OData v2/v4 in SQL

Attach and query any OData service:

```sql
-- OData v4
ATTACH 'https://services.odata.org/TripPinRESTierService' AS trippin (TYPE odata);
SELECT UserName, FirstName, LastName FROM trippin.People WHERE Gender='Female';

-- OData v2
ATTACH 'https://services.odata.org/V2/Northwind/Northwind.svc' AS northwind (TYPE odata);
SELECT CustomerID, CompanyName FROM northwind.Customers WHERE Country='Germany';
```

Read directly via `odata_read`:

```sql
SELECT UserName, AddressInfo[1].City."Name" AS city
FROM odata_read('https://services.odata.org/TripPinRESTierService/People')
WHERE UserName='angelhuffman';
```

### Delta Sharing (Databricks, SAP, others)

Query Delta Sharing repositories with profile files from **local paths, HTTP URLs, or S3 URIs**:

```sql
-- Local profile file
SELECT url, size FROM delta_share_scan(
  './profile.json',
  'share_name',
  'schema_name',
  'table_name'
) LIMIT 10;

-- HTTP URL (remote profile) - Example with public Delta Sharing server
SELECT url, size FROM delta_share_scan(
  'https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/delta-sharing/share/open-datasets.share',
  'delta_sharing',
  'default',
  'owid-covid-data'
) LIMIT 10;

-- S3 URI (requires S3 credentials via CREATE SECRET)
SELECT url, size FROM delta_share_scan(
  's3://my-bucket/profiles/delta.share',
  'delta_sharing',
  'default',
  'your_table_name'
) LIMIT 10;

-- Read data from shared Parquet files (using parquet_scan):
SELECT * FROM read_parquet(
  'https://presigned-url-from-delta-share.s3.amazonaws.com/...'
);
```

**Profile Format (JSON):**
```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "https://sharing.databricks.com",
  "bearerToken": "YOUR_TOKEN"
}
```

**Features:**
- ✅ Local file paths: `./profile.json`, `/path/to/profile.share`
- ✅ HTTP URLs: `https://example.com/profile.share` (auto-downloaded)
- ✅ S3 URIs: `s3://bucket/profile.share` (requires S3 secrets)
- ✅ Azure URLs: `azure://container/profile.share`
- ✅ Bearer token authentication
- ✅ NDJSON response parsing
- ✅ Pre-signed URLs to cloud storage
- ✅ Works with Databricks, SAP, and any Delta Sharing protocol-compliant service
- ✅ Remote profiles transparently loaded via DuckDB's FileSystem API
- Full documentation in `DELTA_SHARE_TESTING.md`

### Delta Sharing Discovery (Catalog Functions)

Discover available shares, schemas, and tables before reading data:

```sql
-- List all shares in a Delta Sharing profile
SELECT share_name, share_id
FROM delta_share_show_shares('./profile.json');

-- List schemas in a specific share
SELECT schema_name, schema_id
FROM delta_share_show_schemas('./profile.json', 'delta_sharing');

-- List tables in a schema
SELECT name, share_name, schema_name
FROM delta_share_show_tables('./profile.json', 'delta_sharing', 'default');
```

**Features:**
- ✅ Discover share structures before querying
- ✅ Works with all profile types (local, HTTP, S3)
- ✅ Helps explore available data
- ⚠️ ATTACH support for Delta Sharing (`ATTACH ... TYPE delta_share`) is planned but not yet implemented

### SAP Datasphere in minutes

1) Create a Datasphere secret (OAuth2).

```sql
-- Minimal OAuth2 secret (authorization_code or client_credentials)
CREATE SECRET datasphere (
  TYPE datasphere,
  PROVIDER oauth2,
  TENANT_NAME 'your-tenant',
  DATA_CENTER 'eu10',
  CLIENT_ID '...optional-if-pre-delivered...',
  CLIENT_SECRET '...optional-if-pre-delivered...',
  SCOPE 'default',                     -- or service-specific scopes
  REDIRECT_URI 'http://localhost:65000' -- default
);
```

2) Explore spaces and assets:

```sql
-- List spaces (DWAAS core)
SELECT * FROM datasphere_show_spaces();

-- List assets in a space (business name, object type, technical name)
SELECT * FROM datasphere_show_assets('MY_SPACE');

-- List assets across all accessible spaces
SELECT * FROM datasphere_show_assets();
```

3) Describe and read data:

```sql
-- Describe a space
SELECT * FROM datasphere_describe_space('MY_SPACE');

-- Describe an asset (15 columns incl. relational/analytical schema)
SELECT * FROM datasphere_describe_asset('MY_SPACE','MART_DIM_LEAD');

-- Read relational data
SELECT * FROM datasphere_read_relational('MY_SPACE','MY_TABLE') LIMIT 10;

-- Read analytical data (metrics + dimensions -> $select)
SELECT *
FROM datasphere_read_analytical(
  'MY_SPACE','MY_CUBE',
  metrics:=['Revenue','Count'],
  dimensions:=['Company','Country']
) LIMIT 10;
```

---

## 🌐 HTTP Functions (REST from SQL)

Functions return rows with consistent columns: `method`, `status`, `url`, `headers`, `content_type`, `content`.

- `http_get(url, [headers], [accept], [auth], [auth_type], [timeout])`
- `http_head(url, [headers], [accept], [auth], [auth_type], [timeout])`
- `http_post(url, body, [content_type], [headers], [accept], [auth], [auth_type], [timeout])`
- `http_put(url, body, [content_type], [headers], [accept], [auth], [auth_type], [timeout])`
- `http_patch(url, body, [content_type], [headers], [accept], [auth], [auth_type], [timeout])`
- `http_delete(url, body, [content_type], [headers], [accept], [auth], [auth_type], [timeout])`

### Authentication Precedence

HTTP functions support two authentication methods with clear precedence:

1. **Function Parameters (Highest Priority)**: Use the `auth` and `auth_type` named parameters
2. **DuckDB Secrets (Fallback)**: Use registered secrets scoped to the URL

**Important**: When the `auth` parameter is provided, it **always takes precedence** over registered secrets, regardless of whether a secret exists for the URL.

#### Using Function Parameters

```sql
-- Basic authentication (default auth_type)
SELECT * FROM http_get('https://api.example.com/data', 
                      auth := 'username:password');

-- Bearer token authentication
SELECT * FROM http_get('https://api.example.com/data', 
                      auth := 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...',
                      auth_type := 'BEARER');

-- Username only (password will be empty, defaults to BASIC auth)
SELECT * FROM http_get('https://api.example.com/data', 
                      auth := 'username_only');

-- Explicit BASIC authentication
SELECT * FROM http_get('https://api.example.com/data', 
                      auth := 'user:pass',
                      auth_type := 'BASIC');
```

#### Using DuckDB Secrets (Fallback)

When no `auth` parameter is provided, the functions automatically use registered secrets:

```sql
-- Create a secret for basic authentication
CREATE SECRET api_auth (
  TYPE http_basic,
  USERNAME 'secret_user',
  PASSWORD 'secret_pass',
  SCOPE 'https://api.example.com/'
);

-- This will use the secret (no auth parameter)
SELECT * FROM http_get('https://api.example.com/data');

-- This will use the auth parameter and ignore the secret
SELECT * FROM http_get('https://api.example.com/data', 
                      auth := 'override_user:override_pass');
```

#### Authentication Type Support

- **BASIC**: Username and password (format: `username:password`)
- **BEARER**: Token-based authentication (the entire `auth` value is used as the token)

**Note**: The `auth_type` parameter defaults to `BASIC` when not specified.

### Examples

```sql
-- JSON body
SELECT *
FROM http_post(
  'https://httpbin.org/anything',
  {'name':'Alice','age':30}::JSON
);

-- Text body
SELECT * FROM http_post('https://httpbin.org/anything','Hello','text/plain');

-- Form body
SELECT * FROM http_post('https://httpbin.org/anything','a=1&b=2','application/x-www-form-urlencoded');
```

### Debugging Authentication

Enable tracing to see which authentication source is being used:

```sql
SET erpl_trace_enabled = TRUE;
SET erpl_trace_level = 'DEBUG';

-- This will show "Using auth parameter" in the trace
SELECT * FROM http_get('https://api.example.com/data', auth := 'user:pass');

-- This will show "No auth parameter provided, using registered secrets" in the trace
SELECT * FROM http_get('https://api.example.com/data');
```

Character sets and binary:

- Content types are inspected and converted to UTF‑8 for textual responses (UTF‑8/ISO‑8859‑1/‑15/Windows‑1252). Binary types are returned as-is.

---

## 📊 OData v2/v4

Two styles are supported:

1) Attach as a DuckDB database: `ATTACH '...url...' AS name (TYPE odata)`
2) Read directly: `SELECT ... FROM odata_read('...entity_set_url...')`

Capabilities:

- Automatic OData version detection (v2/v4)
- Predicate pushdown: `$filter`, `$select`, `$top`, `$skip`
- Pagination handling and total-count awareness
- EDM-aware type mapping into DuckDB types

Example:

```sql
SELECT OrderID, CustomerID
FROM odata_read('https://services.odata.org/V2/Northwind/Northwind.svc/Orders')
WHERE OrderDate >= '1996-01-01'
LIMIT 5;
```

Attach usage:

```sql
ATTACH 'https://services.odata.org/TripPinRESTierService' AS trippin (TYPE odata);
SELECT TOP 5 UserName, FirstName FROM trippin.People;
``;

---

## 🟦 SAP Datasphere (DWAAS Core + Catalog)

ERPL Web includes first-class support for SAP Datasphere using a secured OAuth2 secret. The extension integrates both the DWAAS core APIs and the Catalog OData service to provide discovery and rich metadata.

### Functions overview

- `datasphere_show_spaces()` → `name`
- `datasphere_show_assets(space_id)` → `name`, `object_type`, `technical_name`
- `datasphere_show_assets()` → `name`, `object_type`, `technical_name`, `space_name`
- `datasphere_describe_space(space_id)` → `name`, `label`
- `datasphere_describe_asset(space_id, asset_id)` → 15 columns including:
  - Basic: `name`, `space_name`, `label`, `asset_relational_metadata_url`, `asset_relational_data_url`, `asset_analytical_metadata_url`, `asset_analytical_data_url`, `supports_analytical_queries`
  - Extended: `odata_context`, `relational_schema` (STRUCT of `columns(name, technical_name, type, length)`), `analytical_schema` (STRUCT of `measures|dimensions|variables` each as LIST of STRUCTs `name`, `type`, `edm_type`), `has_relational_access`, `has_analytical_access`, `asset_type`, `odata_metadata_etag`

- Readers with pushdown and parameters:
  - `datasphere_read_relational(space_id, asset_id [, secret])`
    - Named params: `top`, `skip`, `params` (MAP<VARCHAR,VARCHAR>), `secret`
  - `datasphere_read_analytical(space_id, asset_id [, secret])`
    - Named params: `top`, `skip`, `params` (MAP<VARCHAR,VARCHAR>), `secret`, `metrics` (LIST<VARCHAR>), `dimensions` (LIST<VARCHAR>)

Tips:

- `params` passes input parameters where required by the service definition.
- `metrics` and `dimensions` are translated into an OData `$select` automatically.
- Snake_case column names are used consistently in outputs.

### Secrets and OAuth2 flows

Create secrets using providers:

```sql
-- OAuth2 provider (interactive authorization_code or client_credentials)
CREATE SECRET datasphere (
  TYPE datasphere,
  PROVIDER oauth2,
  TENANT_NAME 'your-tenant',
  DATA_CENTER 'eu10',
  SCOPE 'default'
);

-- Config provider (INI-like file with key=value)
CREATE SECRET datasphere_cfg (
  TYPE datasphere,
  PROVIDER config,
  CONFIG_FILE '/path/to/datasphere.conf'
);

-- File provider (store path to credentials artifact)
CREATE SECRET datasphere_file (
  TYPE datasphere,
  PROVIDER file,
  FILEPATH '/secure/path/creds.json'
);
```

Token management is automatic. For `client_credentials`, provide `client_id`, `client_secret`, and either `token_url` or both `tenant_name` and `data_center`. For `authorization_code`, a short local callback server is used to collect the code, then tokens are persisted back into the DuckDB secret.

---

## 🔎 Tracing & Diagnostics

Enable rich tracing to debug network calls and pushdown logic:

```sql
SET erpl_trace_enabled = TRUE;
SET erpl_trace_level   = 'DEBUG';   -- TRACE, DEBUG, INFO, WARN, ERROR
SET erpl_trace_output  = 'both';    -- console, file, both
-- Optional file tuning
SET erpl_trace_file_path   = './erpl_trace.log';
SET erpl_trace_max_file_size = 10485760;  -- 10MB
SET erpl_trace_rotation      = TRUE;
```

What you get:

- URLs, headers, timing, pagination, and retry info
- OData metadata and column extraction
- Datasphere endpoint selection details and schema summaries

---

## 🧠 Performance & Resilience

- Response caching for HTTP; metadata caching for OData
- Exponential backoff and retry handling for transient errors
- Connection reuse/keep-alive for efficient throughput

Best practices:

- Use `SELECT ... LIMIT ...` to control row counts during exploration
- Favor predicate pushdown filters to reduce transferred data
- For analytical reads, specify `metrics`/`dimensions` to generate optimal `$select`

---

## 🧪 Testing

```bash
make test
```

If you need to run the C++ unit tests binary directly, use your build folder path. For example:

```bash
./build/debug/extension/erpl_web/test/cpp/erpl_web_tests
```

---

## 📖 Reference

### HTTP named parameters

- `headers`: MAP(VARCHAR, VARCHAR)
- `content_type`: VARCHAR (for request body)
- `accept`: VARCHAR
- `auth`: VARCHAR (`'user:pass'` for BASIC, token string for BEARER)
- `auth_type`: VARCHAR (`'BASIC'|'BEARER'`)
- `timeout`: BIGINT (ms)

### Datasphere readers

- `datasphere_read_relational(space_id, asset_id)`
  - Named parameters: `secret` VARCHAR, `top` UBIGINT, `skip` UBIGINT, `params` MAP<VARCHAR,VARCHAR>

- `datasphere_read_analytical(space_id, asset_id)`
  - Named parameters: `secret` VARCHAR, `top` UBIGINT, `skip` UBIGINT, `params` MAP<VARCHAR,VARCHAR>, `metrics` LIST<VARCHAR>, `dimensions` LIST<VARCHAR>

---

## ☁️ SAP Analytics Cloud (SAC)

Query planning models, analytics models, and stories from SAP Analytics Cloud directly in SQL with automatic schema discovery and predicate pushdown.

### Setup SAC Secret

```sql
-- Create SAC OAuth2 secret
CREATE SECRET my_sac (
  TYPE sac,
  PROVIDER oauth2,
  TENANT_NAME 'your-tenant',
  REGION 'eu10',                    -- eu10, us10, ap10, ca10, jp10, au10, br10, ch10
  CLIENT_ID 'your-client-id',
  CLIENT_SECRET 'your-client-secret',
  SCOPE 'openid'
);
```

### SAC Discovery Functions

Discover available models and stories:

⚠️ **Implementation Note:** SAC catalog discovery functions (`sac_show_models`, `sac_show_stories`, `sac_get_model_info`, `sac_get_story_info`) are **stub implementations** and currently return empty results. For querying SAC data, use the fully functional data reading functions listed below instead.

```sql
-- For data reading (fully functional):
SELECT * FROM sac_read_planning_data('REVENUE_MODEL', secret := 'my_sac', top := 1000);
SELECT * FROM sac_read_analytical('SALES_CUBE', secret := 'my_sac', dimensions := 'Territory', measures := 'SalesAmount');
SELECT * FROM sac_read_story_data('DASHBOARD_001', secret := 'my_sac');
```

### SAC Data Reading

Query data directly from SAC models:

```sql
-- Read planning model data
SELECT *
FROM sac_read_planning_data('REVENUE_MODEL', secret := 'my_sac', top := 1000);

-- Read analytics model with dimension/measure filtering
SELECT *
FROM sac_read_analytical(
  'SALES_CUBE',
  secret := 'my_sac',
  dimensions := 'Territory,Product,Quarter',
  measures := 'SalesAmount,UnitsShipped',
  top := 5000
);

-- Extract data from stories
SELECT * FROM sac_read_story_data('DASHBOARD_001', secret := 'my_sac');
```

### SAC ATTACH (Direct SQL Access)

For direct SQL access to SAC OData services:

```sql
-- Attach SAC instance
ATTACH 'https://your-tenant.eu10.sapanalytics.cloud' AS sac (
  TYPE sac,
  SECRET my_sac
);

-- Query attached models
SELECT * FROM sac.Planning_Models WHERE ID = 'REVENUE_MODEL';
SELECT * FROM sac.Stories WHERE Owner = 'john.doe@company.com';
```

### SAC Functions Reference

**Discovery Functions (⚠️ Stub Implementations - Return Empty Results):**
- `sac_show_models()`
  - Named parameters: `secret` VARCHAR
  - Returns: id, name, description, type, owner, created_at, last_modified_at

- `sac_show_stories()`
  - Named parameters: `secret` VARCHAR
  - Returns: id, name, description, owner, created_at, last_modified_at, status

- `sac_get_model_info(model_id)`
  - Named parameters: `secret` VARCHAR
  - Returns: id, name, description, type, dimensions (comma-separated), created_at

- `sac_get_story_info(story_id)`
  - Named parameters: `secret` VARCHAR
  - Returns: id, name, description, owner, status, created_at, last_modified_at

**Data Reading Functions (✅ Fully Functional):**
- `sac_read_planning_data(model_id)`
  - Named parameters: `secret` VARCHAR, `top` UBIGINT, `skip` UBIGINT
  - Returns: All columns from the planning model (auto-detected schema)

- `sac_read_analytical(model_id)`
  - Named parameters: `secret` VARCHAR, `top` UBIGINT, `skip` UBIGINT, `dimensions` VARCHAR, `measures` VARCHAR
  - Returns: Selected dimensions and measures with aggregated data

- `sac_read_story_data(story_id)`
  - Named parameters: `secret` VARCHAR
  - Returns: Data used in story visualizations

---

## 🔐 Telemetry

Telemetry is optional and can be disabled at any time:

```sql
SET erpl_telemetry_enabled = FALSE;
-- Optionally set a custom API key
SET erpl_telemetry_key = 'your-posthog-key';
```

Data collected: extension/DuckDB version, OS/arch, anonymized function usage. No sensitive content is collected.

---

## 🤝 Contributing

Issues and PRs are welcome! Please refer to CONTRIBUTING.md for guidance.

---

## 📄 License

This project is licensed under the Business Source License (BSL) 1.1. See [LICENSE](./LICENSE).

---

Build API-powered analytics with DuckDB + ERPL Web. Query the web like it’s a table. 🚀
