<a name="top"></a>

# ERPL Web — DuckDB HTTP + OData + SAP Datasphere Extension

ERPL Web is a production-grade DuckDB extension that lets you call HTTP/REST APIs, query OData v2/v4 services, and work with SAP Datasphere assets directly from SQL. It brings secure OAuth2, DuckDB Secrets integration, predicate pushdown, robust tracing, and smart caching into a single, easy-to-use package.

- SEO topics: DuckDB HTTP client, DuckDB REST, DuckDB OData v2/v4, DuckDB SAP Datasphere, OAuth2 for DuckDB, OData ATTACH, query APIs from SQL.

## ✨ Highlights

- HTTP from SQL: GET, HEAD, POST, PUT, PATCH, DELETE with headers, auth, and body
- OData v2/v4: Universal reader with automatic version handling and pushdown
- SAP Datasphere: List spaces/assets, describe assets, and read relational/analytical data
- OAuth2 + Secrets: Secure flows with refresh, client credentials, and secret providers
- Tracing: Deep, configurable tracing for debugging and performance tuning
- Caching + Resilience: Response caching, metadata caching, and retry logic
- Charset handling: Automatic UTF‑8/ISO‑8859‑1/‑15/Windows‑1252 detection and conversion

---

## 🚀 Install

```sql
-- Install (DuckDB requires -unsigned when installing from a custom URL)
INSTALL 'erpl_web' FROM 'http://get.erpl.io';
LOAD 'erpl_web';
```

Build from source (developers):

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
- `http_delete(url, [headers], [accept], [auth], [auth_type], [timeout])`

Examples:

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

Authentication and secrets:

```sql
-- Basic auth
SELECT * FROM http_get('https://api.example.com/secure', auth:='user:pass', auth_type:='BASIC');

-- Bearer token
SELECT * FROM http_get('https://api.example.com/secure', auth:='token', auth_type:='BEARER');

-- DuckDB HTTP secrets are picked up automatically by scope
CREATE SECRET (
  TYPE http_bearer,
  TOKEN 'your-token',
  SCOPE 'https://api.example.com/'
);
SELECT * FROM http_get('https://api.example.com/secure');
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

- `datasphere_read_relational(space_id, asset_id [, secret])`
  - Named: `top`, `skip`, `params` MAP<VARCHAR,VARCHAR>, `secret`

- `datasphere_read_analytical(space_id, asset_id [, secret])`
  - Named: `top`, `skip`, `params` MAP<VARCHAR,VARCHAR>, `metrics` LIST<VARCHAR>, `dimensions` LIST<VARCHAR>, `secret`

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

Issues and PRs are welcome! Please see CONTRIBUTING.md for guidance.

---

## 📄 License

This project is licensed under the Business Source License (BSL) 1.1. See [LICENSE](./LICENSE).

---

Build API-powered analytics with DuckDB + ERPL Web. Query the web like it’s a table. 🚀