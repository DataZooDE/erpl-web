Here's a tight, implementation‑oriented research brief for SAP Datasphere OData Consumption APIs (both Analytical and Relational) so you can extend ERPL to wrap them cleanly (read first, with a view to where write is/isn't possible). I focused on what you need to build robust adapters: endpoints, query features, auth, metadata, and constraints.

**Key Update**: After analyzing SAP's official CLI implementation, we've discovered critical insights about their actual discovery mechanism and OAuth2 flow that significantly impact our implementation strategy.

What these APIs are (and aren't)
	•	Two OData services: Analytical (multidimensional/cube semantics) and Relational (tabular views). SAP positions both under the "OData API" umbrella for consuming assets exposed from Datasphere.  ￼
	•	Consumption‑oriented (read): Official docs and SAP's own overviews consistently frame the OData API as for consuming views or Analytic Models. (Think SAC, 3rd‑party BI tools, REST clients.)  ￼ ￼
	•	OData version: exposed as OData v4 (JSON). API Hub lists "ODATA V4 API | SAP Datasphere" for consumption endpoints.  ￼

Write access via these OData consumption endpoints is not positioned/guaranteed; SAP's own "External Access Overview" calls the OData API mainly for consumption. For programmatic writes, customers typically use SQL (Open SQL schema via HANA Cloud ODBC/JDBC) or other ingestion mechanisms—not OData consumption endpoints.  ￼ ￼

Endpoint overview (how you'll present them in ERPL)

**CRITICAL INSIGHT FROM SAP CLI ANALYSIS**: SAP does NOT expose OData endpoints directly for discovery. Instead, they use a centralized discovery mechanism at `/dwaas-core/api/v1/discovery` that provides OpenAPI 3.0.3 documents containing all available endpoints and capabilities.

SAP exposes two OData service roots per tenant/space, one Analytical and one Relational. The exact base URLs are shown in each tenant (Administration → App Integration → OData API / Data Service URL). Blogs/tutorials show using those "Data Service URLs" in tools (SAC, Power BI, etc.). You should not hardcode paths; instead let users supply the Data Service URL and ERPL should auto‑discover service metadata and entity sets.  ￼ ￼

**SAP's Actual Discovery Architecture**:
- **Primary Discovery**: `/dwaas-core/api/v1/discovery` (OpenAPI 3.0.3)
- **OAuth Discovery**: `/oauth` endpoint for pre-delivered clients
- **OData Consumption**: `/api/v1/dwc/consumption/{analytical|relational}/{space_id}/{asset_id}`

Aspect	Analytical API	Relational API
Purpose	Query Analytic Models (cubes): measures, dimensions, hierarchies, variables/prompts	Query Views / tables exposed as consumable assets
OData features	OData v4 + Data Aggregation Extension via $apply (groupby/aggregate), plus standard $select,$filter,$orderby,$top,$skip,$count	OData v4 standard: $select,$filter,$orderby,$top,$skip,$count; tabular semantics
Prereqs	Asset must be an Analytic Model and "consumption‑enabled"	View must be exposed for consumption
Row‑level security	DAC (Data Access Controls) enforced at runtime	Same
Metadata	EDM + analytic semantics (measures/dimensions)	EDM (entity sets, properties, types)
Typical clients	SAC, Power BI (import), other BI	Any tool needing tabular OData

(Features drawn from SAP's OData consumption articles and the OData Data Aggregation extension spec.)  ￼ ￼ ￼

Query semantics you need to support

Common (both services)
	•	Service document/$metadata (EDMX) for entity discovery. Expect OData v4 JSON for data, XML for metadata.  ￼
	•	System query options: $select, $filter, $orderby, $top, $skip, $count=true. (Expose via ERPL SQL pushdown mapping as you already do for OData v4.)  ￼
	•	Paging with @odata.nextLink. Handle server‑side continuation properly. (Map to DuckDB LIMIT/OFFSET where possible, otherwise follow nextLink.)  ￼
	•	Variables/parameters: analytic content often defines variables/prompts; clients pass them as query parameters. Your adapter should allow bind parameters in SQL mapped to OData params.  ￼
	•	Security: DAC applies row‑level filters—results vary per user/token. Don't cache cross‑user.  ￼

Analytical API specifics
	•	Aggregation via $apply (OData Data Aggregation ext v4): groupby((dim1,dim2), aggregate(m1 with sum as Sales)), optionally chained with filter, orderby, top, skip. You should translate SQL aggregates/group by into $apply where feasible (or fall back to client‑side if necessary).  ￼
	•	Exception aggregations, hierarchies, input variables may apply depending on the Analytic Model; follow asset metadata and pass variables.  ￼ ￼

Relational API specifics
	•	Tabular semantics: standard OData v4 entity sets, properties, types. No cube functions; treat like normal OData v4 with pushdown of $select/$filter/....

Authentication (what to implement)

**MAJOR INSIGHT FROM SAP CLI**: SAP's OAuth2 implementation is more sophisticated than standard PKCE flows. They use a local HTTP server approach with specific port strategies and browser integration.

Datasphere supports multiple auth modes for consumption APIs; you should plug into ERPL's secret chain and standardize on OAuth2 with fallbacks where available.
	•	OAuth 2.0 clients in Datasphere: Admins create OAuth2 clients (App Integration). You'll need Auth URL, Token URL, Client ID/Secret, Scopes. Support Authorization Code (3‑legged) and Client Credentials (2‑legged) where permitted.  ￼
	•	SAML Bearer Assertion (IAS): common in SAP landscapes; guide shows using IAS to obtain tokens for Datasphere OData consumption (SAML→OAuth exchange). Worth supporting for enterprise SSO.  ￼
	•	Generic OData connection auth variants in Datasphere docs: No Auth, Basic, OAuth 2.0 listed for generic connections—useful signal of modes your adapter should accept in testing, but production tenants typically require OAuth.  ￼

**SAP's OAuth2 Implementation Details**:
- **Local HTTP Server**: Port 8080 for custom clients, 65000 for pre-delivered clients
- **Browser Integration**: Automatic browser opening for authorization
- **Timeout Handling**: 30-second timeout for authorization code reception
- **Port Flexibility**: Configurable via `CLI_HTTP_PORT` environment variable
- **Client Type Distinction**: 
  - Pre-delivered OAuth Client: ID `5a638330-5899-366e-ac00-ab62cc32dcda`
  - Custom OAuth Client: ID starting with `sb-` (e.g., `sb-00bb7bc2-cc32-423c-921c-2abdee11a29d!b49931|client!b3650`)
- **URL Auto-discovery**: For pre-delivered clients, URLs are fetched from `/oauth` endpoint

How tools/clients use it (for adapter UX parity)
	•	SAC "OData Service" connections use the Data Service URL + OAuth 2.0 Authorization Code; SAP KBA shows the exact fields and that the Auth/Token URLs come from the Datasphere "App Integration" page. This is the same info ERPL should request/store in its secret chain.  ￼
	•	Community guides show Power BI/others importing via the provided Data Service URL; errors commonly stem from mis‑auth or using the wrong endpoint (must be OData v4). Keep error messages explicit.  ￼

What to abstract for ERPL users
	•	Single logical catalog: Hide Analytical vs Relational behind a unified ERPL namespace. E.g., expose:
	•	datasphere.<space>.analytical.<model_name>
	•	datasphere.<space>.relational.<view_name>
ERPL resolves to the correct service root automatically (by reading the service document) and exposes columns with proper types/semantics.
	•	Transparent predicate/aggregation pushdown:
	•	Map SELECT … FROM analytical.model WHERE … GROUP BY … into $apply groupby/aggregate (Analytical), and standard query options (Relational).
	•	Push down LIMIT/OFFSET as $top/$skip.
	•	Push down COUNT(*) as $count=true or /$count.
	•	Variables: enable WITH (var1 => '2025-01-01', var2 => 'EU') table hints / function parameters on FROM clause that the adapter converts to OData query parameters for models with variables.
	•	Security: bind token/tenant/space in the ERPL connection; do not cache data across tokens.

Write access: what's feasible
	•	Consumption OData: designed for read. SAP public guidance positions this API for consumption; no official write (POST/PATCH) semantics are documented for Datasphere consumption endpoints. Use SQL for writes (Open SQL schema via JDBC/ODBC) or Datasphere ingestion tooling.  ￼ ￼
	•	If you want to expose ERPL "DuckDB Storage API" DDL/DML and "translate to API calls", do that for SQL channels (HANA Cloud) rather than OData consumption. Keep OData strictly for reads to avoid brittle behavior and surprises for customers.

Practical notes & gotchas (to bake into tests)
	•	OData v4 only: Power BI/forum threads confirm you must use the v4 endpoints. Validate the service root by fetching $metadata and checking OData-Version in headers. Clear 4xx errors if v2 clients hit it.  ￼
	•	DAC / RLS: results differ per token; integration tests must authenticate and assert DAC behavior (e.g., filtered rows).  ￼
	•	Pagination: Analytical queries with $apply can still paginate; always follow @odata.nextLink.  ￼
	•	Performance: multiple vendors note OData is slower than JDBC/ODBC for large extracts; encourage pushdown and avoid client‑side aggregation if possible.  ￼

**SAP CLI-Specific Gotchas**:
- **Discovery Timeout**: Discovery document fetching can timeout; implement proper retry logic
- **ETag Handling**: SAP uses ETags for discovery document caching; implement proper ETag comparison
- **Port Conflicts**: Local server ports (8080, 65000) can conflict with existing services
- **Browser Integration**: Different platforms handle browser opening differently; need robust fallbacks
- **Client Type Detection**: Must correctly identify custom vs pre-delivered clients for proper URL construction

Minimal acceptance checklist for your adapter

Area	Must‑have checks (ERPL integration tests)
Discovery	Fetch discovery document from `/dwaas-core/api/v1/discovery` and extract OData endpoints; list analytic models and relational views; expose as ERPL tables.  ￼
Read (Relational)	SELECT col… FROM … WHERE … ORDER BY … LIMIT n → $select/$filter/$orderby/$top with correct results and @odata.nextLink handling.  ￼
Read (Analytical)	SELECT dim…, SUM(measure)… GROUP BY dim… → $apply=groupby(…,aggregate(…)); verify totals vs reference.  ￼
Variables	Provide variables via table hints/parameters; verify they influence result set.  ￼
Count	SELECT COUNT(*) maps to $count=true or /$count; numeric result matches.  ￼
Auth	OAuth2: Authorization Code + Client Credentials; optionally SAML Bearer (IAS). Token refresh flows covered; secrets stored in ERPL chain.  ￼ ￼
Security	With DAC on an asset, user A vs user B (two tokens) receive different rows as expected.  ￼
Errors	Wrong endpoint or v2 URL: helpful error ("Use Datasphere OData v4 service URL").  ￼

**Updated SAP CLI-Specific Checklist**:
- **OAuth2 Flow**: Local HTTP server opens browser, handles callback, exchanges code for tokens
- **Port Strategy**: Correctly uses port 8080 (custom) vs 65000 (pre-delivered)
- **Discovery Integration**: Successfully fetches and parses OpenAPI 3.0.3 discovery documents
- **Client Type Detection**: Correctly identifies and handles custom vs pre-delivered OAuth clients
- **ETag Caching**: Implements proper ETag-based caching for discovery documents
- **Timeout Handling**: 30-second timeout for OAuth2 authorization with proper error messages

Pointers to primary sources
	•	SAP help – "Consume Data via the OData API" (purpose, how clients consume views & analytic models; Data Service URL lives in tenant):  ￼
	•	SAP blog – "SAP Datasphere: Analytical and Relational OData APIs" (overview; both services; examples):  ￼
	•	OAuth 2.0 in Datasphere – create OAuth clients (Auth/Token URLs, client credentials):  ￼
	•	SAML bearer to OAuth for Datasphere OData (enterprise SSO pattern):  ￼
	•	Analytical semantics / $apply – OData Data Aggregation v4.0 spec:  ￼
	•	External access overview (OData mainly for consumption; other channels for SQL):  ￼
	•	Perf caveat (vendor doc) – OData slower vs JDBC/ODBC for large extracts:  ￼

**New SAP CLI Implementation Sources**:
- **SAP CLI Core**: `@sap/cli-core` package implementation for OAuth2 and discovery patterns
- **SAP Datasphere CLI**: `@sap/datasphere-cli` package for Datasphere-specific patterns
- **Discovery Schema**: OpenAPI 3.0.3 schema validation for discovery documents
- **OAuth2 Flow**: Local HTTP server implementation with browser integration
- **Secret Management**: SAP's proven secret storage and token management patterns
