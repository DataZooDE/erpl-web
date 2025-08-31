# OData-ODP Replication: Concepts and Client Contract

## Scope

This document explains how to consume SAP ODP sources over OData ("OData-ODP") and defines the client contract for ERPL (DuckDB target). We want to build the ODP functionality on top of existing functionality for OData Metadata and OData Feed parsing. However ODP needs some additions with regards to persistent subscriptions to manage delta replication.

## What Is OData-ODP

- ODP is SAP's framework for extracting full and delta changes from defined providers into an Operational Delta Queue (ODQ).
- SAP discourages third-party use of the classic RFC ODP API; use OData-ODP instead (OData v2 over SAP Gateway).
- An SAP Gateway service exposes a single ODP provider as an OData entity set and surfaces delta state via delta links.
- ODP services typically have entity sets named with patterns like `EntityOf<Provider>` or `FactsOf<Provider>`.

## Prerequisites (Server)

- NetWeaver AS ABAP with SAP Gateway Foundation (7.50 SP02+ typical).
- A generated/activated OData v2 service (SEGW) that redefines an ODP provider (Extractor, CDS for extraction, BW, SLT, etc.).
- Entity sets commonly include:
  - Main data set: records of the ODP provider (e.g., `EntityOfSEPM_ISO`).
  - Delta links helper: often named `DeltaLinksOf<Provider>` (exact naming varies by template).
  - Termination function: often named `TerminateDeltasFor<Provider>` for subscription cleanup.
- One active ODP subscription per service+user is typical; avoid concurrent runs for the same user.

## Client Responsibilities (ERPL)

- Perform initial full load with change tracking, persist the returned delta token, and write rows into DuckDB.
- Periodically call the delta link to fetch only changes since the last token.
- Guarantee idempotency and correctness under retries, pagination, and partial failures.
- Handle soft deletes and update semantics from the source (I/U/D or change-mode indicators).
- Track schema and type mapping to DuckDB (DECIMAL, DATE/TIME, NVARCHAR sizes, nullability).
- Manage subscription lifecycle: establish, monitor, and terminate delta subscriptions.

## OData Service Discovery of SAP Systems (Not OData Specific)

 - SAP System typically exposes two SAP OData catalog endpoints to discover available services:
   - OData V2: /sap/opu/odata/iwfnd/catalogservice;v=2/ServiceCollection
   - OData V4: /sap/opu/odata4/iwfnd/config/default/iwfnd/catalog/0002/ServiceGroups?$expand=DefaultSystem($expand=Services())
   - Both endpoints have to be queried for discovery of ODP services.
 - Service Information Retrieved: For each discovered service, the connector extracts:
   - Service ID/Name: Technical identifier
   - Description: Human-readable description
   - Service URL: Endpoint for accessing the service
   - Version: OData V2 or V4
   - Metadata URL: Location of the service metadata

## ODP Service Discovery of SAP Systems (OData Specific)

 - We use  a service catalog approach specifically designed for ODP (Open Data Provisioning) services. 
 - The endpoint is /sap/opu/odata/IWFND/CATALOGSERVICE;v=2/ServiceCollection (which is the same as the general OData V2 catalog service endpoint above)
 - To detect the ODP Services (IsOdpRelatedEntitySetName) we check whether the entity set name starts with "EntityOf" or "FactsOf" (case insensitive)
 - This method filters services to only include those with entity sets that follow ODP naming conventions:
   - EntityOf: For entity-based ODP sources (tables, views)
   - FactsOf: For fact-based ODP sources (DataSources)
 - Service Information Retrieved
   - Service ID: Technical identifier
   - Description: Human-readable description
   - Service URL: Endpoint for accessing the service
   - Entity Sets: List of available ODP data collections
 - There is addtional information in the Metdata (EDM AdditionalAttributes)
  - It looks for sap:change-tracking 

## ODP OData Protocol Specification

### Metadata Structure and ODP Extensions

The ODP OData service metadata follows the standard OData v2 EDMX format with SAP-specific extensions:

#### **Root Structure**
- **EDMX Version**: Always "1.0" for OData v2
- **DataServiceVersion**: "2.0" (OData v2 standard)
- **Namespaces**: 
  - `edmx`: Microsoft OData metadata
  - `m`: Microsoft data services metadata  
  - `sap`: SAP-specific protocol extensions

#### **Schema Elements**
- **Namespace**: Service-specific namespace (e.g., `Z_ODP_BW_1_SRV`)
- **Schema Version**: `sap:schema-version` attribute indicates metadata version
- **Language**: `xml:lang` attribute for internationalization

#### **Entity Type Structure for ODP**

##### **Key Properties (Primary Key)**
- **Purpose**: Uniquely identify each record in the ODP fact table
- **Structure**: Multiple `<PropertyRef>` elements under `<Key>` section
- **Semantics**: All key properties are `Nullable="false"` and represent dimensional attributes

##### **Dimension Properties**
- **Naming Convention**: Typically prefixed with `D_` (e.g., `D_NW_CHANN`, `D_NW_SORG`)
- **Attributes**:
  - `sap:aggregation-role="dimension"`: Identifies dimensional attributes
  - `sap:label`: Human-readable description
  - `sap:creatable="false"`: Read-only in OData context
  - `sap:updatable="false"`: Read-only in OData context
  - `sap:semantics`: Semantic classification (e.g., `"yearmonth"`, `"year"`, `"currency-code"`)

##### **Measure Properties**
- **Naming Convention**: Typically represent numeric values (e.g., `D_NW_NETV`, `D_NW_QUANT`)
- **Attributes**:
  - `sap:aggregation-role="measure"`: Identifies measurable values
  - `sap:unit`: Unit of measurement (e.g., `"CURRENCY"`, `"UNIT"`)
  - `sap:filterable="false"`: Often not filterable for performance reasons
  - **Type Constraints**: `Edm.Decimal` with specific `Precision` and `Scale`

##### **ODP-Specific Properties**
- **Change Mode**: `ODQ_CHANGEMODE` with `sap:semantics="change-mode"`
  - **Purpose**: Indicates the type of change (Insert, Update, Delete)
  - **Format**: Single character string (e.g., "I", "U", "D")
- **Entity Counter**: `ODQ_ENTITYCNTR` with `sap:semantics="entity-counter"`
  - **Purpose**: Monotonic counter for entity identification
  - **Format**: High-precision decimal (typically Precision 37, Scale 0)

#### **Delta Links Entity Type**

##### **Structure**
- **Naming Convention**: `DeltaLinksOf{EntitySetName}` (e.g., `DeltaLinksOfFactsOf0D_NW_C01`)
- **Purpose**: Manages delta tokens for change tracking and incremental data loading

##### **Properties**
- **DeltaToken**: Primary key, string value representing the delta state
- **CreatedAt**: Timestamp when the delta token was created
- **IsInitialLoad**: Boolean flag indicating if this represents an initial load

##### **Navigation Properties**
- **ChangesAfter**: Links to the main entity set for delta queries

#### **Entity Container and Entity Sets**

##### **Container Attributes**
- **IsDefaultEntityContainer**: `m:IsDefaultEntityContainer="true"` identifies the primary container
- **Supported Formats**: `sap:supported-formats="atom json xlsx"` lists available response formats

##### **Entity Set Configuration**
- **Change Tracking**: `sap:change-tracking="true"` enables delta replication
- **Content Version**: `sap:content-version` tracks data versioning
- **Max Page Size**: `sap:maxpagesize` sets server-side pagination limits
- **CRUD Operations**: `sap:creatable="false"`, `sap:updatable="false"`, `sap:deletable="false"` for read-only access

#### **Function Imports for ODP Management**

##### **Subscription Functions**
- **SubscribedTo{EntitySet}**: Returns `ChangeTrackingDetails` complex type
  - **Purpose**: Check subscription status
  - **Return Type**: Contains `SubscribedFlag` boolean
- **TerminateDeltasFor{EntitySet}**: Returns `SubscriptionTerminationDetails` complex type
  - **Purpose**: Clean up delta subscriptions
  - **Return Type**: Contains `ResultFlag` boolean indicating success

##### **Complex Types**
- **ChangeTrackingDetails**: Subscription status information
- **SubscriptionTerminationDetails**: Termination operation results

#### **Associations and Navigation**

##### **Association Structure**
- **Name**: Follows pattern `{entitySetName}` (e.g., `factsOf0D_NW_C01`)
- **End Points**: Links `DeltaLinksOf{EntitySet}` (1) to main `{EntitySet}` (*)
- **Multiplicity**: One-to-many relationship between delta links and data records

##### **Association Sets**
- **Purpose**: Expose associations as navigable endpoints
- **CRUD Operations**: Typically read-only (`sap:creatable="false"`, etc.)

### URL Construction for ODP
- **Delta tokens**: Use exclamation mark format `!deltatoken={token}` (OData v2 standard)
- **Query parameters**: Always include `$format=json` for JSON responses
- **Base URL handling**: Ensure proper trailing slash handling in URL construction

## Initial Load (subscribe to delta)

- **Request**: GET main entity set with header `Prefer: odata.track-changes` and `$format=json`.
- **Response**: returns full dataset (paged) and a delta link URL containing a delta token (e.g., `…!deltatoken=…`).
- **Action**: ingest pages, persist rows, then persist the final delta token in ERPL state.
- **Verification**: `ODQMON` in SAP shows an entry for the service/user as a delta subscriber.
- **Critical**: Verify the `preference-applied` header contains `odata.track-changes` to confirm subscription was established.

## Delta Fetch (incremental)

- **Request**: follow the delta link returned by the previous call (no `Prefer` header).
- **Response**: a page of changes plus a new delta link; when no changes, expect empty result with a fresh token.
- Changes can include inserts, updates, and deletions; services expose change mode fields or tombstones depending on provider.
- **Action**: apply changes transactionally in DuckDB, then atomically advance the stored token.

## Delta Token Discovery and Management

- **Discovery Pattern**: Query the `DeltaLinksOf<EntitySet>` entity set to find available delta tokens.
- **Token Selection**: Select the latest token based on `CreatedAt` timestamp from the delta links entity.
- **Token Validation**: Ensure the returned delta token is valid and not expired.
- **Fallback Strategy**: If delta links entity doesn't exist, fall back to full extraction mode.

## Subscription Lifecycle Management

- **Establishment**: Use `Prefer: odata.track-changes` header to create subscription.
- **Monitoring**: Track subscription health through delta token validity and response patterns.
- **Termination**: Call `TerminateDeltasFor<EntitySet>` function to clean up subscriptions.
- **Recovery**: Re-establish subscription if tokens expire or become invalid.

## Pagination and Flow Control

- Expect server-driven paging (`nextLink`) and/or `$skiptoken`. Loop until no `nextLink`.
- Do not run parallel extractions for the same subscription; process sequentially.
- Choose conservative page sizes if client-driven paging is enabled; favor server defaults if unsure.
- Use `Prefer: odata.maxpagesize=<size>` header for client-controlled paging when needed.

## Error Handling and Recovery

- **Lost/expired token** (HTTP 410/404 or service-specific error): re-run initial load with tracking to re-establish state.
- **Conflicting subscription** (another run active): back off or use a distinct SAP user.
- **Authorization errors**: ensure the SAP user is authorized for both the Gateway service and ODP provider.
- **Network/timeout**: use bounded retries with backoff; ensure idempotent upserts and delete application.
- **Delta preference not applied**: if `preference-applied` header doesn't contain `odata.track-changes`, fall back to full extraction.
- **Delta links entity missing**: if `DeltaLinksOf<EntitySet>` returns 404, service doesn't support delta extraction.

## Data Semantics and Mapping

- **Keys**: derive from the OData metadata; use for upsert matching in DuckDB.
- **Deletions**: either explicit tombstones or change-mode flags; map to DELETEs in DuckDB as appropriate.
- **Types**: mind precision/scale for DECIMAL, timezone behavior for DATETIME, and string length constraints.
- **Schema evolution**: detect added/removed fields on metadata refresh; apply non-breaking migrations first.

## Service Discovery and Catalog

- **ODP Service Detection**: Identify ODP services by entity set naming patterns (`EntityOf*`, `FactsOf*`).
- **Service Catalog**: Maintain catalog of available ODP services with metadata and capabilities.
- **Entity Set Discovery**: Discover available entity sets and their delta capabilities through `$metadata`.
- **Capability Assessment**: Determine delta support, change tracking, and termination functions available.

## Security

- Use HTTPS to the Gateway endpoint and store credentials securely per existing ERPL patterns.
- Avoid embedding secrets in logs; redact tokens and credentials.
- Validate subscription scope and permissions before establishing delta connections.

## Authentication & Secrets

- Use DuckDB secrets for HTTP auth against the Gateway host:
  - `http_basic` with `username`/`password` scoped to the base URL (e.g., `https://localhost:50000`).
  - `http_bearer` with an OAuth2 access token scoped to the base URL.
- S/4HANA/BW/4HANA guidance:
  - Trial/on-prem: HTTPS + Basic auth is simplest for local testing.
  - Production: configure OAuth2 on Gateway (`OA2C_CONFIG`) or use S/4HANA Cloud communication arrangements; obtain tokens from the platform token endpoint and store as `http_bearer` secret.
- Registration stores `secret_name`; all requests resolve and apply the secret at runtime, with credentials redacted in logs.


## Operational Guidance

- **Monitoring**: `ODQMON` for subscription state; Gateway error logs for request failures.
- **Scheduling**: run deltas on a cadence aligned with ODQ retention to avoid token expiry.
- **Backfill**: re-run initial load during major schema changes or after long outages.
- **Subscription Cleanup**: Regularly terminate unused subscriptions to prevent resource accumulation.

## Concrete Implementation Details

!!! Always reuse existing ERPL HTTP client and OData parsing functionality !!!
If changes are needed, make sure to update the existing functionality. Ask before introducing big new refactorings and features.

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

### HTTP Headers and Preferences
- **OData Version**: `DataServiceVersion: 2.0`, `MaxDataServiceVersion: 2.0`
- **Change Tracking**: `Prefer: odata.track-changes`
- **Page Size**: `Prefer: odata.maxpagesize=15000`
- **Accept**: `application/json` for data, `application/xml` for metadata
- **Verification**: Check `preference-applied: odata.track-changes` response header

### OData v2 URL Structure and Parameters

- **Base URL Pattern**: `{gateway_host}/sap/opu/odata/sap/{SERVICE_NAME}/`
- **Entity Set URL**: `{base_url}/{EntitySet}?$format=json`
- **Delta Token Format**: `!deltatoken={token}` (note the exclamation mark, not question mark)
- **Metadata URL**: `{base_url}/$metadata`
- **Service Catalog URL**: `{gateway_host}/sap/opu/odata/IWFND/CATALOGSERVICE;v=2/ServiceCollection?search={pattern}&$format=json&$expand=EntitySets`

### OData Query Parameters

- **Format**: `$format=json` (always required for JSON responses)
- **Select**: `$select=field1,field2,field3` (field selection)
- **Filter**: `$filter=field1 eq 'value' and field2 ge 100` (filtering)
- **Top**: `$top=1000` (limit results per page)
- **Skip**: `$skiptoken={token}` (pagination token)
- **Expand**: `$expand=EntitySets` (expand related entities)

### HTTP Headers

- **OData Version**: `DataServiceVersion: 2.0`, `MaxDataServiceVersion: 2.0`
- **Change Tracking**: `Prefer: odata.track-changes`
- **Page Size**: `Prefer: odata.maxpagesize=1000`
- **Accept**: `application/json` for data, `application/xml` for metadata
- **Authorization**: `Basic {base64_credentials}` or `Bearer {token}`
- **Preference Applied**: `preference-applied: odata.track-changes` (response header to verify subscription)

### JSON Response Structure (OData v2)

- **Root Object**: `{"d": {...}}` (data wrapper)
- **Results Array**: `{"d": {"results": [...]}}`
- **Pagination**: `{"d": {"results": [...], "__next": "next_page_url"}}`
- **Delta Links**: `{"d": {"results": [...], "__delta": "delta_link_url"}}`
- **Metadata Context**: `{"d": {"__metadata": {...}}}`

### Filter Query Syntax

- **Equality**: `field eq 'value'`
- **Inequality**: `field ne 'value'`
- **Greater/Less**: `field gt 100`, `field lt 100`
- **Greater/Less Equal**: `field ge 100`, `field le 100`
- **Logical Operators**: `and`, `or`, `not`
- **Grouping**: `(field1 eq 'value1' or field2 eq 'value2')`
- **String Functions**: `substringof('search', field)`, `startswith(field, 'prefix')`

### Delta Token Discovery Response

- **Delta Links Entity**: `DeltaLinksOf{EntitySet}`
- **Required Fields**: `DeltaToken`, `CreatedAt`
- **Query**: `?$select=DeltaToken,CreatedAt&$format=json`
- **Response Structure**: `{"d": {"results": [{"DeltaToken": "token", "CreatedAt": "/Date(1234567890)/"}]}}`

### Subscription Termination

- **Function Name**: `TerminateDeltasFor{EntitySet}`
- **URL**: `{base_url}/TerminateDeltasFor{EntitySet}?$format=json`
- **Response**: `{"d": {"Success": true/false, "Message": "..."}}`

### Package Size and Performance

- **Default Package Size**: 15,000 rows (Theobald default)
- **Configurable**: via `Prefer: odata.maxpagesize={size}` header
- **Server Limits**: respect server-side pagination limits
- **Memory Management**: process pages sequentially to avoid memory issues

## Example Requests

- **Initial load with change tracking**
  ```
  GET /sap/opu/odata/sap/ZODP_SRV/EntityOfSEPM_ISO?$format=json
  Header: Prefer: odata.track-changes
  ```

- **Delta fetch**
  ```
  GET /sap/opu/odata/sap/ZODP_SRV/EntityOfSEPM_ISO!deltatoken=abc123&$format=json
  ```

- **Delta token discovery**
  ```
  GET /sap/opu/odata/sap/ZODP_SRV/DeltaLinksOfEntityOfSEPM_ISO?$select=DeltaToken,CreatedAt&$format=json
  ```

- **Subscription termination**
  ```
  GET /sap/opu/odata/sap/ZODP_SRV/TerminateDeltasForEntityOfSEPM_ISO?$format=json
  ```

- **Service discovery**
  ```
  GET /sap/opu/odata/IWFND/CATALOGSERVICE;v=2/ServiceCollection?search=ODP&$format=json&$expand=EntitySets
  ```

- **Field selection with filtering**
  ```
  GET /sap/opu/odata/sap/ZODP_SRV/EntityOfSEPM_ISO?$select=ID,Name,Status&$filter=Status eq 'ACTIVE'&$format=json
  ```

## References in This Repo

- Implementation surfaces as ERPL SQL/C++ entry points for `register`, `initial_load`, `delta_pull`, `resubscribe`, and `terminate_subscription`.
- Tests follow the agent-agnostic TDD loop in `.ai/development-workflow.md` using `make test_debug` and C++/SQL runners.

## Authentication

- Supported methods via DuckDB secrets consumed by the ERPL HTTP client:
  - **Basic authentication**: create a `http_basic` secret scoped to the host.
    Example:
      ```sql
      CREATE PERSISTENT SECRET s4_basic (
        TYPE http_basic,
        SCOPE 'https://vhcala4hci:50000',
        username 'USER',
        password 'PASS'
      );
      ```
  - **Bearer token**: create a `http_bearer` secret with an access token (from your OAuth2 server).
    Example:
      ```sql
      CREATE SECRET s4_bearer (
        TYPE http_bearer,
        SCOPE 'https://vhcala4hci:50000',
        token 'eyJhbGciOi…'
      );
      ```
- S/4HANA and BW/4HANA options:
  - **On-prem/trial**: HTTPS + Basic auth is simplest for testing (enable in ICF; assign roles for OData + ODP).
  - **OAuth 2.0** (recommended for prod): configure in SAP Gateway (transaction OA2C_CONFIG). Common grants: client_credentials and authorization_code. Token endpoint typically under `/sap/bc/sec/oauth2/token` on the Gateway host. For S/4HANA Cloud, tokens come from XSUAA (`<subdomain>.authentication.<region>.hana.ondemand.com/oauth/token`) tied to the communication arrangement.
  - **SAML2/Certificates**: supported by Gateway but not required for this plugin; if used, terminate at reverse proxy and inject Bearer.
- **Secret scoping**: ensure `SCOPE` matches the scheme/host/port of your OData base URL so lookups resolve. Do not log credentials; the plugin already redacts secret values.
