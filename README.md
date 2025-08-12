<a name="top"></a>

# ERPL Web

**ERPL Web** is a powerful DuckDB extension that transforms your database into a web-enabled powerhouse. With seamless HTTP integration, OData support, comprehensive tracing, and enterprise-grade features, you can now interact with web services directly from SQL queries.

## ✨ Key Features

- **🌐 HTTP Functions**: Make HTTP requests (GET, POST, PUT, PATCH, DELETE, HEAD) directly from SQL
- **📊 Universal OData Support**: Connect to both OData v2 and v4 services with automatic version detection
- **🔍 Advanced Tracing**: Comprehensive request/response monitoring and debugging
- **🔐 Authentication**: Built-in support for Basic Auth, Bearer tokens, and DuckDB secrets
- **📈 Telemetry**: Optional usage analytics to improve the extension
- **🚀 Performance**: Intelligent caching, retry logic, and connection pooling
- **🔄 Charset Support**: Automatic encoding detection and conversion
- **🔄 Version Transparency**: Zero-configuration OData version handling

## 🚀 Quick Start

### Installation

```sql
-- Install the extension (requires -unsigned flag)
INSTALL 'erpl_web' FROM 'http://get.erpl.io';
LOAD 'erpl_web';
```

### Your First HTTP Request

```sql
-- Simple GET request
SELECT content FROM http_get('https://httpbun.com/ip');

-- Extract JSON data
SELECT content::JSON->>'ip' as ip_address 
FROM http_get('https://httpbun.com/ip');
```

## 🌐 HTTP Functions

### Basic HTTP Operations

| Function | Description | Example |
|----------|-------------|---------|
| `http_get(url)` | HTTP GET request | `SELECT * FROM http_get('https://api.example.com/data')` |
| `http_head(url)` | HTTP HEAD request | `SELECT status FROM http_head('https://example.com')` |
| `http_post(url, body)` | HTTP POST with JSON | `SELECT * FROM http_post('https://api.example.com', {'key': 'value'}::JSON)` |
| `http_put(url, body)` | HTTP PUT with JSON | `SELECT * FROM http_put('https://api.example.com/1', {'name': 'updated'}::JSON)` |
| `http_patch(url, body)` | HTTP PATCH with JSON | `SELECT * FROM http_patch('https://api.example.com/1', {'status': 'active'}::JSON)` |
| `http_delete(url)` | HTTP DELETE request | `SELECT status FROM http_delete('https://api.example.com/1')` |

### Advanced HTTP Features

#### Custom Headers and Authentication

```sql
-- With custom headers
SELECT * FROM http_get('https://api.example.com/data', 
    headers:={'Authorization': 'Bearer token123', 'Custom-Header': 'value'});

-- With authentication
SELECT * FROM http_get('https://api.example.com/protected',
    auth:='username:password',
    auth_type:='BASIC');
```

#### Content Types and Request Bodies

```sql
-- POST with JSON
SELECT * FROM http_post('https://httpbin.org/anything', 
    {'name': 'John', 'age': 30}::JSON);

-- POST with text
SELECT * FROM http_post('https://httpbin.org/anything', 
    'Hello World', 'text/plain');

-- POST with form data
SELECT * FROM http_post('https://httpbin.org/anything', 
    'key1=value1&key2=value2', 'application/x-www-form-urlencoded');
```

### Response Structure

All HTTP functions return a consistent table structure:

| Column | Type | Description |
|--------|------|-------------|
| `method` | VARCHAR | HTTP method used |
| `status` | INTEGER | HTTP status code |
| `url` | VARCHAR | Request URL |
| `headers` | MAP(VARCHAR, VARCHAR) | Response headers |
| `content_type` | VARCHAR | Response content type |
| `content` | VARCHAR | Response body content |

## 📊 OData Integration

### Universal OData Support (v2 & v4)

ERPL Web now provides **universal OData support** that automatically detects and handles both OData v2 and v4 services transparently. No configuration required - the extension automatically:

- **Detects OData version** from service metadata
- **Applies appropriate headers** for each version
- **Parses both JSON formats** (v2: `{"d": [...]}`, v4: `{"value": [...]}`)
- **Handles version-specific features** like `$inlinecount` (v2) and `$count` (v4)

### Connect to OData Services

```sql
-- Attach any OData service (v2 or v4) as a database
ATTACH 'https://services.odata.org/TripPinRESTierService' AS trippin (TYPE odata);  -- OData v4
ATTACH 'https://services.odata.org/V2/Northwind/Northwind.svc' AS northwind (TYPE odata);  -- OData v2

-- Query OData entities as regular tables (works for both versions)
SELECT UserName, FirstName, LastName 
FROM trippin.People 
WHERE Gender = 'Female';

SELECT CustomerID, CompanyName, ContactName
FROM northwind.Customers
WHERE Country = 'Germany';
```

### Direct OData Queries

```sql
-- Query entity sets directly (automatic version detection)
SELECT UserName, AddressInfo[1].City."Name" AS City
FROM odata_read('https://services.odata.org/TripPinRESTierService/People')
WHERE UserName = 'angelhuffman';

-- OData v2 service with automatic version handling
SELECT OrderID, CustomerID, OrderDate
FROM odata_read('https://services.odata.org/V2/Northwind/Northwind.svc/Orders')
WHERE OrderDate >= '1996-01-01';

-- Complex nested property access (works for both versions)
SELECT UserName, 
       Emails[1] AS PrimaryEmail,
       AddressInfo[1].Address."Address" AS FullAddress
FROM odata_read('https://services.odata.org/TripPinRESTierService/People')
LIMIT 5;
```

### OData Features

- **Universal Version Support**: Automatic OData v2 and v4 detection and handling
- **Automatic Pagination**: Handles large datasets seamlessly with version-appropriate pagination
- **Predicate Pushdown**: Filters are translated to OData $filter clauses
- **Column Selection**: Optimizes queries with $select clauses
- **Metadata Caching**: Improves performance with intelligent caching
- **Type Mapping**: Automatic DuckDB type conversion from OData metadata
- **Version-Specific Features**: Support for v2 `$inlinecount` and v4 `$count`
- **Error Recovery**: Graceful fallback between v2 and v4 parsing

## 🔍 Tracing & Monitoring

### Enable Tracing

```sql
-- Enable comprehensive tracing
SET erpl_trace_enabled = TRUE;
SET erpl_trace_level = 'TRACE';
SET erpl_trace_output = 'both'; -- console and file

-- Make HTTP requests to see trace output
SELECT * FROM http_get('https://httpbun.com/ip');
```

### Trace Configuration

| Setting | Description | Options |
|---------|-------------|---------|
| `erpl_trace_enabled` | Enable/disable tracing | `TRUE`/`FALSE` |
| `erpl_trace_level` | Trace verbosity | `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR` |
| `erpl_trace_output` | Output destination | `console`, `file`, `both` |
| `erpl_trace_file_path` | Trace file location | File path |
| `erpl_trace_max_file_size` | Max file size | Bytes (default: 10MB) |
| `erpl_trace_rotation` | Enable file rotation | `TRUE`/`FALSE` |

### Trace Information

Traces include:
- Request/response details
- Timing information
- Headers and authentication
- Error details
- Performance metrics

## 🔐 Authentication & Security

### DuckDB Secrets Integration

```sql
-- Create Basic Auth secret
CREATE SECRET (
    TYPE http_basic,
    USERNAME 'myuser',
    PASSWORD 'mypassword',
    SCOPE 'https://api.example.com/'
);

-- Create Bearer token secret
CREATE SECRET (
    TYPE http_bearer,
    TOKEN 'your-token-here',
    SCOPE 'https://api.example.com/'
);

-- Use secrets automatically
SELECT * FROM http_get('https://api.example.com/protected');
```

### Manual Authentication

```sql
-- Basic Auth
SELECT * FROM http_get('https://api.example.com/protected',
    auth:='username:password',
    auth_type:='BASIC');

-- Bearer Token
SELECT * FROM http_get('https://api.example.com/protected',
    auth:='your-token-here',
    auth_type:='BEARER');
```

## 📈 Telemetry

The extension includes optional usage analytics to help improve performance and features:

```sql
-- Disable telemetry if desired
SET erpl_telemetry_enabled = FALSE;

-- Customize API key
SET erpl_telemetry_key = 'your-posthog-key';
```

**Data Collected**: Extension version, DuckDB version, OS, architecture, function usage (no sensitive data)

## 🚀 Performance Features

### HTTP Caching

```sql
-- Automatic response caching (30 seconds default)
-- Subsequent identical requests use cached responses
SELECT * FROM http_get('https://api.example.com/data');
```

### OData Metadata Caching

```sql
-- Automatic metadata caching for OData services
-- Version detection and schema information cached for performance
ATTACH 'https://services.odata.org/V2/Northwind/Northwind.svc' AS northwind (TYPE odata);
-- Metadata cached automatically - subsequent queries are faster
```

### Retry Logic

- **Automatic Retries**: Configurable retry attempts with exponential backoff
- **Timeout Handling**: Configurable request timeouts
- **Connection Pooling**: Efficient connection reuse
- **Version Detection**: Robust OData version detection with fallback mechanisms

### Configuration

```sql
-- Set custom timeout (milliseconds)
SET erpl_http_timeout = 30000;

-- Configure retry behavior
SET erpl_http_retries = 5;
SET erpl_http_retry_wait_ms = 1000;

-- OData-specific settings
SET erpl_odata_metadata_cache_ttl = 3600;  -- Cache metadata for 1 hour
```

## 🔧 Advanced Usage

### Charset Conversion

Automatic encoding detection and conversion for various content types:
- UTF-8, ISO-8859-1, ISO-8859-15
- Windows-1252, Binary content
- Smart fallback handling

### Error Handling

```sql
-- Handle HTTP errors gracefully
SELECT 
    CASE 
        WHEN status >= 400 THEN 'Error: ' || content
        ELSE content::JSON->>'data'
    END as result
FROM http_get('https://api.example.com/data');

-- Handle OData version detection errors
SELECT * FROM odata_read('https://services.odata.org/V2/Northwind/Northwind.svc/Customers')
WHERE CustomerID = 'ALFKI';
-- Automatic fallback to v4 if v2 detection fails
```

### Batch Operations

```sql
-- Process multiple URLs
WITH urls AS (
    SELECT unnest(['https://api1.com', 'https://api2.com', 'https://api3.com']) as url
)
SELECT url, status, content 
FROM urls, http_get(urls.url);

-- Process multiple OData services
ATTACH 'https://services.odata.org/TripPinRESTierService' AS trippin (TYPE odata);
ATTACH 'https://services.odata.org/V2/Northwind/Northwind.svc' AS northwind (TYPE odata);

-- Cross-service analysis
SELECT 'TripPin' as source, UserName, FirstName, LastName FROM trippin.People LIMIT 5
UNION ALL
SELECT 'Northwind' as source, CustomerID, CompanyName, ContactName FROM northwind.Customers LIMIT 5;
```

## 🏢 Enterprise Features

### Production-Ready OData Support

- **Universal Compatibility**: Works with any OData v2 or v4 service without configuration
- **Enterprise Services**: Tested with SAP, Microsoft Dynamics, SharePoint, and other enterprise OData services
- **Large Dataset Handling**: Efficiently processes datasets with thousands of entities
- **Robust Error Recovery**: Graceful handling of network issues, malformed responses, and version conflicts
- **Performance Optimization**: Intelligent caching and connection pooling for high-throughput scenarios

### Enterprise Integration Examples

```sql
-- SAP Business Suite OData v2
ATTACH 'https://your-sap-system.com/sap/opu/odata/sap/ZGW_SAMPLE_BASIC_SRV/' AS sap (TYPE odata);

-- Microsoft Dynamics 365 OData v4
ATTACH 'https://your-org.crm.dynamics.com/api/data/v9.2/' AS dynamics (TYPE odata);

-- SharePoint Online OData v4
ATTACH 'https://your-tenant.sharepoint.com/sites/yoursite/_api/web/' AS sharepoint (TYPE odata);

-- Cross-platform data analysis
SELECT 
    'SAP' as source,
    CompanyCode,
    COUNT(*) as transaction_count
FROM sap.BusinessPartnerSet
WHERE Date >= '2024-01-01'
GROUP BY CompanyCode

UNION ALL

SELECT 
    'Dynamics' as source,
    name as company,
    COUNT(*) as record_count
FROM dynamics.accounts
WHERE statecode = 0
GROUP BY name;
```

## 📚 Examples

### API Data Integration

```sql
-- Fetch and analyze weather data
SELECT 
    content::JSON->>'location' as city,
    content::JSON->'current'->>'temp_c' as temperature,
    content::JSON->'current'->>'condition'->>'text' as weather
FROM http_get('https://api.weatherapi.com/v1/current.json?key=YOUR_KEY&q=London');
```

### OData Business Intelligence

```sql
-- Connect to SAP OData v2 service (automatic version detection)
ATTACH 'https://your-sap-system.com/sap/opu/odata/sap/ZGW_SAMPLE_BASIC_SRV/' AS sap (TYPE odata);

-- Connect to Microsoft Dynamics OData v4 service
ATTACH 'https://your-dynamics-instance.crm.dynamics.com/api/data/v9.2/' AS dynamics (TYPE odata);

-- Analyze business data from SAP (OData v2)
SELECT 
    CompanyCode,
    COUNT(*) as transaction_count,
    SUM(Amount) as total_amount
FROM sap.BusinessPartnerSet
WHERE Date >= '2024-01-01'
GROUP BY CompanyCode
ORDER BY total_amount DESC;

-- Analyze customer data from Dynamics (OData v4)
SELECT 
    name,
    emailaddress1,
    createdon,
    statecode
FROM dynamics.accounts
WHERE statecode = 0  -- Active accounts
ORDER BY createdon DESC;
```

### Web Scraping and Monitoring

```sql
-- Monitor website status
SELECT 
    url,
    status,
    CASE 
        WHEN status = 200 THEN 'OK'
        WHEN status >= 400 THEN 'Error'
        ELSE 'Warning'
    END as health
FROM (
    SELECT unnest([
        'https://example.com',
        'https://api.example.com',
        'https://docs.example.com'
    ]) as url
), http_head(url);
```

## 🧪 Testing

Run the comprehensive test suite:

```bash
# Build and test
make test

# Run specific test categories
make test-http
make test-odata
make test-tracing
```

## 📖 API Reference

### HTTP Function Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `headers` | MAP(VARCHAR, VARCHAR) | `{}` | Custom HTTP headers |
| `content_type` | VARCHAR | `application/json` | Request content type |
| `accept` | VARCHAR | `application/json` | Accept header |
| `auth` | VARCHAR | `NULL` | Authentication credentials |
| `auth_type` | ENUM | `BASIC` | Auth type (BASIC, BEARER) |
| `timeout` | BIGINT | `60000` | Request timeout (ms) |

### OData Function Parameters

| Function | Parameters | Description |
|----------|------------|-------------|
| `odata_read(url)` | `url` | Read OData entity set |
| `ATTACH ... TYPE odata` | `url` | Attach OData service as database |

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## 📄 License

This extension is licensed under the [Business Source License (BSL) Version 1.1](./LICENSE). See the [LICENSE](./LICENSE) file for full details.

## 🔗 Links

- **Documentation**: [https://docs.erpl.io](https://docs.erpl.io)
- **Issues**: [GitHub Issues](https://github.com/your-repo/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-repo/discussions)
- **Website**: [https://erpl.io](https://erpl.io)

---

**ERPL Web** - Transform your DuckDB into a web-enabled powerhouse! 🚀