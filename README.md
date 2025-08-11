<a name="top"></a>

# ERPL Web

**ERPL Web** is a powerful DuckDB extension that transforms your database into a web-enabled powerhouse. With seamless HTTP integration, OData support, comprehensive tracing, and enterprise-grade features, you can now interact with web services directly from SQL queries.

## ✨ Key Features

- **🌐 HTTP Functions**: Make HTTP requests (GET, POST, PUT, PATCH, DELETE, HEAD) directly from SQL
- **📊 OData Integration**: Connect to OData services as if they were local database tables
- **🔍 Advanced Tracing**: Comprehensive request/response monitoring and debugging
- **🔐 Authentication**: Built-in support for Basic Auth, Bearer tokens, and DuckDB secrets
- **📈 Telemetry**: Optional usage analytics to improve the extension
- **🚀 Performance**: Intelligent caching, retry logic, and connection pooling
- **🔄 Charset Support**: Automatic encoding detection and conversion

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

### Connect to OData Services

```sql
-- Attach an OData service as a database
ATTACH 'https://services.odata.org/TripPinRESTierService' AS trippin (TYPE odata);

-- Query OData entities as regular tables
SELECT UserName, FirstName, LastName 
FROM trippin.People 
WHERE Gender = 'Female';
```

### Direct OData Queries

```sql
-- Query entity sets directly
SELECT UserName, AddressInfo[1].City."Name" AS City
FROM odata_read('https://services.odata.org/TripPinRESTierService/People')
WHERE UserName = 'angelhuffman';

-- Complex nested property access
SELECT UserName, 
       Emails[1] AS PrimaryEmail,
       AddressInfo[1].Address."Address" AS FullAddress
FROM odata_read('https://services.odata.org/TripPinRESTierService/People')
LIMIT 5;
```

### OData Features

- **Automatic Pagination**: Handles large datasets seamlessly
- **Predicate Pushdown**: Filters are translated to OData $filter clauses
- **Column Selection**: Optimizes queries with $select clauses
- **Metadata Caching**: Improves performance with intelligent caching
- **Type Mapping**: Automatic DuckDB type conversion from OData metadata

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

### Retry Logic

- **Automatic Retries**: Configurable retry attempts with exponential backoff
- **Timeout Handling**: Configurable request timeouts
- **Connection Pooling**: Efficient connection reuse

### Configuration

```sql
-- Set custom timeout (milliseconds)
SET erpl_http_timeout = 30000;

-- Configure retry behavior
SET erpl_http_retries = 5;
SET erpl_http_retry_wait_ms = 1000;
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
```

### Batch Operations

```sql
-- Process multiple URLs
WITH urls AS (
    SELECT unnest(['https://api1.com', 'https://api2.com', 'https://api3.com']) as url
)
SELECT url, status, content 
FROM urls, http_get(urls.url);
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
-- Connect to SAP OData service
ATTACH 'https://your-sap-system.com/sap/opu/odata/sap/ZGW_SAMPLE_BASIC_SRV/' AS sap (TYPE odata);

-- Analyze business data
SELECT 
    CompanyCode,
    COUNT(*) as transaction_count,
    SUM(Amount) as total_amount
FROM sap.BusinessPartnerSet
WHERE Date >= '2024-01-01'
GROUP BY CompanyCode
ORDER BY total_amount DESC;
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