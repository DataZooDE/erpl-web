# ODP (Operational Data Provisioning) OData Tutorial

## Table of Contents
1. [What is ODP OData?](#what-is-odp-odata)
2. [Quick Start Guide](#quick-start-guide)
3. [Understanding the Data Schema](#understanding-the-data-schema)
4. [Delta Replication & Change Tracking](#delta-replication--change-tracking)
5. [Subscription Management](#subscription-management)
6. [Monitoring & Auditing](#monitoring--auditing)
7. [Advanced Usage](#advanced-usage)
8. [Troubleshooting](#troubleshooting)

---

## What is ODP OData?

**ODP (Operational Data Provisioning)** is SAP's framework for extracting data from various SAP systems with support for **delta replication** (incremental updates). **OData (Open Data Protocol)** is a web-based protocol that allows you to query and access this data. Since SAP notified customers in SAP OSS Note 3255746 that use of SAP RFCs for extraction of ABAP data from sources external to SAP (On premise and Cloud) is banned for customers and third-party tools, we are left with using ODP OData for extraction. 

### Why Use ODP OData?

- **🔄 Delta Replication**: Get only changed data since your last extraction
- **📊 Real-time Analytics**: Keep your data warehouse up-to-date automatically
- **🔒 Security**: Secure authentication via DuckDB secrets
- **📈 Scalability**: Handle large datasets with automatic chunking

### Key Concepts for Data Scientists

| Term | Description | Example |
|------|-------------|---------|
| **Entity Set** | A collection of data (like a table) | `FactsOf0D_NW_C01` (sales data) |
| **Delta Token** | A bookmark for incremental updates | `'D20250914154609_000019000'` |
| **Subscription** | A persistent connection for delta updates | Automatically managed |
| **OData Service** | The API endpoint providing the data | `/sap/opu/odata/sap/Z_ODP_BW_1_SRV/` |

---

## Quick Start Guide

### Step 1: Set Up Authentication

First, create a DuckDB secret for your SAP system authentication:

```sql
-- Create a secret for HTTP Basic authentication
CREATE SECRET sap_system (
    TYPE http_basic,
    username 'your_username',
    password 'your_password'
);
```

### Step 2: Discover Available Data

Find what data is available in your SAP system:

```sql
-- Load the extension
LOAD erpl_web;

-- Discover ODP OData services
SELECT 
    service_name,
    entity_set_name,
    full_entity_set_url,
    description
FROM odp_odata_show('https://your-sap-server:port', secret='sap_system')
WHERE entity_set_name LIKE '%SALES%'  -- Filter for sales data
LIMIT 10;
```

**Example Output:**
```
┌─────────────────────┬──────────────────┬────────────────────────────────┬─────────────────────┐
│    service_name     │ entity_set_name  │      full_entity_set_url       │    description      │
├─────────────────────┼──────────────────┼────────────────────────────────┼─────────────────────┤
│ Z_ODP_BW_1_SRV      │ FactsOf0D_NW_C01 │ https://server/sap/opu/odata...│ Sales Facts Data    │
│ Z_ODP_FINANCE_SRV   │ GLAccountData    │ https://server/sap/opu/odata...│ GL Account Master   │
└─────────────────────┴──────────────────┴────────────────────────────────┴─────────────────────┘
```

### Step 3: Complete ODP Subscription Workflow

Now let's walk through the complete lifecycle of an ODP subscription:

#### 3.1 Create Subscription & Initial Load

The first time you call `odp_odata_read()`, it automatically:
- Creates a new ODP subscription
- Performs an initial full load
- Stores a delta token for future incremental updates

```sql
-- Create subscription and perform initial load
-- This will load ALL data and create a subscription
SELECT COUNT(*) as total_records
FROM odp_odata_read(
    'https://your-sap-server:port/sap/opu/odata/sap/Z_ODP_BW_1_SRV/FactsOf0D_NW_C01',
    secret='sap_system'
);
```

**Example Output:**
```
┌───────────────┐
│ total_records │
├───────────────┤
│        15420  │
└───────────────┘
```

#### 3.2 Check Your New Subscription

After the initial load, verify your subscription was created:

```sql
-- List all active subscriptions
SELECT 
    subscription_id,
    entity_set_name,
    subscription_status,
    created_at,
    delta_token IS NOT NULL as has_delta_token
FROM odp_odata_list_subscriptions();
```

**Example Output:**
```
┌─────────────────────────────────────┬──────────────────┬─────────────────────┬─────────────────────┬─────────────────┐
│           subscription_id           │ entity_set_name  │ subscription_status │     created_at      │ has_delta_token │
├─────────────────────────────────────┼──────────────────┼─────────────────────┼─────────────────────┼─────────────────┤
│ 20250914_174953_localhost_50000_... │ FactsOf0D_NW_C01 │ DELTA_FETCH         │ 2025-09-14 17:49:53 │ true            │
└─────────────────────────────────────┴──────────────────┴─────────────────────┴─────────────────────┴─────────────────┘
```

#### 3.3 Fetch Delta Changes

Now when you run the same query again, it will automatically fetch only changes:

```sql
-- Fetch only changes since last extraction (delta load)
-- This uses the stored delta token automatically
SELECT 
    RECORD_MODE,
    COUNT(*) as change_count
FROM odp_odata_read(
    'http://your-sap-server:port/sap/opu/odata/sap/Z_ODP_BW_1_SRV/FactsOf0D_NW_C01',
    secret='sap_system'
)
GROUP BY RECORD_MODE;
```

**Example Output (if there were changes):**
```
┌─────────────┬──────────────┐
│ RECORD_MODE │ change_count │
├─────────────┼──────────────┤
│             │          45  │  -- Updated records
│ N           │          12  │  -- New records  
│ D           │           3  │  -- Deleted records
└─────────────┴──────────────┘
```

**Example Output (if no changes):**
```
┌─────────────┬──────────────┐
│ RECORD_MODE │ change_count │
├─────────────┼──────────────┤
│             │           0  │  -- No changes since last extraction
└─────────────┴──────────────┘
```

## Working with ODP Results: Insert, Update, Delete

### Understanding RECORD_MODE Values

The `RECORD_MODE` column tells you what type of change each record represents:

| RECORD_MODE | Meaning | Action Needed |
|-------------|---------|---------------|
| `''` (empty) | **Updated Record** | UPDATE in your target system |
| `'N'` | **New Record** | INSERT in your target system |
| `'D'` | **Deleted Record** | DELETE from your target system |

### Practical Example: Processing Changes

Let's say you have a customer master data entity and want to process changes:

```sql
-- Get all changes and see what needs to be done
SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    RECORD_MODE,
    CASE 
        WHEN RECORD_MODE = '' THEN 'UPDATE_CUSTOMER'
        WHEN RECORD_MODE = 'N' THEN 'INSERT_CUSTOMER'  
        WHEN RECORD_MODE = 'D' THEN 'DELETE_CUSTOMER'
        ELSE 'UNKNOWN_ACTION'
    END as required_action
FROM odp_odata_read(
    'http://your-sap-server:port/sap/opu/odata/sap/CUSTOMER_SRV/CustomerData',
    secret='sap_system'
)
WHERE RECORD_MODE IS NOT NULL  -- Only show actual changes
ORDER BY CUSTOMER_ID;
```

**Example Output:**
```
┌─────────────┬─────────────────────┬─────────────┬─────────────────┐
│ CUSTOMER_ID │   CUSTOMER_NAME     │ RECORD_MODE │ required_action │
├─────────────┼─────────────────────┼─────────────┼─────────────────┤
│ CUST001     │ Acme Corporation    │             │ UPDATE_CUSTOMER │
│ CUST002     │ Beta Industries     │ N           │ INSERT_CUSTOMER │
│ CUST003     │ Gamma Solutions     │ D           │ DELETE_CUSTOMER │
└─────────────┴─────────────────────┴─────────────┴─────────────────┘
```

### Applying Changes to Your Data Warehouse

Here's how to apply these changes to a target table:

```sql
-- Create your target table (if it doesn't exist)
CREATE TABLE IF NOT EXISTS warehouse.customers AS
SELECT CUSTOMER_ID, CUSTOMER_NAME, LAST_UPDATED
FROM odp_odata_read('your_customer_entity_url', secret='sap_system')
WHERE 1=0; -- Create empty table with correct schema

-- Step 1: Handle INSERTS (new records)
INSERT INTO warehouse.customers
SELECT CUSTOMER_ID, CUSTOMER_NAME, CURRENT_TIMESTAMP as LAST_UPDATED
FROM odp_odata_read('your_customer_entity_url', secret='sap_system')
WHERE RECORD_MODE = 'N';

-- Step 2: Handle UPDATES (changed records)
UPDATE warehouse.customers 
SET 
    CUSTOMER_NAME = src.CUSTOMER_NAME,
    LAST_UPDATED = CURRENT_TIMESTAMP
FROM (
    SELECT CUSTOMER_ID, CUSTOMER_NAME
    FROM odp_odata_read('your_customer_entity_url', secret='sap_system')
    WHERE RECORD_MODE = ''
) src
WHERE warehouse.customers.CUSTOMER_ID = src.CUSTOMER_ID;

-- Step 3: Handle DELETES (removed records)
DELETE FROM warehouse.customers
WHERE CUSTOMER_ID IN (
    SELECT CUSTOMER_ID
    FROM odp_odata_read('your_customer_entity_url', secret='sap_system')
    WHERE RECORD_MODE = 'D'
);
```

### Advanced: Single Query for All Changes

You can also handle all changes in a single MERGE-like operation:

```sql
-- Modern approach: Use UPSERT for inserts and updates
INSERT INTO warehouse.customers
SELECT CUSTOMER_ID, CUSTOMER_NAME, CURRENT_TIMESTAMP as LAST_UPDATED
FROM odp_odata_read('your_customer_entity_url', secret='sap_system')
WHERE RECORD_MODE IN ('', 'N')  -- Both updates and inserts
ON CONFLICT (CUSTOMER_ID) DO UPDATE SET
    CUSTOMER_NAME = EXCLUDED.CUSTOMER_NAME,
    LAST_UPDATED = EXCLUDED.LAST_UPDATED;

-- Then handle deletes separately
DELETE FROM warehouse.customers
WHERE CUSTOMER_ID IN (
    SELECT CUSTOMER_ID
    FROM odp_odata_read('your_customer_entity_url', secret='sap_system')
    WHERE RECORD_MODE = 'D'
);
```

## Subscription Management

### List All Subscriptions

Get a complete overview of your ODP subscriptions:

```sql
-- Comprehensive subscription overview
SELECT 
    subscription_id,
    entity_set_name,
    subscription_status,
    created_at,
    last_updated,
    CASE 
        WHEN delta_token IS NOT NULL THEN 'Ready for Delta'
        ELSE 'Initial Load Only'
    END as replication_status,
    -- Extract entity name from subscription_id for readability
    REGEXP_EXTRACT(subscription_id, '.*_([^_]+)$', 1) as short_name
FROM odp_odata_list_subscriptions()
ORDER BY last_updated DESC;
```

**Example Output:**
```
┌─────────────────────────────────────┬──────────────────┬─────────────────────┬─────────────────────┬──────────────────┬────────────┐
│           subscription_id           │ entity_set_name  │ subscription_status │     created_at      │ replication_status│ short_name │
├─────────────────────────────────────┼──────────────────┼─────────────────────┼─────────────────────┼──────────────────┼────────────┤
│ 20250914_174953_localhost_50000_... │ FactsOf0D_NW_C01 │ DELTA_FETCH         │ 2025-09-14 17:49:53 │ Ready for Delta  │ NW_C01     │
│ 20250914_163022_localhost_50000_... │ CustomerMaster   │ INITIAL_LOAD        │ 2025-09-14 16:30:22 │ Initial Load Only│ Master     │
└─────────────────────────────────────┴──────────────────┴─────────────────────┴─────────────────────┴──────────────────┴────────────┘
```

### Remove a Subscription

When you no longer need a subscription, you can remove it:

```sql
-- Option 1: Remove subscription and all local data
PRAGMA odp_odata_remove_subscription('20250914_174953_localhost_50000_...', false);

-- Option 2: Remove subscription but keep local audit history
PRAGMA odp_odata_remove_subscription('20250914_174953_localhost_50000_...', true);
```

**Parameters Explained:**
- **First parameter**: The exact `subscription_id` from `odp_odata_list_subscriptions()`
- **Second parameter**: 
  - `false` = Remove everything (SAP subscription + local data)
  - `true` = Remove SAP subscription but keep local audit logs

### Force Full Reload

Sometimes you need to reset a subscription and start fresh:

```sql
-- Method 1: Remove old subscription and create new one
PRAGMA odp_odata_remove_subscription('old_subscription_id', false);

-- Then create fresh subscription with full load
SELECT COUNT(*) FROM odp_odata_read('your_entity_url', secret='sap_system');

-- Method 2: Force full reload while keeping subscription
SELECT COUNT(*) FROM odp_odata_read(
    'your_entity_url', 
    secret='sap_system',
    force_full_load=true  -- Ignores delta token, loads everything
);
```

---

## Understanding the Data Schema

### Typical ODP Data Structure

ODP data typically includes both **business data** and **metadata columns**:

```sql
-- Example: Sales facts data structure
SELECT column_name, data_type, description
FROM (
    SELECT * FROM odp_odata_read('...') LIMIT 0
) 
DESCRIBE;
```

**Common Column Types:**

| Column Pattern | Purpose | Example |
|----------------|---------|---------|
| `*_KEY` | Primary keys | `CUSTOMER_KEY`, `PRODUCT_KEY` |
| `*_DATE` | Date dimensions | `ORDER_DATE`, `DELIVERY_DATE` |
| `*_AMOUNT` | Measures/Facts | `SALES_AMOUNT`, `DISCOUNT_AMOUNT` |
| `RECORD_MODE` | Change type | `''` (unchanged), `'D'` (deleted), `'N'` (new) |
| `REQUEST_TSN` | Technical sequence | Used for ordering changes |

### Data Types Mapping

| SAP/OData Type | DuckDB Type | Notes |
|----------------|-------------|-------|
| `Edm.String` | `VARCHAR` | Text data |
| `Edm.Decimal` | `DECIMAL` | Precise numbers |
| `Edm.DateTime` | `TIMESTAMP` | Date and time |
| `Edm.Int32` | `INTEGER` | Whole numbers |
| `Edm.Boolean` | `BOOLEAN` | True/false values |

### Understanding Change Tracking

When delta replication is enabled, look for these special columns:

```sql
-- Analyze change patterns in your data
SELECT 
    RECORD_MODE,
    COUNT(*) as record_count,
    MIN(REQUEST_TSN) as first_change,
    MAX(REQUEST_TSN) as last_change
FROM odp_odata_read('your_entity_url', secret='sap_system')
GROUP BY RECORD_MODE;
```

---

## Delta Replication & Change Tracking

### How Delta Replication Works

Delta replication is the core feature that makes ODP powerful for real-time analytics:

1. **Initial Load**: First extraction gets all data and establishes a subscription
2. **Delta Token**: SAP provides a "bookmark" (like `'D20250914154609_000019000'`) for the next extraction
3. **Incremental Updates**: Subsequent extractions get only changes since the last token
4. **Automatic Management**: DuckDB handles token storage and updates transparently

### Delta Replication Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ First Call  │───▶│ Full Load   │───▶│ Store Token │───▶│ Ready for   │
│ odp_odata_  │    │ (All Data)  │    │ 'D20250...' │    │ Delta Loads │
│ read()      │    │             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                │
                                                                ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Update      │◀───│ Delta Load  │◀───│ Second Call │◀───│ Automatic   │
│ Token       │    │ (Changes    │    │ odp_odata_  │    │ Delta Mode  │
│             │    │ Only)       │    │ read()      │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### Practical Delta Replication Example

Let's walk through a complete example with a sales data entity:

#### Day 1: Initial Load
```sql
-- First extraction: Creates subscription and loads all historical data
SELECT 
    COUNT(*) as total_records,
    MIN(ORDER_DATE) as earliest_order,
    MAX(ORDER_DATE) as latest_order
FROM odp_odata_read(
    'http://sap-server/sap/opu/odata/sap/SALES_SRV/SalesOrders',
    secret='sap_system'
);
```

**Result:**
```
┌───────────────┬───────────────┬──────────────┐
│ total_records │ earliest_order│ latest_order │
├───────────────┼───────────────┼──────────────┤
│        45,230 │  2020-01-01   │  2025-09-14  │
└───────────────┴───────────────┴──────────────┘
```

#### Day 2: Delta Load (Next Day)
```sql
-- Same query, but now gets only changes since yesterday
SELECT 
    RECORD_MODE,
    COUNT(*) as change_count,
    MIN(ORDER_DATE) as earliest_change,
    MAX(ORDER_DATE) as latest_change
FROM odp_odata_read(
    'http://sap-server/sap/opu/odata/sap/SALES_SRV/SalesOrders',
    secret='sap_system'
)
GROUP BY RECORD_MODE;
```

**Result:**
```
┌─────────────┬──────────────┬─────────────────┬────────────────┐
│ RECORD_MODE │ change_count │ earliest_change │ latest_change  │
├─────────────┼──────────────┼─────────────────┼────────────────┤
│             │          127 │    2025-09-13   │   2025-09-15   │  -- Updated orders
│ N           │           89 │    2025-09-15   │   2025-09-15   │  -- New orders
│ D           │            3 │    2025-09-14   │   2025-09-14   │  -- Cancelled orders
└─────────────┴──────────────┴─────────────────┴────────────────┘
```

#### Continuous Updates
Every subsequent call automatically fetches only the changes since the last extraction!

### Manual Delta Control

For advanced use cases, you can control delta replication manually:

```sql
-- Force a full reload (ignores existing delta token)
SELECT * FROM odp_odata_read(
    'your_entity_url',
    secret='sap_system',
    force_full_load=true
);

-- Import an existing delta token from another system
SELECT * FROM odp_odata_read(
    'your_entity_url',
    secret='sap_system',
    import_delta_token='D20250914154609_000019000'
);

-- Optimize for large datasets with custom page size
SELECT * FROM odp_odata_read(
    'your_entity_url',
    secret='sap_system',
    max_page_size=5000
);
```

### Best Practices for Delta Replication

1. **Regular Extractions**: Run extractions regularly to keep delta tokens fresh
2. **Monitor Changes**: Use audit tables to track extraction patterns
3. **Handle Deletions**: Pay attention to `RECORD_MODE='D'` for deleted records
4. **Sequence Ordering**: Use `REQUEST_TSN` to order changes correctly

---

## Subscription Management

### Understanding Subscriptions

Each ODP entity you extract creates a **subscription** - a persistent connection that tracks:
- Delta tokens for incremental updates
- Extraction history and performance
- Error states and recovery information

### List Active Subscriptions

```sql
-- View all your active ODP subscriptions
SELECT 
    subscription_id,
    entity_set_name,
    subscription_status,
    delta_token,
    created_at,
    last_updated
FROM odp_odata_list_subscriptions()
ORDER BY last_updated DESC;
```

**Example Output:**
```
┌─────────────────────────────────────┬──────────────────┬─────────────────────┬──────────────────────────┐
│           subscription_id           │ entity_set_name  │ subscription_status │       delta_token        │
├─────────────────────────────────────┼──────────────────┼─────────────────────┼──────────────────────────┤
│ 20250914_174953_localhost_50000_... │ FactsOf0D_NW_C01 │ DELTA_FETCH         │ D20250914154609_000019000│
│ 20250914_163022_localhost_50000_... │ GLAccountData    │ INITIAL_LOAD        │ NULL                     │
└─────────────────────────────────────┴──────────────────┴─────────────────────┴──────────────────────────┘
```

### Subscription Status Meanings

| Status | Description | Next Action |
|--------|-------------|-------------|
| `INITIAL_LOAD` | First extraction in progress | Wait for completion |
| `DELTA_FETCH` | Ready for incremental updates | Normal operation |
| `ERROR_STATE` | Extraction failed | Check audit logs |

### Remove Subscriptions

```sql
-- Remove a subscription (both SAP-side and local data)
PRAGMA odp_odata_remove_subscription('subscription_id_here', false);

-- Remove subscription but keep local audit data
PRAGMA odp_odata_remove_subscription('subscription_id_here', true);
```

---

## Monitoring & Auditing

### Audit Schema Overview

The system automatically tracks detailed metrics in the `erpl_web.odp_subscription_audit` table:

```sql
-- View recent extraction performance
SELECT 
    subscription_id,
    operation_type,
    request_timestamp,
    response_timestamp,
    http_status_code,
    rows_fetched,
    package_size_bytes,
    duration_ms,
    error_message
FROM erpl_web.odp_subscription_audit
ORDER BY request_timestamp DESC
LIMIT 10;
```

### Key Performance Metrics

#### Extraction Performance Analysis

```sql
-- Analyze extraction performance over time
SELECT 
    DATE_TRUNC('hour', request_timestamp) as hour,
    COUNT(*) as extractions,
    AVG(rows_fetched) as avg_rows,
    AVG(duration_ms) as avg_duration_ms,
    AVG(package_size_bytes) as avg_size_bytes
FROM erpl_web.odp_subscription_audit
WHERE request_timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour;
```

#### Delta vs Full Load Analysis

```sql
-- Compare full loads vs delta extractions
SELECT 
    CASE 
        WHEN delta_token_before IS NULL THEN 'Full Load'
        ELSE 'Delta Load'
    END as load_type,
    COUNT(*) as extraction_count,
    AVG(rows_fetched) as avg_rows,
    AVG(duration_ms) as avg_duration,
    SUM(package_size_bytes) as total_bytes
FROM erpl_web.odp_subscription_audit
GROUP BY load_type;
```

#### Error Analysis

```sql
-- Identify extraction errors and patterns
SELECT 
    subscription_id,
    http_status_code,
    error_message,
    COUNT(*) as error_count,
    MAX(request_timestamp) as last_error
FROM erpl_web.odp_subscription_audit
WHERE http_status_code != 200 OR error_message IS NOT NULL
GROUP BY subscription_id, http_status_code, error_message
ORDER BY error_count DESC;
```

### Setting Up Monitoring Dashboards

#### Daily Extraction Summary

```sql
-- Create a daily monitoring view
CREATE VIEW daily_odp_summary AS
SELECT 
    DATE(request_timestamp) as extraction_date,
    subscription_id,
    COUNT(*) as total_extractions,
    SUM(rows_fetched) as total_rows,
    AVG(duration_ms) as avg_duration,
    MAX(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) as has_errors
FROM erpl_web.odp_subscription_audit
GROUP BY DATE(request_timestamp), subscription_id;

-- Query the summary
SELECT * FROM daily_odp_summary 
WHERE extraction_date >= CURRENT_DATE - 7
ORDER BY extraction_date DESC, subscription_id;
```

---

## Advanced Usage

### Batch Processing Multiple Entities

```sql
-- Extract multiple entities in a batch
WITH entity_urls AS (
    SELECT full_entity_set_url, entity_set_name
    FROM sap_odp_odata_show('http://your-sap-server:port', secret='sap_system')
    WHERE entity_set_name IN ('FactsOf0D_NW_C01', 'GLAccountData', 'CustomerMaster')
)
SELECT 
    e.entity_set_name,
    COUNT(*) as record_count,
    MAX(d.last_updated_timestamp) as latest_update
FROM entity_urls e
CROSS JOIN LATERAL (
    SELECT * FROM odp_odata_read(e.full_entity_set_url, secret='sap_system')
) d
GROUP BY e.entity_set_name;
```

### Data Quality Checks

```sql
-- Check for data quality issues in delta loads
SELECT 
    subscription_id,
    COUNT(*) as total_records,
    COUNT(DISTINCT RECORD_MODE) as change_types,
    SUM(CASE WHEN RECORD_MODE = 'D' THEN 1 ELSE 0 END) as deletions,
    SUM(CASE WHEN RECORD_MODE = 'N' THEN 1 ELSE 0 END) as new_records,
    SUM(CASE WHEN RECORD_MODE = '' THEN 1 ELSE 0 END) as updates
FROM (
    SELECT * FROM odp_odata_read('your_entity_url', secret='sap_system')
) data
JOIN erpl_web.odp_subscriptions s ON s.service_url LIKE '%your_entity%'
GROUP BY subscription_id;
```

### Performance Optimization

#### Optimal Page Sizes

```sql
-- Test different page sizes for performance
SELECT 
    page_size,
    AVG(duration_ms) as avg_duration,
    AVG(rows_fetched) as avg_rows_per_request
FROM (
    -- Test with different page sizes
    SELECT 1000 as page_size, duration_ms, rows_fetched 
    FROM erpl_web.odp_subscription_audit 
    WHERE request_url LIKE '%$top=1000%'
    
    UNION ALL
    
    SELECT 5000 as page_size, duration_ms, rows_fetched 
    FROM erpl_web.odp_subscription_audit 
    WHERE request_url LIKE '%$top=5000%'
) performance_data
GROUP BY page_size;
```

### Integration with Data Pipelines

#### Incremental Data Warehouse Updates

```sql
-- Example: Incremental update pattern for a data warehouse
CREATE TABLE IF NOT EXISTS warehouse.sales_facts AS
SELECT * FROM odp_odata_read('your_sales_entity_url', secret='sap_system')
WHERE 1=0; -- Create empty table with correct schema

-- Incremental update procedure
INSERT INTO warehouse.sales_facts
SELECT * FROM odp_odata_read('your_sales_entity_url', secret='sap_system')
WHERE RECORD_MODE IN ('', 'N'); -- Only new and updated records

-- Handle deletions
DELETE FROM warehouse.sales_facts
WHERE (primary_key_column) IN (
    SELECT primary_key_column 
    FROM odp_odata_read('your_sales_entity_url', secret='sap_system')
    WHERE RECORD_MODE = 'D'
);
```

---

## Troubleshooting

### Common Issues and Solutions

#### 1. Authentication Errors

**Error**: `HTTP 401 Unauthorized`

**Solution**:
```sql
-- Verify your secret is correctly configured
SELECT * FROM duckdb_secrets() WHERE name = 'sap_system';

-- Recreate the secret if needed
DROP SECRET sap_system;
CREATE SECRET sap_system (
    TYPE http_basic,
    username 'correct_username',
    password 'correct_password'
);
```

#### 2. Delta Token Expired

**Error**: `Delta token has expired or is invalid`

**Solution**:
```sql
-- Force a full reload to get a fresh delta token
SELECT * FROM odp_odata_read(
    'your_entity_url',
    secret='sap_system',
    force_full_load=true
) LIMIT 1;
```

#### 3. Large Dataset Timeouts

**Error**: `Request timeout` or `Memory allocation failed`

**Solution**:
```sql
-- Use smaller page sizes for large datasets
SELECT * FROM odp_odata_read(
    'your_entity_url',
    secret='sap_system',
    max_page_size=1000  -- Reduce from default 10000
);
```

#### 4. Service Discovery Issues

**Error**: `No OData services found`

**Solutions**:
- Verify the SAP system URL is correct
- Check that ODP OData services are deployed in SAP
- Ensure your user has the necessary SAP authorizations

#### 5. Subscription Management Issues

**Problem**: Subscriptions not persisting between sessions

**Explanation**: Each DuckDB session creates its own in-memory database. For persistent subscriptions, use a file-based database:

```sql
-- Use a persistent database file
.open my_database.duckdb
LOAD erpl_web;
-- Now subscriptions will persist
```

### Debugging with Trace Logs

Enable detailed tracing to diagnose issues:

```sql
-- Enable tracing
SET erpl_trace_enabled = true;

-- Run your extraction
SELECT * FROM odp_odata_read('your_entity_url', secret='sap_system') LIMIT 1;

-- Check the trace log file
-- Logs are written to: erpl_web_trace.log
```

### Performance Troubleshooting

#### Slow Extractions

1. **Check network latency** to your SAP system
2. **Reduce page size** for better memory usage
3. **Monitor SAP system load** during extractions
4. **Use delta extractions** instead of full loads when possible

#### Memory Issues

```sql
-- Monitor memory usage in audit logs
SELECT 
    subscription_id,
    AVG(package_size_bytes) as avg_package_size,
    MAX(package_size_bytes) as max_package_size,
    COUNT(*) as extraction_count
FROM erpl_web.odp_subscription_audit
GROUP BY subscription_id
ORDER BY max_package_size DESC;
```

### Getting Help

1. **Check audit logs** for detailed error information
2. **Enable tracing** for debugging
3. **Verify SAP system connectivity** and permissions
4. **Review subscription status** for state information
5. **Monitor performance metrics** for optimization opportunities

---

## Summary

You now have a complete understanding of ODP OData functionality in DuckDB! Here's what you've learned:

✅ **Basic Usage**: Discover and extract SAP data with simple SQL commands  
✅ **Delta Replication**: Automatic incremental updates for efficient data synchronization  
✅ **Subscription Management**: Monitor and control your data extraction subscriptions  
✅ **Performance Monitoring**: Track extraction performance and optimize for your use case  
✅ **Troubleshooting**: Diagnose and resolve common issues  

### Next Steps

1. **Start Small**: Begin with a single entity to understand the data structure
2. **Monitor Performance**: Use audit tables to optimize your extractions
3. **Automate**: Set up regular delta extractions for real-time analytics
4. **Scale Up**: Gradually add more entities as you become comfortable with the system

## Quick Reference: Complete ODP Workflow

Here's a complete workflow summary for easy reference:

### 1. Setup (One-time)
```sql
-- Create authentication secret
CREATE SECRET sap_system (TYPE http_basic, username 'user', password 'pass');
LOAD erpl_web;
```

### 2. Discover Data
```sql
-- Find available entities
SELECT entity_set_name, full_entity_set_url 
FROM sap_odp_odata_show('http://sap-server:port', secret='sap_system');
```

### 3. Create Subscription & Initial Load
```sql
-- First call: Creates subscription + full load
SELECT COUNT(*) FROM odp_odata_read('full_entity_set_url', secret='sap_system');
```

### 4. Check Subscription
```sql
-- Verify subscription was created
SELECT subscription_id, entity_set_name, subscription_status 
FROM odp_odata_list_subscriptions();
```

### 5. Fetch Delta Changes
```sql
-- Subsequent calls: Automatic delta loads
SELECT RECORD_MODE, COUNT(*) 
FROM odp_odata_read('full_entity_set_url', secret='sap_system')
GROUP BY RECORD_MODE;
```

### 6. Process Changes
```sql
-- Handle different change types
-- RECORD_MODE = ''  → UPDATE existing records
-- RECORD_MODE = 'N' → INSERT new records  
-- RECORD_MODE = 'D' → DELETE removed records
```

### 7. Monitor & Manage
```sql
-- View performance metrics
SELECT * FROM erpl_web.odp_subscription_audit ORDER BY request_timestamp DESC;

-- Remove subscription when done
PRAGMA odp_odata_remove_subscription('subscription_id', false);
```

**Happy Data Extracting! 🚀📊**
