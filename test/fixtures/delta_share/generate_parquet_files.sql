-- Generate 5 mock Parquet files for Phase 8 parallel testing
-- Each file contains 100 rows with deterministic data
-- Total: 500 rows of test data

-- File 1: parquet_001.parquet (rows 0-99)
COPY (
    SELECT
        100 + row_number() OVER () - 1 as id,
        'product_' || (100 + row_number() OVER () - 1) as name,
        CAST((100 + row_number() OVER () - 1) * 1.5 AS DOUBLE) as value
    FROM (
        SELECT * FROM generate_series(1, 100) as t(x)
    )
)
TO '/tmp/test_data/parquet_001.parquet' (FORMAT 'PARQUET');

-- File 2: parquet_002.parquet (rows 100-199)
COPY (
    SELECT
        200 + row_number() OVER () - 1 as id,
        'product_' || (200 + row_number() OVER () - 1) as name,
        CAST((200 + row_number() OVER () - 1) * 1.5 AS DOUBLE) as value
    FROM (
        SELECT * FROM generate_series(1, 100) as t(x)
    )
)
TO '/tmp/test_data/parquet_002.parquet' (FORMAT 'PARQUET');

-- File 3: parquet_003.parquet (rows 200-299)
COPY (
    SELECT
        300 + row_number() OVER () - 1 as id,
        'product_' || (300 + row_number() OVER () - 1) as name,
        CAST((300 + row_number() OVER () - 1) * 1.5 AS DOUBLE) as value
    FROM (
        SELECT * FROM generate_series(1, 100) as t(x)
    )
)
TO '/tmp/test_data/parquet_003.parquet' (FORMAT 'PARQUET');

-- File 4: parquet_004.parquet (rows 300-399)
COPY (
    SELECT
        400 + row_number() OVER () - 1 as id,
        'product_' || (400 + row_number() OVER () - 1) as name,
        CAST((400 + row_number() OVER () - 1) * 1.5 AS DOUBLE) as value
    FROM (
        SELECT * FROM generate_series(1, 100) as t(x)
    )
)
TO '/tmp/test_data/parquet_004.parquet' (FORMAT 'PARQUET');

-- File 5: parquet_005.parquet (rows 400-499)
COPY (
    SELECT
        500 + row_number() OVER () - 1 as id,
        'product_' || (500 + row_number() OVER () - 1) as name,
        CAST((500 + row_number() OVER () - 1) * 1.5 AS DOUBLE) as value
    FROM (
        SELECT * FROM generate_series(1, 100) as t(x)
    )
)
TO '/tmp/test_data/parquet_005.parquet' (FORMAT 'PARQUET');
