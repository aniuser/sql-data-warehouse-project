-- =============================================================
-- Script   : proc_load_bronze.sql
-- Layer    : Bronze
-- Purpose  : Loads raw CSV source data into the Bronze schema.
--            Performs a full reload on every execution:
--            TRUNCATE each table then COPY from CSV file.
-- WARNING  : All existing Bronze data is deleted on each run.
-- Mac Note : CSV files must live in:
--            /Library/PostgreSQL/18/data/imports/
--            Copy them there with:
--            sudo cp /path/to/*.csv /Library/PostgreSQL/18/data/imports/
-- Usage    : CALL bronze.load_bronze();
-- =============================================================

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    -- Track timing per table and total batch
    v_start        TIMESTAMP;
    v_batch_start  TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Starting Bronze Layer Load';
    RAISE NOTICE '==========================================';

    -- ----------------------------------------------------------
    -- CRM TABLES
    -- Source system: CRM (Customer Relationship Management)
    -- ----------------------------------------------------------
    RAISE NOTICE '--- Loading CRM Tables ---';

    -- Load customer master data
    -- Contains: customer ID, key, name, marital status, gender, create date
    v_start := clock_timestamp();
    TRUNCATE TABLE bronze.crm_customer_info;
    COPY bronze.crm_customer_info
    FROM '/Library/PostgreSQL/18/data/imports/cust_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE NOTICE '>> crm_customer_info loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- Load product catalogue data
    -- Contains: product ID, key, name, cost, line, start and end dates
    v_start := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/Library/PostgreSQL/18/data/imports/prd_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE NOTICE '>> crm_prd_info loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- Load transactional sales data
    -- Contains: order number, product key, customer ID,
    --           order/ship/due dates (stored as INT), sales, qty, price
    v_start := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/Library/PostgreSQL/18/data/imports/sales_details.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE NOTICE '>> crm_sales_details loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- ----------------------------------------------------------
    -- ERP TABLES
    -- Source system: ERP (Enterprise Resource Planning)
    -- ----------------------------------------------------------
    RAISE NOTICE '--- Loading ERP Tables ---';

    -- Load ERP customer demographic data
    -- Contains: customer ID (with NAS prefix), birth date, gender
    v_start := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/Library/PostgreSQL/18/data/imports/CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE NOTICE '>> erp_cust_az12 loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- Load ERP customer location data
    -- Contains: customer ID (with hyphens), country code
    v_start := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/Library/PostgreSQL/18/data/imports/LOC_A101.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE NOTICE '>> erp_loc_a101 loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- Load ERP product category reference data
    -- Contains: category ID, category, subcategory, maintenance flag
    v_start := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/Library/PostgreSQL/18/data/imports/PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE NOTICE '>> erp_px_cat_g1v2 loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- ----------------------------------------------------------
    -- COMPLETION SUMMARY
    -- ----------------------------------------------------------
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Bronze layer loaded successfully.';
    RAISE NOTICE 'Total duration: % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_batch_start))::INT;
    RAISE NOTICE '==========================================';

-- ----------------------------------------------------------
-- ERROR HANDLING
-- Catches any failure and prints the error code and message
-- without crashing the session
-- ----------------------------------------------------------
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ERROR during Bronze load: % - %', SQLSTATE, SQLERRM;
END;
$$;


-- =============================================================
-- HOW TO VERIFY THE PROCEDURE
-- =============================================================

-- 1. Check the procedure is registered in the database:
SELECT routine_name, routine_type
FROM information_schema.routines
WHERE routine_schema = 'bronze';

-- 2. View the procedure source code:
SELECT prosrc
FROM pg_proc
WHERE proname = 'load_bronze';

-- 3. Execute the procedure:
CALL bronze.load_bronze();

-- 4. Verify row counts after loading:
SELECT 'crm_customer_info' AS table_name, COUNT(*) FROM bronze.crm_customer_info
UNION ALL
SELECT 'crm_prd_info',      COUNT(*) FROM bronze.crm_prd_info
UNION ALL
SELECT 'crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
UNION ALL
SELECT 'erp_cust_az12',     COUNT(*) FROM bronze.erp_cust_az12
UNION ALL
SELECT 'erp_loc_a101',      COUNT(*) FROM bronze.erp_loc_a101
UNION ALL
SELECT 'erp_px_cat_g1v2',   COUNT(*) FROM bronze.erp_px_cat_g1v2;
