/*
=============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=============================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema
    from external CSV source files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the COPY command to load data from CSV files
      into bronze tables.

    NOTE (Mac/PostgreSQL):
    CSV files must be placed in:
    /Library/PostgreSQL/18/data/imports/
    Copy them there using Terminal:
    sudo cp /path/to/*.csv /Library/PostgreSQL/18/data/imports/

Parameters:
    None.
    This stored procedure does not accept any parameters
    or return any values.

Usage Example:
    CALL bronze.load_bronze();
=============================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start        TIMESTAMP;
    v_batch_start  TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '==========================================';

    -- -------------------------------------------------------
    -- CRM TABLES
    -- -------------------------------------------------------
    RAISE NOTICE '--- Loading CRM Tables ---';

    -- Load customer master data
    v_start := clock_timestamp();
    TRUNCATE TABLE bronze.crm_customer_info;
    COPY bronze.crm_customer_info
    FROM '/Library/PostgreSQL/18/data/imports/cust_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE NOTICE '>> bronze.crm_customer_info loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- Load product catalogue data
    v_start := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/Library/PostgreSQL/18/data/imports/prd_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE NOTICE '>> bronze.crm_prd_info loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- Load sales transaction data
    v_start := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/Library/PostgreSQL/18/data/imports/sales_details.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE NOTICE '>> bronze.crm_sales_details loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- -------------------------------------------------------
    -- ERP TABLES
    -- -------------------------------------------------------
    RAISE NOTICE '--- Loading ERP Tables ---';

    -- Load ERP customer demographic data
    v_start := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/Library/PostgreSQL/18/data/imports/CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE NOTICE '>> bronze.erp_cust_az12 loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- Load ERP customer location data
    v_start := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/Library/PostgreSQL/18/data/imports/LOC_A101.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE NOTICE '>> bronze.erp_loc_a101 loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- Load product category reference data
    v_start := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/Library/PostgreSQL/18/data/imports/PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE NOTICE '>> bronze.erp_px_cat_g1v2 loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- -------------------------------------------------------
    -- COMPLETION SUMMARY
    -- -------------------------------------------------------
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Bronze layer loaded successfully.';
    RAISE NOTICE 'Total duration: % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_batch_start))::INT;
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ERROR during Bronze load: % - %', SQLSTATE, SQLERRM;
END;
$$;
