/*
=============================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=============================================================
Script Purpose:
    This stored procedure loads and transforms data from the
    'bronze' schema into the 'silver' schema.
    It performs the following actions:
    - Truncates silver tables before loading.
    - Applies data quality fixes and transformations.
    - Inserts clean data into silver tables.

Transformations Applied:
    crm_customer_info : Remove duplicates, trim spaces,
                        normalise gender and marital status
    crm_prd_info      : Extract cat_id, fix end dates via LEAD(),
                        normalise product line, fix NULL costs
    crm_sales_details : Convert INT dates to DATE, fix broken
                        sales and price calculations
    erp_cust_az12     : Remove NAS prefix, nullify future dates,
                        normalise gender formats
    erp_loc_a101      : Remove hyphens from cid,
                        normalise country names
    erp_px_cat_g1v2   : No issues — pass through cleanly

Parameters:
    None.
    This stored procedure does not accept any parameters
    or return any values.

Usage Example:
    CALL silver.load_silver();
=============================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start        TIMESTAMP;
    v_batch_start  TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '==========================================';

    -- -------------------------------------------------------
    -- 1. crm_customer_info
    -- Issues Fixed:
    --   - Duplicate customer IDs (keep most recent record)
    --   - Unwanted spaces in first and last name
    --   - Abbreviated marital status: S/M -> Single/Married
    --   - Abbreviated gender: F/M -> Female/Male
    --   - NULL customer IDs excluded
    -- -------------------------------------------------------
    v_start := clock_timestamp();
    TRUNCATE TABLE silver.crm_customer_info;
    INSERT INTO silver.crm_customer_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gender,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname)  AS cst_firstname,
        TRIM(cst_lastname)   AS cst_lastname,
        CASE UPPER(TRIM(cst_marital_status))
            WHEN 'S' THEN 'Single'
            WHEN 'M' THEN 'Married'
            ELSE 'N/A'
        END AS cst_marital_status,
        CASE UPPER(TRIM(cst_gender))
            WHEN 'F' THEN 'Female'
            WHEN 'M' THEN 'Male'
            ELSE 'N/A'
        END AS cst_gender,
        cst_create_date
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id
                ORDER BY cst_create_date DESC
            ) AS flag_last
        FROM bronze.crm_customer_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;
    RAISE NOTICE '>> silver.crm_customer_info loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- -------------------------------------------------------
    -- 2. crm_prd_info
    -- Issues Fixed:
    --   - Category ID embedded in prd_key -> extracted to cat_id
    --   - Product number extracted from prd_key
    --   - NULL costs replaced with 0
    --   - Abbreviated product line normalised to full words
    --   - Corrupted end dates rebuilt using LEAD() window function
    --   - TIMESTAMP cast to DATE
    -- -------------------------------------------------------
    v_start := clock_timestamp();
    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key))       AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0)                         AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'N/A'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
            LEAD(prd_start_dt) OVER (
                PARTITION BY SUBSTRING(prd_key, 7, LENGTH(prd_key))
                ORDER BY prd_start_dt
            ) - INTERVAL '1 day'
        AS DATE) AS prd_end_dt
    FROM bronze.crm_prd_info;
    RAISE NOTICE '>> silver.crm_prd_info loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- -------------------------------------------------------
    -- 3. crm_sales_details
    -- Issues Fixed:
    --   - Dates stored as INT converted to proper DATE type
    --   - Invalid dates (0 or not 8 digits) set to NULL
    --   - NULL/zero/negative sales recalculated as qty * price
    --   - NULL/zero/negative price recalculated as sales / qty
    -- -------------------------------------------------------
    v_start := clock_timestamp();
    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt = 0
              OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt = 0
              OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt = 0
              OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
        END AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL OR sls_sales <= 0
              OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE ABS(sls_price)
        END AS sls_price
    FROM bronze.crm_sales_details;
    RAISE NOTICE '>> silver.crm_sales_details loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- -------------------------------------------------------
    -- 4. erp_cust_az12
    -- Issues Fixed:
    --   - NAS prefix removed from customer IDs
    --   - Future birth dates set to NULL
    --   - Mixed gender formats normalised to Female/Male/N/A
    -- -------------------------------------------------------
    v_start := clock_timestamp();
    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE WHEN cid LIKE 'NAS%'
            THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END AS cid,
        CASE WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE UPPER(TRIM(gen))
            WHEN 'F'      THEN 'Female'
            WHEN 'FEMALE' THEN 'Female'
            WHEN 'M'      THEN 'Male'
            WHEN 'MALE'   THEN 'Male'
            ELSE 'N/A'
        END AS gen
    FROM bronze.erp_cust_az12;
    RAISE NOTICE '>> silver.erp_cust_az12 loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- -------------------------------------------------------
    -- 5. erp_loc_a101
    -- Issues Fixed:
    --   - Hyphens removed from customer IDs
    --   - Country codes and inconsistent names normalised
    --   - NULL and empty country values set to N/A
    -- -------------------------------------------------------
    v_start := clock_timestamp();
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE UPPER(TRIM(cntry))
            WHEN 'DE'            THEN 'Germany'
            WHEN 'US'            THEN 'United States'
            WHEN 'USA'           THEN 'United States'
            WHEN 'UNITED STATES' THEN 'United States'
            WHEN ''              THEN 'N/A'
            ELSE COALESCE(TRIM(cntry), 'N/A')
        END AS cntry
    FROM bronze.erp_loc_a101;
    RAISE NOTICE '>> silver.erp_loc_a101 loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    -- -------------------------------------------------------
    -- 6. erp_px_cat_g1v2
    -- No quality issues found — loaded as-is from bronze
    -- -------------------------------------------------------
    v_start := clock_timestamp();
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;
    RAISE NOTICE '>> silver.erp_px_cat_g1v2 loaded in % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start))::INT;

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Silver layer loaded successfully.';
    RAISE NOTICE 'Total duration: % seconds',
        EXTRACT(EPOCH FROM (clock_timestamp() - v_batch_start))::INT;
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ERROR during Silver load: % - %', SQLSTATE, SQLERRM;
END;
$$;

-- Run it:
-- CALL silver.load_silver();
