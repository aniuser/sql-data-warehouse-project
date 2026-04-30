/*
===============================================================
Quality Checks
===============================================================
Script Purpose:
    This script performs various quality checks for data
    consistency, accuracy, and standardization across the
    'silver' schema. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after loading the Silver Layer.
    - Investigate and resolve any discrepancies found
      during the checks.
===============================================================
*/

-- =============================================================
-- silver.crm_customer_info
-- =============================================================

-- Check 1: No duplicate customer IDs
-- Expected: 0 rows
SELECT cst_id, COUNT(*)
FROM silver.crm_customer_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Check 2: No NULL customer IDs
-- Expected: 0 rows
SELECT *
FROM silver.crm_customer_info
WHERE cst_id IS NULL;

-- Check 3: No unwanted spaces in name columns
-- Expected: 0 rows
SELECT cst_firstname
FROM silver.crm_customer_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Check 4: Only valid gender values (Female, Male, N/A)
-- Expected: only 3 distinct values
SELECT DISTINCT cst_gender
FROM silver.crm_customer_info;

-- Check 5: Only valid marital status values (Single, Married, N/A)
-- Expected: only 3 distinct values
SELECT DISTINCT cst_marital_status
FROM silver.crm_customer_info;

-- =============================================================
-- silver.crm_prd_info
-- =============================================================

-- Check 6: No NULL or negative product costs
-- Expected: 0 rows
SELECT *
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check 7: Only valid product line values
-- Expected: Mountain, Road, Other Sales, Touring, N/A
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check 8: Start date is before end date
-- Expected: 0 rows
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Check 9: cat_id is correctly extracted (no dashes)
-- Expected: 0 rows
SELECT cat_id
FROM silver.crm_prd_info
WHERE cat_id LIKE '%-%';

-- =============================================================
-- silver.crm_sales_details
-- =============================================================

-- Check 10: No invalid sales values (null, zero, negative)
-- Expected: 0 rows
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_sales <= 0;

-- Check 11: Sales = quantity * price
-- Expected: 0 rows
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price;

-- Check 12: Order date is before ship and due date
-- Expected: 0 rows
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- Check 13: No invalid dates remaining
-- Expected: 0 rows
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL
  AND sls_ship_dt IS NULL
  AND sls_due_dt IS NULL;

-- =============================================================
-- silver.erp_cust_az12
-- =============================================================

-- Check 14: No NAS prefix remaining in cid
-- Expected: 0 rows
SELECT cid
FROM silver.erp_cust_az12
WHERE cid LIKE 'NAS%';

-- Check 15: No future birth dates
-- Expected: 0 rows
SELECT *
FROM silver.erp_cust_az12
WHERE bdate > CURRENT_DATE;

-- Check 16: Only valid gender values
-- Expected: Female, Male, N/A
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- =============================================================
-- silver.erp_loc_a101
-- =============================================================

-- Check 17: No hyphens remaining in cid
-- Expected: 0 rows
SELECT cid
FROM silver.erp_loc_a101
WHERE cid LIKE '%-%';

-- Check 18: Only clean country values
-- Expected: Full country names only (no codes like US, DE)
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- =============================================================
-- Row count summary — all silver tables
-- =============================================================
SELECT 'crm_customer_info' AS table_name, COUNT(*) FROM silver.crm_customer_info
UNION ALL
SELECT 'crm_prd_info',      COUNT(*) FROM silver.crm_prd_info
UNION ALL
SELECT 'crm_sales_details', COUNT(*) FROM silver.crm_sales_details
UNION ALL
SELECT 'erp_cust_az12',     COUNT(*) FROM silver.erp_cust_az12
UNION ALL
SELECT 'erp_loc_a101',      COUNT(*) FROM silver.erp_loc_a101
UNION ALL
SELECT 'erp_px_cat_g1v2',   COUNT(*) FROM silver.erp_px_cat_g1v2;
