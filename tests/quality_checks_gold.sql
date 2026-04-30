/*
=============================================================
Quality Checks: Gold Layer
=============================================================
Script Purpose:
    This script performs quality checks on all gold layer
    views to validate the star schema is correctly built.
    It includes checks for:
    - Null or duplicate surrogate keys.
    - Orphan records in the fact table.
    - Data consistency between fact and dimensions.
    - Row count validation.

Usage Notes:
    - Run these checks after loading the Silver Layer.
    - Gold views update automatically so no reload needed.
    - Every query should return 0 rows if data is clean.
    - If rows are returned, investigate the Silver layer.
=============================================================
*/

-- =============================================================
-- gold.dim_customers
-- =============================================================

-- Check 1: No duplicate surrogate keys
-- Expected: 0 rows
SELECT customer_key, COUNT(*)
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Check 2: No NULL surrogate keys
-- Expected: 0 rows
SELECT *
FROM gold.dim_customers
WHERE customer_key IS NULL;

-- Check 3: No NULL customer IDs
-- Expected: 0 rows
SELECT *
FROM gold.dim_customers
WHERE customer_id IS NULL;

-- Check 4: Only valid gender values
-- Expected: Female, Male, N/A only
SELECT DISTINCT gender
FROM gold.dim_customers
ORDER BY gender;

-- Check 5: Only valid marital status values
-- Expected: Single, Married, N/A only
SELECT DISTINCT marital_status
FROM gold.dim_customers
ORDER BY marital_status;

-- Check 6: No future birth dates
-- Expected: 0 rows
SELECT *
FROM gold.dim_customers
WHERE birth_date > CURRENT_DATE;

-- Check 7: Row count
-- Expected: less than bronze (duplicates removed)
SELECT COUNT(*) AS total_customers
FROM gold.dim_customers;

-- =============================================================
-- gold.dim_products
-- =============================================================

-- Check 8: No duplicate surrogate keys
-- Expected: 0 rows
SELECT product_key, COUNT(*)
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- Check 9: No NULL surrogate keys
-- Expected: 0 rows
SELECT *
FROM gold.dim_products
WHERE product_key IS NULL;

-- Check 10: No NULL or negative costs
-- Expected: 0 rows
SELECT *
FROM gold.dim_products
WHERE cost < 0 OR cost IS NULL;

-- Check 11: Only valid product line values
-- Expected: Mountain, Road, Touring, Other Sales, N/A
SELECT DISTINCT product_line
FROM gold.dim_products
ORDER BY product_line;

-- Check 12: All products have a category
-- Expected: 0 rows (every product should link to a category)
SELECT *
FROM gold.dim_products
WHERE category IS NULL;

-- Check 13: Row count
-- Expected: current products only (no historical)
SELECT COUNT(*) AS total_products
FROM gold.dim_products;

-- =============================================================
-- gold.fact_sales
-- =============================================================

-- Check 14: No NULL surrogate keys in fact table
-- These would be orphan records with no dimension match
-- Expected: 0 rows
SELECT *
FROM gold.fact_sales
WHERE customer_key IS NULL
   OR product_key IS NULL;

-- Check 15: All customer keys exist in dim_customers
-- Expected: 0 rows
SELECT f.*
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- Check 16: All product keys exist in dim_products
-- Expected: 0 rows
SELECT f.*
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

-- Check 17: No NULL or negative sales amounts
-- Expected: 0 rows
SELECT *
FROM gold.fact_sales
WHERE sales_amount IS NULL
   OR sales_amount <= 0;

-- Check 18: Sales = quantity * price
-- Expected: 0 rows
SELECT *
FROM gold.fact_sales
WHERE sales_amount != quantity * price;

-- Check 19: No invalid date orders (order before ship and due)
-- Expected: 0 rows
SELECT *
FROM gold.fact_sales
WHERE order_date > shipping_date
   OR order_date > due_date;

-- Check 20: Row count
-- Expected: same as bronze crm_sales_details (60398)
SELECT COUNT(*) AS total_sales
FROM gold.fact_sales;

-- =============================================================
-- Final star schema validation query
-- The ultimate test — joins all three gold objects
-- If this returns clean data, your warehouse is working
-- =============================================================
SELECT
    c.first_name,
    c.last_name,
    c.country,
    p.product_name,
    p.category,
    p.product_line,
    f.order_date,
    f.sales_amount,
    f.quantity,
    f.price
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
JOIN gold.dim_products  p ON f.product_key  = p.product_key
ORDER BY f.order_date DESC
LIMIT 20;

-- =============================================================
-- Row count summary — all gold objects
-- =============================================================
SELECT 'dim_customers' AS view_name, COUNT(*) AS row_count
FROM gold.dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*)
FROM gold.dim_products
UNION ALL
SELECT 'fact_sales', COUNT(*)
FROM gold.fact_sales;
