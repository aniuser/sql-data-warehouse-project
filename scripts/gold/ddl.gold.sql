/*
=============================================================
DDL Script: Create Gold Layer Views (Star Schema)
=============================================================
Script Purpose:
    This script creates analytical views in the 'gold' schema.
    These views represent the final business-ready data model
    built as a Star Schema for reporting and analytics.

Views Created:
    gold.dim_customers — customer dimension integrating CRM
                         and ERP customer/location data
    gold.dim_products  — product dimension integrating CRM
                         products and ERP categories
    gold.fact_sales    — sales fact table linking transactions
                         to customer and product dimensions

Usage:
    Views update automatically — no load procedure needed.
    Query directly: SELECT * FROM gold.fact_sales LIMIT 100;
=============================================================
*/

-- -------------------------------------------------------------
-- dim_customers
-- Source: silver.crm_customer_info (master)
--       + silver.erp_cust_az12 (demographics)
--       + silver.erp_loc_a101  (location)
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id          AS customer_id,
    ci.cst_key         AS customer_number,
    ci.cst_firstname   AS first_name,
    ci.cst_lastname    AS last_name,
    la.cntry           AS country,
    ci.cst_marital_status AS marital_status,
    -- CRM is master for gender. Use ERP only if CRM is N/A
    CASE
        WHEN ci.cst_gender != 'N/A' THEN ci.cst_gender
        ELSE COALESCE(ca.gen, 'N/A')
    END                AS gender,
    ca.bdate           AS birth_date,
    ci.cst_create_date AS create_date
FROM silver.crm_customer_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101  la ON ci.cst_key = la.cid;

-- -------------------------------------------------------------
-- dim_products
-- Source: silver.crm_prd_info (master)
--       + silver.erp_px_cat_g1v2 (categories)
-- Filter: current products only (prd_end_dt IS NULL)
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY pn.prd_start_dt, pn.prd_key
    )               AS product_key,
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;

-- -------------------------------------------------------------
-- fact_sales
-- Source: silver.crm_sales_details
-- Joins: dim_customers and dim_products via surrogate keys
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT
    pr.product_key,
    c.customer_key,
    sd.sls_ord_num  AS order_number,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products  pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers c  ON sd.sls_cust_id = c.customer_id;
