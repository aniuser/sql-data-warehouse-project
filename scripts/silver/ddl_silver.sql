/*
=============================================================
DDL Script: Create Silver Tables
=============================================================
Script Purpose:
    This script creates tables in the 'silver' schema,
    dropping existing tables if they already exist.
    Run this script to re-define the DDL structure
    of the 'silver' tables.

Changes from Bronze:
    - Added dwh_create_date metadata column to all tables
    - prd_start_dt and prd_end_dt changed from TIMESTAMP to DATE
    - sls_order_dt, sls_ship_dt, sls_due_dt changed from INT to DATE
    - Added cat_id column to crm_prd_info (extracted from prd_key)
=============================================================
*/

-- -------------------------------------------------------------
-- CRM Tables
-- -------------------------------------------------------------

DROP TABLE IF EXISTS silver.crm_customer_info;
CREATE TABLE silver.crm_customer_info (
    cst_id             INT,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gender         VARCHAR(50),
    cst_create_date    DATE,
    dwh_create_date    TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          VARCHAR(50),   -- extracted from prd_key
    prd_key         VARCHAR(50),   -- trimmed product number only
    prd_nm          VARCHAR(100),
    prd_cost        NUMERIC(10,2),
    prd_line        VARCHAR(50),
    prd_start_dt    DATE,          -- cast from TIMESTAMP
    prd_end_dt      DATE,          -- rebuilt using LEAD()
    dwh_create_date TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num     VARCHAR(50),
    sls_prd_key     VARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,          -- converted from INT
    sls_ship_dt     DATE,          -- converted from INT
    sls_due_dt      DATE,          -- converted from INT
    sls_sales       NUMERIC(10,2),
    sls_quantity    INT,
    sls_price       NUMERIC(10,2),
    dwh_create_date TIMESTAMP DEFAULT NOW()
);

-- -------------------------------------------------------------
-- ERP Tables
-- -------------------------------------------------------------

DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid             VARCHAR(50),
    bdate           DATE,
    gen             VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid             VARCHAR(50),
    cntry           VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id              VARCHAR(50),
    cat             VARCHAR(50),
    subcat          VARCHAR(50),
    maintenance     VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT NOW()
);
