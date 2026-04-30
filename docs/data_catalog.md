# Data Catalog — SQL Data Warehouse Project

## Gold Layer

### gold.dim_customers
| Column | Data Type | Description | Example |
|---|---|---|---|
| customer_key | INT | Surrogate key — system generated | 1, 2, 3 |
| customer_id | INT | Original customer ID from CRM | 11000 |
| customer_number | VARCHAR | Customer business key | AW00011000 |
| first_name | VARCHAR | Customer first name (trimmed) | Jon |
| last_name | VARCHAR | Customer last name (trimmed) | Yang |
| country | VARCHAR | Country from ERP location data | United States |
| marital_status | VARCHAR | Single or Married | Single |
| gender | VARCHAR | Female, Male or N/A | Male |
| birth_date | DATE | Customer birth date | 1966-04-08 |
| create_date | DATE | Date customer was created in CRM | 2025-10-06 |

### gold.dim_products
| Column | Data Type | Description | Example |
|---|---|---|---|
| product_key | INT | Surrogate key — system generated | 1, 2, 3 |
| product_id | INT | Original product ID from CRM | 1 |
| product_number | VARCHAR | Product business key | BK-R93R-62 |
| product_name | VARCHAR | Full product name | Road-750 Black, 44 |
| category_id | VARCHAR | Category ID extracted from product key | AC_HE |
| category | VARCHAR | Product category from ERP | Accessories |
| subcategory | VARCHAR | Product subcategory from ERP | Helmets |
| maintenance | VARCHAR | Maintenance flag from ERP | Yes |
| cost | NUMERIC | Product cost | 734.04 |
| product_line | VARCHAR | Mountain, Road, Touring, Other Sales | Road |
| start_date | DATE | Date product became active | 2025-10-06 |

### gold.fact_sales
| Column | Data Type | Description | Example |
|---|---|---|---|
| product_key | INT | Foreign key to dim_products | 1 |
| customer_key | INT | Foreign key to dim_customers | 1 |
| order_number | VARCHAR | Sales order number | SO43697 |
| order_date | DATE | Date order was placed | 2025-07-01 |
| shipping_date | DATE | Date order was shipped | 2025-07-08 |
| due_date | DATE | Date order was due | 2025-07-13 |
| sales_amount | NUMERIC | Total sales amount | 3578.27 |
| quantity | INT | Number of items ordered | 1 |
| price | NUMERIC | Unit price | 3578.27 |
