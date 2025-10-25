/* ===============================================================
   VIEW CREATION SCRIPT: gold.dim_customers
   ---------------------------------------------------------------
   Author: Muqadas
   Purpose:
       This view consolidates and transforms customer-related data 
       from multiple Silver-layer tables into a unified Gold-layer 
       dimension view. It serves as a single, cleaned, and enriched 
       source of customer information for analytical and reporting 
       purposes.

   Description:
       - Combines customer core data from silver.crm_cust_info (CI)
         with additional demographic and location details from 
         silver.erp_cust_az12 (CA) and silver.erp_loc_a101 (LA).
       - Generates a unique surrogate key (customer_key) using 
         ROW_NUMBER() for consistent dimensional referencing.
       - Cleans and enriches gender data by replacing 'n/a' values 
         with available gender information from the ERP source.
       - Standardizes key attributes such as customer_id, names, 
         marital status, gender, birthdate, and country.

   Output Columns:
       - customer_key        : Surrogate key for each unique customer
       - customer_id         : Original customer identifier
       - customer_number     : Unique customer number from CRM
       - first_name          : Customer’s first name
       - last_name           : Customer’s last name
       - marital_status      : Marital status from CRM system
       - gender              : Cleaned gender value (from CRM or ERP)
       - birthdate           : Customer birthdate from ERP data
       - country             : Country location of the customer

   Data Sources:
       1. silver.crm_cust_info   (Primary CRM source)
       2. silver.erp_cust_az12   (ERP demographic data)
       3. silver.erp_loc_a101    (ERP location data)
   ---------------------------------------------------------------
*/



create view gold.dim_customers as
SELECT
    ROW_NUMBER() over (order by cst_id) as customer_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    ci.cst_material_status as marital_status,
    case when ci.cst_gndr !='n/a' then ci.cst_gndr
	else coalesce(ca.gen,'n/a')
	end as gender,
    ca.bdate as birthdate,
	la.cntry as country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid

/* ===============================================================
   VIEW CREATION SCRIPT: gold.dimension_products
   ---------------------------------------------------------------
   Author: Muqadas
   Purpose:
       This view integrates and refines product-related information 
       from multiple Silver-layer tables into a single Gold-layer 
       dimension. It is designed to serve as a clean and consistent 
       source of product data for analytics, reporting, and business 
       intelligence models.

   Description:
       - Combines core product attributes from silver.crm_prd_info (PN)
         with category reference data from silver.erp_px_cat_glv2 (PC).
       - Creates a surrogate key (product_key) using ROW_NUMBER() for 
         consistent identification within the dimensional model.
       - Includes only currently active products by filtering out 
         records where prd_end_dt is not null.
       - Standardizes key product attributes including IDs, names, 
         categories, costs, product lines, and validity dates.

   Output Columns:
       - product_key       : Surrogate key for each product record
       - product_id        : Original product identifier
       - category_id       : Product category identifier
       - product_number    : Unique product number
       - product_name      : Product name as listed in CRM
       - cost              : Product cost or base price
       - product_line      : Line or series of the product
       - start_date        : Product introduction date
       - end_date          : Product discontinuation date (null = active)

   Data Sources:
       1. silver.crm_prd_info     (Primary CRM product information)
       2. silver.erp_px_cat_glv2  (ERP product category reference)
   ---------------------------------------------------------------
*/



create view gold.dimension_products as
SELECT
    ROW_NUMBER() over(order by pn.prd_start_dt,pn.prd_key) as product_key,
    pn.prd_id as product_id,
    pn.cat_id as category_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,
    pn.prd_cost as cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date,
    pn.prd_end_dt as end_date
FROM silver.crm_prd_info pn
left join silver.erp_px_cat_glv2 pc
on pn.cat_id=pc.id
where prd_end_dt is null

	/* ===============================================================
   VIEW CREATION SCRIPT: gold.fact_sales
   ---------------------------------------------------------------
   Author: Muqadas
   Purpose:
       This view constructs the core Sales Fact table within the 
       Gold layer of the data warehouse. It integrates transactional 
       sales data with related product and customer dimensions to 
       support comprehensive sales performance analytics.

   Description:
       - Combines detailed sales transactions from silver.crm_sales_details 
         (SD) with dimensional references from gold.dimension_products (PR) 
         and gold.dim_customers (CU).
       - Links each sales record to its corresponding product and customer 
         using surrogate keys to ensure referential consistency in reporting.
       - Provides key financial and operational measures such as sales amount, 
         quantity, price, and order lifecycle dates.

   Output Columns:
       - order_number   : Unique identifier for each sales order
       - product_key    : Foreign key reference to product dimension
       - customer_key   : Foreign key reference to customer dimension
       - order_date     : Date when the order was placed
       - shipping_date  : Date when the order was shipped
       - due_date       : Expected delivery or due date
       - sales_amount   : Total sales amount for the order
       - sls_quantity   : Quantity of products sold
       - sls_price      : Unit price of the product sold

   Data Sources:
       1. silver.crm_sales_details   (Primary transactional sales data)
       2. gold.dimension_products    (Product dimension reference)
       3. gold.dim_customers         (Customer dimension reference)

   Notes:
       - Ensures accurate dimensional mapping through joins on 
         product_number and customer_id.
       - Can be used directly by BI tools to analyze revenue, sales trends, 
         and customer-product relationships.
   ---------------------------------------------------------------
*/




create view gold.fact_sales as
SELECT
    sd.sls_ord_num as order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity,
    sd.sls_price
FROM silver.crm_sales_details sd
left join gold.dimension_products pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id=cu.customer_id




