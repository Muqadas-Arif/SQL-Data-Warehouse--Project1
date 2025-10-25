/* ===============================================================
   🧱 DATA WAREHOUSE PROJECT — BRONZE · SILVER · GOLD
   ---------------------------------------------------------------
   Author      : Muqadas Arif
   Description : Complete Data Warehouse implementation showing
                 3-tier architecture: Bronze → Silver → Gold.
   Purpose     : To demonstrate practical ETL, data modeling, and 
                 analytical SQL skills for professional portfolio use.
   =============================================================== */


/* ===============================================================
   ⚙️ PROJECT OVERVIEW
   ---------------------------------------------------------------
   This project implements a modern Data Warehouse pipeline where:
   
   🔸 BRONZE layer  → stores raw source data.
   🔸 SILVER layer  → cleanses, standardizes, and transforms data.
   🔸 GOLD layer    → creates analytical models (facts & dimensions).

   Each step reflects a real-world data engineering workflow,
   ensuring data quality, lineage, and consistency.
   =============================================================== */


/* ===============================================================
   🟤 BRONZE LAYER — RAW SOURCE DATA
   ---------------------------------------------------------------
   ▪ Purpose :
     - Acts as the landing zone for raw CRM and ERP datasets.
     - Retains original data structure for traceability.

   ▪ Key Tables :
     - bronze.crm_cust_info
     - bronze.crm_prd_info
     - bronze.crm_sales_details
     - bronze.erp_cust_az12
     - bronze.erp_loc_a101
     - bronze.erp_px_cat_glv2

   ▪ Notes :
     - Only essential filtering (like non-null IDs).
     - Each table feeds into its respective Silver counterpart.
   =============================================================== */


/* ===============================================================
   ⚪ SILVER LAYER — CLEANSING & STANDARDIZATION
   ---------------------------------------------------------------
   ▪ Purpose :
     - To clean, validate, and enrich Bronze data.
     - To apply transformations that make data analytics-ready.

   ▪ Transformation Logic :

     1️⃣ silver.crm_cust_info
         → Extracts unique customer records.
         → Standardizes gender & marital status fields.
         → Keeps latest record using ROW_NUMBER() logic.

     2️⃣ silver.crm_prd_info
         → Extracts and formats product information.
         → Cleans cost data and expands product line codes.
         → Derives category_id from product key.

     3️⃣ silver.crm_sales_details
         → Validates sales, order, ship, and due dates.
         → Fixes inconsistent price/sales calculations.
         → Ensures sales = quantity × price where possible.

     4️⃣ silver.erp_cust_az12
         → Standardizes customer demographic data.
         → Converts gender formats and validates birthdates.

     5️⃣ silver.erp_loc_a101
         → Cleans country codes and replaces abbreviations 
           (e.g., ‘US’ → ‘United States’, ‘DE’ → ‘Germany’).

     6️⃣ silver.erp_px_cat_glv2
         → Retains product category hierarchy.
         → Serves as mapping for higher-level classifications.

   ▪ Notes :
     - Ensures consistency across CRM and ERP datasets.
     - All Silver tables form the base for Gold layer joins.
   =============================================================== */


/* ===============================================================
   🟡 GOLD LAYER — ANALYTICAL MODELING
   ---------------------------------------------------------------
   ▪ Purpose :
     - To deliver business-ready data models.
     - To structure datasets into analytical dimensions and facts.

   ▪ Core Views :

     1️⃣ gold.dim_customers
         → Integrates customer information from:
             - silver.crm_cust_info
             - silver.erp_cust_az12
             - silver.erp_loc_a101
         → Produces:
             - customer_key (ROW_NUMBER surrogate)
             - gender resolution (CRM first, ERP fallback)
             - birthdate and country enrichment.

     2️⃣ gold.dimension_products
         → Builds the product dimension from:
             - silver.crm_prd_info
             - silver.erp_px_cat_glv2
         → Produces:
             - product_key surrogate
             - lifecycle attributes (start_date, end_date)
             - filters active products (prd_end_dt IS NULL).

     3️⃣ gold.fact_sales
         → Central fact table connecting:
             - silver.crm_sales_details
             - gold.dimension_products
             - gold.dim_customers
         → Contains:
             - order_number, product_key, customer_key
             - order, shipping, and due dates
             - sales_amount, quantity, and price.

   ▪ Notes :
     - Represents a classic star schema design.
     - Ready for BI tools like Power BI, Tableau, or Looker.
   =============================================================== */


/* ===============================================================
   🧠 SQL CONCEPTS USED
   ---------------------------------------------------------------
   - Window Functions : ROW_NUMBER(), LEAD().
   - Conditional Logic : CASE, COALESCE, ISNULL.
   - Joins : LEFT JOIN for dimensional enrichment.
   - Data Cleaning : TRIM, REPLACE, CAST, UPPER.
   - Surrogate Key Generation : ROW_NUMBER() pattern.
   - Analytical Modeling : Fact and Dimension views.
   =============================================================== */


/* ===============================================================
   📈 PROJECT VALUE
   ---------------------------------------------------------------
   ✅ Showcases end-to-end ETL and Data Warehouse design.
   ✅ Demonstrates real SQL engineering with multi-layer logic.
   ✅ Resume-ready project for data analytics and engineering roles.
   ✅ Highlights clear documentation and maintainable SQL structure.
   ✅ Easily extendable to orchestration tools (Airflow, dbt).
   =============================================================== */


/* ===============================================================
   💡 AUTHOR NOTE
   ---------------------------------------------------------------
   Muqadas Arif  
   Data Engineer & Analyst — Pakistan 🇵🇰  
   Focused on crafting structured, insightful, and scalable 
   data pipelines that turn raw data into decision power.
   =============================================================== */
