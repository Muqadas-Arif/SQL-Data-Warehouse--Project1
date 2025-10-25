/*═══════════════════════════════════════════════════════════════════════
 💎 DATA WAREHOUSE SILVER LAYER TRANSFORMATION  
 🧩 ETL Process: From Bronze → Silver
 ------------------------------------------------------------------------
 This section describes the data flow and transformations applied while
 loading data from the Bronze Layer to the Silver Layer. Each table below
 ensures data cleansing, standardization, and enrichment for analytics.
═══════════════════════════════════════════════════════════════════════*/


-- 🧠 TABLE 1 – silver.crm_cust_info
-- ------------------------------------------------------------
-- PURPOSE:
-- Refine and deduplicate customer data from bronze.crm_cust_info.
--
-- PROCESS:
-- • Removes duplicates using ROW_NUMBER() (keeps latest record).
-- • Trims spaces from names and marital status fields.
-- • Converts codes:
--     S → Single
--     M → Married
--     Else → n/a
-- • Standardizes gender:
--     F → Female
--     M → Male
--     Unknown → n/a
--
-- RESULT:
-- Cleaned, deduplicated, and standardized customer master data.


insert into silver.crm_cust_info
	(cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gndr

	)
select 
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname ,
case when upper(trim(cst_material_status))='S' then 'Single'
     when upper(trim(cst_material_status))='M' then 'Married'
	 else 'n/a'
	 end cst_material_status,

case when upper(trim(cst_gndr))='F' then 'Female'
     when upper(trim(cst_gndr))='M' then 'Male'
	 else 'n/a'
	 end as cst_gndr

from (
select
*,
row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id is not null
)t where flag_last =1


	
-- 📦 TABLE 2 – silver.crm_prd_info
-- ------------------------------------------------------------
-- PURPOSE:
-- Transform and standardize product information.
--
-- PROCESS:
-- • Extracts product key using SUBSTRING and REPLACE.
-- • Replaces null or missing cost values with 0.
-- • Converts product line codes:
--     M → Mountain
--     R → Road
--     S → Other Sales
--     T → Touring
--     Else → n/a
-- • Calculates prd_end_dt using LEAD() for product period tracking.
-- • Builds cat_id by reformatting the prd_key string.
--
-- RESULT:
-- Clean, consistent product dimension table with readable values.



use datawarehouse
insert into silver.crm_prd_info (
prd_id,
prd_key, 
prd_nm, 
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt,
cat_id          
)

select 
	prd_id,
	substring(prd_key,7,len(prd_key)) as prd_key,
	prd_nm,
	isnull(prd_cost,0) as prd_cost,
case when upper(trim(prd_line))='M' then 'Mountain'
	when upper(trim(prd_line))='R' then 'Road'
	when upper(trim(prd_line))='S' then 'other Sales'
	when upper(trim(prd_line))='T' then 'Touring'
	else 'n/a'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt,
replace(substring(prd_key,1,5), '-','_') as cat_id
from bronze.crm_prd_info
order by prd_id


select*from bronze.crm_prd_info

	
-- 💰 TABLE 3 – silver.crm_sales_details
-- ------------------------------------------------------------
-- PURPOSE:
-- Validate and correct sales transaction data.
--
-- PROCESS:
-- • Cleans invalid or incorrectly formatted date fields.
-- • Ensures sales = quantity × price; fixes mismatches.
-- • Handles null or negative prices by recalculating logically.
-- • Ensures integrity across all sales transaction fields.
--
-- RESULT:
-- Verified and corrected sales fact table for accurate reporting.

insert  into silver.crm_sales_details(
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
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,

case when sls_order_dt=0 or len(sls_order_dt)!=8 then null
else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,

case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then null
else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt,

case when sls_due_dt=0 or len(sls_due_dt)!=8 then null
else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt,

case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
then sls_quantity  * abs(sls_sales)
else sls_sales
end as sls_sales,

sls_quantity,


case when sls_price is null or sls_price <=0 
then sls_price /nullif(sls_quantity,0)
else sls_price 
end as sls_price

from bronze.crm_sales_details


-- 👤 TABLE 4 – silver.erp_cust_az12
-- ------------------------------------------------------------
-- PURPOSE:
-- Standardize and clean customer demographic data.
--
-- PROCESS:
-- • Removes prefix ‘NAS’ from customer IDs.
-- • Replaces future or invalid birthdates with NULL.
-- • Normalizes gender entries:
--     F / FEMALE → Female
--     M / MALE → Male
--     Else → n/a
--
-- RESULT:
-- Clean and reliable demographic data for integration.


insert into silver.erp_cust_az12(cid,bdate,gen)
select
case when cid like 'NAS%' then substring(cid,4,len(cid))
else cid
end as cid,
case when bdate > getdate() then null
else bdate
end as bdate,
case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
when upper(trim(gen)) in ('M','MALE') then 'Male'
else 'n/a'
end as gen
from bronze.erp_cust_az12

-- 🌍 TABLE 5 – silver.erp_loc_a101
-- ------------------------------------------------------------
-- PURPOSE:
-- Standardize and enhance customer location details.
--
-- PROCESS:
-- • Removes hyphens from customer IDs.
-- • Expands country codes:
--     DE → Germany
--     US / USA → United States
--     NULL or empty → n/a
-- • Ensures all country names follow consistent formatting.
--
insert into silver.erp_loc_a101(cid,cntry)
select 
replace (cid,'-','') cid,
case when trim(cntry)='DE' then 'Germany'
     when trim(cntry) in ('US','USA') THEN 'United states' 
	 when trim(cntry) ='' or cntry is null then 'n/a'
	 else trim(cntry)
	 end as cnrty
from bronze.erp_loc_a101

-- 🗂 TABLE 6 – silver.erp_px_cat_glv2
-- ------------------------------------------------------------
-- PURPOSE:
-- Load validated product category data from bronze to silver.
--
-- PROCESS:
-- • Transfers clean records directly (id, cat, subcat, maintenance).
-- • No transformation required (data already verified).
--
-- RESULT:
-- Reliable category reference table for mapping and classification.


use datawarehouse
insert into silver.erp_px_cat_glv2(id,cat,subcat,maintenance)
select
id,cat,subcat,maintenance
from bronze.erp_px_cat_glv2
















