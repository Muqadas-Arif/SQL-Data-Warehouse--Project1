use datawarehouse
if object_id('silver.crm_cust_info','U') is not null
drop table silver.crm_cust,info;
CREATE TABLE silver.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(60),
cst_gndr NVARCHAR(50),
cst_create_date DATE
)

if object_id('silver.crm_sales_details','U') is not null
drop table silver.crm_sales_details;
create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int,
);

if object_id('silver.erp_loc_a101','U') is not null
drop table silver.erp_loc_a101;
create table silver.erp_loc_a101 (
cid nvarchar(50),
cntry nvarchar(50)
);
if object_id('silver.erp_cust_az12','U') is not null
drop table silver.erp_cust_az12;
create table silver.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50)
);

if object_id('silver.erp_px_cat_glv2','U') is not null
drop table silver.erp_px_cat_glv2;
create table silver.erp_px_cat_glv2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50)
);

if object_id('silver.crm_prd_info','U') is not null
drop table silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info
(
    prd_id              INT,
    prd_key             NVARCHAR(50),
    prd_nm              NVARCHAR(50),
    prd_cost            INT,
    prd_line            NVARCHAR(50),
    prd_start_dt        DATETIME,
    prd_end_dt   DATETIME
);
