/* ===============================================================
   DATA WAREHOUSE INITIALIZATION SCRIPT
   ---------------------------------------------------------------
   Author: Muqadas
   Purpose: To safely create a new Data Warehouse database and
            its three-tier schema architecture: Bronze, Silver, Gold.
   
   Notes:
   - If an existing database named 'DataWarehouse' exists, 
     it will be DROPPED (all data will be permanently lost).
   - The script ensures idempotency: running it multiple times 
     will not cause duplication errors.
================================================================= */
USE master;
Go

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create core schemas
CREATE SCHEMA Bronze;
GO

CREATE SCHEMA Silver;
GO

CREATE SCHEMA Gold;
GO
se datawarehouse
if object_id('bronze.crm_sales_details','U') is not null
drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
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

if object_id('bronze.erp_loc_a101','U') is not null
drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101 (
cid nvarchar(50),
cntry nvarchar(50)
);
if object_id('bronze.erp_cust_az12','U') is not null
drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50)
);

if object_id('bronze.erp_px_cat_glv2','U') is not null
drop table bronze.erp_px_cat_glv2;
create table bronze.erp_px_cat_glv2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50)
);

if object_id('bronze.crm_prd_info','U') is not null
drop table bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info
(
    prd_id              INT,
    prd_key             NVARCHAR(50),
    prd_nm              NVARCHAR(50),
    prd_cost            INT,
    prd_line            NVARCHAR(50),
    prd_start_dt        DATETIME,
    prd_end_dt   DATETIME
);


