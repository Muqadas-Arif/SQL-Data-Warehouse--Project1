
/* ==============================================================
   DDL SCRIPT: Create Bronze Layer Table
   --------------------------------------------------------------
   Author      : Muqadas
   Schema      : bronze
   Description : This script creates the bronze-layer CRM product
                 information table. If the table already exists,
                 it will be dropped and recreated to redefine the
                 structure safely.
============================================================== */
use datawarehouse
if object_id('bronze.crm_cust_info','U') is not null
drop table bronze.crm_cust,info;
CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(60),
cst_gndr NVARCHAR(50),
)	
	
	
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


create or alter procedure bronze.load_bronze as
begin
        declare @start_time datetime, @end_time datetime,@batch_starttime datetime,@batch_endtime datetime;  
		begin try
		set @batch_starttime=getdate();
		print'------------loading bronze layer----------------';

		print'################loading crm tables###############';
		set @start_time= getdate();

		print'>>>>>>>>>>>>truncating table bronze.crm_cust_info <<<<<<<<<<<<';
		truncate table bronze.crm_cust_info;

		print'>>>>>>>>>>>inserting data into bronze.crm_cust_info<<<<<<<<<<<';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Hp\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time= getdate();
		print'>> load duaration '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'
		print'................';

		print'>>>>>>>truncating table bronze.crm_prd_info<<<<<<<<<<<';
		set @start_time= getdate();
		truncate table bronze.crm_prd_info;

		print'>>>>>>>>>inserting data into table bronze.crm_prd_info><<<<<<<<<<<<<<<<<';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\Hp\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time= getdate();
		print'>> load duaration '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'
		print'................';

		print'>>>>>>>>>>>truncating table bronze.crm_sales_details<<<<<<<<<<';
		set @start_time= getdate();

		truncate table bronze.crm_sales_details;

		print'>>>>>>>>>>>inserting data into bronze.crm_sales_details<<<<<<<<<';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\Hp\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time= getdate();
		print'>> load duaration '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'
		print'................';

		print'#############loading erp#########';

		print'>>>>>>truncating table bronze.erp_cust_az12<<<';
		set @start_time= getdate();

		truncate table bronze.erp_cust_az12;

		print'>>>>inserting data into bronze.erp_cust_az12<<<<<<<<';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\Hp\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time= getdate();
		print'>> load duaration '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'
		print'................';

		print'<>>>>>>>>>>>>truncating table bronze.erp_loc_a101<<<<<<';
		set @start_time= getdate();

		truncate table bronze.erp_loc_a101;

		print'>>>>>>>>>>inserting data into bronze.erp_loc_a101<<<<<<<<<';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\Hp\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time= getdate();
		print'>> load duaration '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'
		print'................';

		print'>>>>>>>>>>>truncating table bronze.erp_px_cat_glv2'
		set @start_time= getdate();

		truncate table bronze.erp_px_cat_glv2;

		print'>>inserting data into bronze.erp_px_cat_glv2<<<<<<'; 
		bulk insert bronze.erp_px_cat_glv2
		from 'C:\Users\Hp\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time= getdate();
		print'>> load duaration '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'
		print'................';

		set @batch_endtime=getdate();
		print'................'
		print'>>>bronze loading comleted <<<<<'
		print'>>>total time taken<<<<<' +cast(datediff(second,@batch_starttime,@batch_endtime)  as nvarchar ) + 'seconds'
		print'======================================================================================'

		end try
		begin catch
		print'++++++++++++ error occured +++++++++++++++++++'
		print'error'+error_message();
		print'error'+ cast(error_number() as varchar);
		end catch
		end






