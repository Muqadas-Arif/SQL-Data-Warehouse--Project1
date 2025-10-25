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


