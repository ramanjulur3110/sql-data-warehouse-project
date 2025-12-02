/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
create view gold.dim_customers as 
select 
	row_number() over (order by c.cst_id) as customer_key,
	c.cst_id as customer_id, 
	c.cst_key as customer_number, 
	c.cst_firstname as first_name, 
	c.cst_lastname as last_name, 
	b.cntry as country,
	c.cst_marital_status as marital_status, 
	CASE
		WHEN c.cst_gender = 'n/a' then a.gen
		else c.cst_gender
	END as gender,
	a.bdate as birthdate,
	c.cst_create_date as create_date
from silver.crm_cust_info c
left join silver.erp_cust_az12 a
	on a.cid = c.cst_key
left join silver.erp_loc_a101 b
	on b.cid = c.cst_key
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
create view gold.dim_products as 
select
	row_number() over (order by a.prd_key, a.prd_start_dt) as product_key,  
	a.prd_id as product_id, 
	a.prd_key as product_number, 
	a.prd_nm as product_name, 
	a.cat_id as category_id, 
	b.cat as category,
	b.subcat as subcategory,
	b.maintenance,
	a.prd_cost as cost, 
	a.prd_line as product_line, 
	a.prd_start_dt as start_date
from silver.crm_prd_info a
left join silver.erp_px_cat_g1v2 b
	on a.cat_id = b.id
where a.prd_end_dt is NULL
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
create view gold.fact_sales as
select 
	a.sls_ord_num order_number, 
	c.product_key as product_key,
	b.customer_key as customer_key,
	a.sls_order_dt as order_date, 
	a.sls_ship_dt as shipping_date, 
	a.sls_due_dt as due_date, 
	a.sls_sales as sales, 
	a.sls_quantity as quantity, 
	a.sls_price as price
from silver.crm_sales_details a
left join gold.dim_customers b
	on a.sls_cust_id = b.customer_id
left join gold.dim_products c
	on a.sls_prd_key = c.product_number
GO
