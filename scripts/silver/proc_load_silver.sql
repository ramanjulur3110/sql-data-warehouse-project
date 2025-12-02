/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

create or alter procedure silver.load_silver as 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT ('=============================================================');
		PRINT ('Loading Silver Layer');
		PRINT ('=============================================================');

		PRINT ('**************************************************************');
		Print ('Loading CRM Tables');
		PRINT ('**************************************************************');

		SET @start_time = GETDATE()
		PRINT ('>> Truncating Table: silver.crm_cust_info <<')
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT ('>> Inserting Data Into: silver.crm_cust_info <<')
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gender,
			cst_create_date
		)
		select 
			cst_id,
			cst_key,
			TRIM (cst_firstname) as cst_firstname,
			TRIM (cst_lastname) as cst_lastname,
			CASE	
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
				ELSE 'n/a'
			END as cst_marital_status,
			CASE	
				WHEN UPPER(TRIM(cst_gender)) = 'M' then 'Male'
				WHEN UPPER(TRIM(cst_gender)) = 'F' then 'Female'
				ELSE 'n/a'
			END as cst_gender,
			cst_create_date
		from (
			select
				*,
				rank() over (partition by cst_id order by cst_create_date DESC) as row_rank
			from bronze.crm_cust_info
			where cst_id IS NOT NULL
		) as sub_query
		where row_rank = 1;
		SET @end_time = GETDATE()
		print ('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds')
		print ('----------------')

		SET @start_time = GETDATE()
		PRINT ('>> Truncating Table: silver.crm_prd_info <<')
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT ('>> Inserting Data Into: silver.crm_prd_info <<')
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select prd_id, 
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,	-- Extract category ID
		SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,			-- Extract product key
		prd_nm,
		ISNULL(prd_cost, 0) as prd_cost,
		CASE UPPER(TRIM(prd_line))
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'M' then 'Mountain'
			when 'T' then 'Touring'
			else 'n/a'
		END as prd_line, --Map prodcuct line codes to descriptive values
		CAST(prd_start_dt as DATE) as prd_start_dt,
		DATEADD(day, -1, CAST(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) as DATE)) as prd_start_dt -- Calculate end date as one day after the next start date
		from bronze.crm_prd_info
		SET @end_time = GETDATE()
		print ('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds')
		print ('----------------')

		SET @start_time = GETDATE()
		PRINT ('>> Truncating Table: silver.crm_sales_details <<')
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT ('>> Inserting Data Into: silver.crm_sales_details <<')
		INSERT INTO silver.crm_sales_details(
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
			CASE 
				WHEN len(sls_order_dt) != 8 or sls_order_dt = 0 then NULL
				ELSE CAST(CAST(sls_order_dt as nvarchar) as DATE)
			END as sls_order_dt,
			CASE 
				WHEN len(sls_ship_dt) != 8 or sls_ship_dt = 0 then NULL
				ELSE CAST(CAST(sls_ship_dt as nvarchar) as DATE)
			END as sls_ship_dt,
			CASE 
				WHEN len(sls_due_dt) != 8 or sls_due_dt = 0 then NULL
				ELSE CAST(CAST(sls_due_dt as nvarchar) as DATE)
			END as sls_due_dt,
			CASE
				WHEN sls_sales is NULL or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price) 
					then sls_quantity * ABS(sls_price)
				else sls_sales
			END as sls_sales,
			sls_quantity, 
			CASE
				WHEN sls_price is NULL or sls_price <=0 
					then sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
		from bronze.crm_sales_details
		SET @end_time = GETDATE()
		print ('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds')
		print ('----------------')


		PRINT ('**************************************************************');
		Print ('Loading ERP Tables');
		PRINT ('**************************************************************');

		SET @start_time = GETDATE()
		PRINT ('>> Truncating Table: silver.erp_cust_az12 <<')
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT ('>> Inserting Data Into: silver.erp_cust_az12 <<')
		INSERT INTO silver.erp_cust_az12(
			cid, 
			bdate,
			gen
		)
		select 
		CASE 
			WHEN cid like ('NAS%') then SUBSTRING(cid, 4, len(cid))
			else cid
		END as cid,
		CASE
			when bdate > getdate() then NULL
			else bdate
		END as bdate, 
		CASE
			when UPPER(TRIM(gen)) in ('F', 'FEMALE') then ('Female')
			when UPPER(TRIM(gen)) in ('M', 'MALE') then ('Male')
			else ('n/a')
		END as gen
		from bronze.erp_cust_az12
		SET @end_time = GETDATE()
		print ('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds')
		print ('----------------')

		SET @start_time = GETDATE()
		PRINT ('>> Truncating Table: silver.erp_loc_a101 <<')
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT ('>> Inserting Data Into: silver.erp_loc_a101 <<')
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)
		select 
		TRIM(REPLACE(cid, '-', '')) as cid,
		CASE
			when TRIM(cntry) in ('DE') then 'Germany'
			when TRIM(cntry) in ('US', 'USA') then 'United States'
			when TRIM(cntry) = '' or TRIM(cntry) is null then 'n/a'
			else cntry
		END as cntry
		from bronze.erp_loc_a101
		SET @end_time = GETDATE()
		print ('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds')
		print ('----------------')
		
		SET @start_time = GETDATE()
		PRINT ('>> Truncating Table: silver.erp_px_cat_g1v2 <<')
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT ('>> Inserting Data Into: silver.erp_px_cat_g1v2 <<')
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat, 
			subcat, 
			maintenance
		)
		select 
			id, 
			cat, 
			subcat, 
			maintenance
		from bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE()
		print ('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds')
		print ('----------------')

		SET @end_time = GETDATE()
		print ('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds')
		print ('----------------')

		SET @batch_end_time = GETDATE()
		PRINT ('=============================================================');
		PRINT ('Loading Silver Layer is Completed');
		PRINT ('>>>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds <<<<');
		PRINT ('=============================================================');
	END TRY
	BEGIN CATCH
		PRINT ('=============================================================');
		PRINT ('ERROR OCCURRED DURING LOADING Silver LAYER')
		PRINT ('ERROR MESSAGE: ') + ERROR_MESSAGE()
		PRINT ('ERROR NUMBER: ') + CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT ('ERROR STATE: ') + CAST(ERROR_STATE() AS NVARCHAR)
		PRINT ('=============================================================');
	END CATCH
END
