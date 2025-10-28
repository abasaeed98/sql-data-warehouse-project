/*
a stored procedure to load the data from flat files into the bronze schema using the truncate and bulk insert method.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_bronze_time DATETIME, @end_bronze_time DATETIME; 
	BEGIN TRY
	    SET @start_bronze_time = GETDATE()
		PRINT '===============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===============================================';

		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\mobar\Desktop\learning\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>> --------------------------------------------'

		PRINT '>> Trancating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		SET @start_time = GETDATE()
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\mobar\Desktop\learning\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>> --------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\mobar\Desktop\learning\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>> --------------------------------------------'

		PRINT '-----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\mobar\Desktop\learning\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>> --------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\mobar\Desktop\learning\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>> --------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\mobar\Desktop\learning\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>> --------------------------------------------'

		SET @end_bronze_time = GETDATE()
		PRINT '>> Bronze Layer Load Duration: ' + CAST(DATEDIFF(second, @start_bronze_time, @end_bronze_time) AS NVARCHAR) + ' seconds'
		PRINT '>> --------------------------------------------'

	END TRY
	BEGIN CATCH
		PRINT '===============================================';
		PRINT 'Error Occurred During Loading Bronze Layer';
		PRINT 'Error Message' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===============================================';
	END CATCH
END
