
EXEC bronze.load_bronze; 

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		PRINT '===============================';
		PRINT 'Loading Bronze Layer';
		PRINT '===============================';
		-- To load the data in bulk: Full Load
		-- Truncate is used so that when any user runs the below mentioned query, data does not gets loaded twice.
		PRINT 'Loading CRM Tables';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Prashant\Data with Bara\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- FIRSTROW indicates that the actual data starts from the second row in the table
			FIELDTERMINATOR = ',', -- This is the delimiter inside the table.
			TABLOCK -- Locks the table when it is inserting the data.
			);
		SET @end_time = GETDATE();
		PRINT  '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'


		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Prashant\Data with Bara\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, -- FIRSTROW indicates that the actual data starts from the second row in the table
			FIELDTERMINATOR = ',', -- This is the delimiter inside the table.
			TABLOCK -- Locks the table when it is inserting the data.
			);
		SET @end_time = GETDATE();
		PRINT  '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'
	
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Prashant\Data with Bara\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, -- FIRSTROW indicates that the actual data starts from the second row in the table
			FIELDTERMINATOR = ',', -- This is the delimiter inside the table.
			TABLOCK -- Locks the table when it is inserting the data.
			);
		SET @end_time = GETDATE();
		PRINT  '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'

		PRINT '==========================='
		PRINT 'Loading ERP tables'
		PRINT '==========================='

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_CUST_AZ12
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Prashant\Data with Bara\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, -- FIRSTROW indicates that the actual data starts from the second row in the table
			FIELDTERMINATOR = ',', -- This is the delimiter inside the table.
			TABLOCK -- Locks the table when it is inserting the data.
			);
		SET @end_time = GETDATE();
		PRINT  '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_LOC_A101
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Prashant\Data with Bara\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, -- FIRSTROW indicates that the actual data starts from the second row in the table
			FIELDTERMINATOR = ',', -- This is the delimiter inside the table.
			TABLOCK -- Locks the table when it is inserting the data.
			);
		SET @end_time = GETDATE();
		PRINT  '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Prashant\Data with Bara\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, -- FIRSTROW indicates that the actual data starts from the second row in the table
			FIELDTERMINATOR = ',', -- This is the delimiter inside the table.
			TABLOCK -- Locks the table when it is inserting the data.
			);
		SET @end_time = GETDATE();
		PRINT  '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'

		END TRY
		BEGIN CATCH
			PRINT '============================'
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE(); 
			PRINT 'Error Message' + CAST (ERROR_NUMBER() as NVARCHAR); 
			PRINT 'Error Message' + CAST (ERROR_STATE() as NVARCHAR); 
			PRINT '============================'
		END CATCH
END





