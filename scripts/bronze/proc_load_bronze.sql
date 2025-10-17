/*
Loads data into the bronze schema from external CSV files

Usage Example:
    EXEC bronze.load_bronze;


*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
  DECLARE @start_time AS DATETIME, @end_time AS DATETIME, @batch_start_time AS DATETIME, @batch_end_time AS DATETIME;
    BEGIN TRY

       SET @batch_start_time = GETDATE();
        ---------------------------
        --- INSERTING THE DATA ---
        ---------------------------
        PRINT '======================================';
        PRINT 'BEGIN Loading Bronze Layer';
        PRINT '======================================';

        PRINT '======================================';
        PRINT 'BEGIN Loading CRM TABLES';
        PRINT '======================================';
      -- DECLARE @start_time AS DATETIME, @end_time AS DATETIME --for testing
        SET @start_time = GETDATE();
        -- bulk inserting crm customer info into bronze
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:.....\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            TABLOCK  
        );
        SET @end_time = GETDATE();
        PRINT CAST((@end_time - @start_time) AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '======================================';
       --DECLARE @start_time AS DATETIME, @end_time AS DATETIME --for testing
        SET @start_time = GETDATE();
        -- bulk instering bronze product info
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:.....\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT CAST((@end_time - @start_time) AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '======================================';
       --DECLARE @start_time AS DATETIME, @end_time AS DATETIME --for testing 
        SET @start_time = GETDATE();
        --bulk inserting customer sales details
        TRUNCATE TABLE bronze.crm_sales_details;
   
        BULK INSERT bronze.crm_sales_details
        FROM 'C:.....\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT CAST((@end_time - @start_time) AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '======================================';

        PRINT '======================================';
        PRINT 'BEGIN Loading ERP TABLES';
        PRINT '======================================';
      -- DECLARE @start_time AS DATETIME, @end_time AS DATETIME --for testing
        SET @start_time = GETDATE();
        --bulk insert cus_az12
        TRUNCATE TABLE bronze.erp_cust_az12

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:.....\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCk
        );
        SET @end_time = GETDATE();
        PRINT CAST((@end_time - @start_time) AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '======================================';

       --DECLARE @start_time AS DATETIME, @end_time AS DATETIME --for testing
        SET @start_time = GETDATE();
        -- bulk insert loc_a101
        TRUNCATE TABLE bronze.erp_loc_a101

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:.....\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT CAST((@end_time - @start_time) AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '======================================';

      --DECLARE @start_time AS DATETIME, @end_time AS DATETIME --for testing
        SET @start_time = GETDATE();
        --bulk insert values into px_cat_g1v2
        TRUNCATE TABLE bronze.erp_px_cat_g1v2
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:.....\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT CAST((@end_time - @start_time) AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '======================================';

        SET @batch_end_time = GETDATE();
        PRINT 'Batch Total Time: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.'
        END TRY
        -- ERROR HANDELING MESSAGING
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message' + ERROR_MESSAGE()
        PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';

    END CATCH
END

GO


-- SELECT * FROM bronze.crm_cust_info
-- SELECT * FROM bronze.crm_prd_info
-- SELECT * FROM bronze.crm_sales_details
-- SELECT * FROM bronze.erp_cust_az12
-- SELECT * FROM bronze.erp_loc_a101
-- SELECT * FROM bronze.erp_px_cat_g1v2
