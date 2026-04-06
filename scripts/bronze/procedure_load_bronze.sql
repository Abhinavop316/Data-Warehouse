-- =============================================
-- Stored Procedure: bronze.load_bronze
-- Purpose:
-- This procedure loads raw data into the Bronze layer
-- of a data warehouse from CSV files using BULK INSERT.
-- It follows ETL (Extract → Load) pattern for staging data.
-- =============================================

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
    -- Declare variables to track execution time
    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @start_time_batch DATETIME, @end_time_batch DATETIME;

    BEGIN TRY

        -- Capture overall batch start time
        SET @start_time_batch = GETDATE();

        PRINT '=============================================================';
        PRINT 'Loading Bronze Layer (Raw Data Ingestion)';
        PRINT '=============================================================';

        -- =============================================================
        -- SECTION 1: LOAD CRM SOURCE TABLES
        -- =============================================================

        PRINT '-------------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-------------------------------------------------------------';

        -- =============================================================
        -- Table: bronze.crm_cust_info
        -- =============================================================

        SET @start_time = GETDATE();

        -- Remove old data (full refresh strategy)
        PRINT 'Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        -- Load fresh data from CSV
        PRINT 'Inserting data into: bronze.crm_cust_info';

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\abhin\OneDrive\Desktop\project\data warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,              -- Skip header row
            FIELDTERMINATOR = ',',     -- CSV delimiter
            TABLOCK                    -- Improves performance using bulk locking
        );

        SET @end_time = GETDATE();

        -- Log execution time
        PRINT 'Load duration for bronze.crm_cust_info: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- =============================================================
        -- Table: bronze.crm_prd_info
        -- =============================================================

        SET @start_time = GETDATE();

        PRINT 'Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT 'Inserting data into: bronze.crm_prd_info';

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\abhin\OneDrive\Desktop\project\data warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT 'Load duration for bronze.crm_prd_info: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- =============================================================
        -- Table: bronze.crm_sales_details
        -- =============================================================

        SET @start_time = GETDATE();

        PRINT 'Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT 'Inserting data into: bronze.crm_sales_details';

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\abhin\OneDrive\Desktop\project\data warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT 'Load duration for bronze.crm_sales_details: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- =============================================================
        -- SECTION 2: LOAD ERP SOURCE TABLES
        -- =============================================================

        PRINT '-------------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-------------------------------------------------------------';


        -- =============================================================
        -- Table: bronze.erp_cust_az12
        -- =============================================================

        SET @start_time = GETDATE();

        PRINT 'Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT 'Inserting data into: bronze.erp_cust_az12';

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\abhin\OneDrive\Desktop\project\data warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT 'Load duration for bronze.erp_cust_az12: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- =============================================================
        -- Table: bronze.erp_loc_a101
        -- =============================================================

        SET @start_time = GETDATE();

        PRINT 'Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT 'Inserting data into: bronze.erp_loc_a101';

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\abhin\OneDrive\Desktop\project\data warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT 'Load duration for bronze.erp_loc_a101: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- =============================================================
        -- Table: bronze.erp_px_cat_g1v2
        -- =============================================================

        SET @start_time = GETDATE();

        PRINT 'Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT 'Inserting data into: bronze.erp_px_cat_g1v2';

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\abhin\OneDrive\Desktop\project\data warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT 'Load duration for bronze.erp_px_cat_g1v2: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- =============================================================
        -- Batch Completion
        -- =============================================================

        SET @end_time_batch = GETDATE();

        PRINT 'Total batch load time: ' 
              + CAST(DATEDIFF(SECOND, @start_time_batch, @end_time_batch) AS NVARCHAR) + ' seconds';


    END TRY

    -- =============================================================
    -- ERROR HANDLING BLOCK
    -- =============================================================
    BEGIN CATCH

        PRINT '-------------------------------------------------------------';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';

        -- Print detailed error info
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);

        PRINT '-------------------------------------------------------------';

    END CATCH
END
