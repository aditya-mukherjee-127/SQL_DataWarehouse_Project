CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
BEGIN TRY
    PRINT 'Loading data into bronze tables...';




    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating bronze.crm_cust_info table',
        'Started',
        GETDATE()
    );
    TRUNCATE TABLE bronze.crm_cust_info;
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating bronze.crm_cust_info table',
        'Completed',
        GETDATE()
    );
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to bronze.crm_cust_info table',
        'Started',
        GETDATE()
    );
    BULK INSERT bronze.crm_cust_info
    FROM 'D:\Data Engineering\SQL Datawarehouse Project\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to bronze.crm_cust_info table',
        'Completed',
        GETDATE()
    );







    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating bronze.crm_prd_info table',
        'Started',
        GETDATE()
    );
    TRUNCATE TABLE bronze.crm_prd_info;
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating bronze.crm_prd_info table',
        'Completed',
        GETDATE()
    );
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to bronze.crm_prd_info table',
        'Started',
        GETDATE()
    );
    BULK INSERT bronze.crm_prd_info
    FROM 'D:\Data Engineering\SQL Datawarehouse Project\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to bronze.crm_prd_info table',
        'Completed',
        GETDATE()
    );







    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating bronze.crm_sales_details table',
        'Started',
        GETDATE()
    );
    TRUNCATE TABLE bronze.crm_sales_details;
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating bronze.crm_sales_details table',
        'Completed',
        GETDATE()
    );
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to bronze.crm_sales_details table',
        'Started',
        GETDATE()
    );
    BULK INSERT bronze.crm_sales_details
    FROM 'D:\Data Engineering\SQL Datawarehouse Project\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to bronze.crm_sales_details table',
        'Completed',
        GETDATE()
    );







    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating bronze.erp_cust_az12 table',
        'Started',
        GETDATE()
    );
    TRUNCATE TABLE bronze.erp_cust_az12;
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating bronze.erp_cust_az12 table',
        'Completed',
        GETDATE()
    );
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to bronze.erp_cust_az12 table',
        'Started',
        GETDATE()
    );
    BULK INSERT bronze.erp_cust_az12
    FROM 'D:\Data Engineering\SQL Datawarehouse Project\datasets\source_erp\cust_az12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to bronze.erp_cust_az12 table',
        'Completed',
        GETDATE()
    );







    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating bronze.erp_loc_a101 table',
        'Started',
        GETDATE()
    );
    TRUNCATE TABLE bronze.erp_loc_a101;
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating bronze.erp_loc_a101 table',
        'Completed',
        GETDATE()
    );
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to bronze.erp_loc_a101 table',
        'Started',
        GETDATE()
    );
    BULK INSERT bronze.erp_loc_a101
    FROM 'D:\Data Engineering\SQL Datawarehouse Project\datasets\source_erp\loc_a101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to bronze.erp_loc_a101 table',
        'Completed',
        GETDATE()
    );








    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating bronze.erp_px_cat_g1v2 table',
        'Started',
        GETDATE()
    );
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating bronze.erp_px_cat_g1v2 table',
        'Completed',
        GETDATE()
    );
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to bronze.erp_px_cat_g1v2 table',
        'Started',
        GETDATE()
    );
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'D:\Data Engineering\SQL Datawarehouse Project\datasets\source_erp\px_cat_g1v2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to bronze.erp_px_cat_g1v2 table',
        'Completed',
        GETDATE()
    );


    PRINT 'Data loading into bronze tables completed successfully.';
END TRY
BEGIN CATCH
    PRINT 'An error occurred while loading data into bronze tables.';
    INSERT INTO logs.bronze_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Error during data load',
        ERROR_MESSAGE(),
        GETDATE()
    );
END CATCH
END