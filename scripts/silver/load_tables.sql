CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
BEGIN TRY
    PRINT 'Loading data into silver tables...';

    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating silver.crm_cust_info table',
        'Started',
        GETDATE()
    );
    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating silver.crm_cust_info table',
        'Completed',
        GETDATE()
    );
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to silver.crm_cust_info table',
        'Started',
        GETDATE()
    );
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) as cst_firstname,
        TRIM(cst_lastname) as cst_lastname,
        CASE WHEN cst_marital_status = 'S' THEN 'Single'
            WHEN cst_marital_status = 'M' THEN 'Married'
            ELSE 'Unknown'
        END AS cst_marital_status,
        CASE WHEN cst_gndr = 'F' THEN 'Female'
            WHEN cst_gndr = 'M' THEN 'Male'
            ELSE 'Unknown'
        END AS cst_gndr,
        cst_create_date
    FROM
        (
            SELECT
                *,
                DENSE_RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rank
            FROM bronze.crm_cust_info
        ) AS ranked_cust_info
    WHERE rank = 1 AND cst_id IS NOT NULL;
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to silver.crm_cust_info table',
        'Completed',
        GETDATE()
    );





    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating silver.crm_prd_info table',
        'Started',
        GETDATE()
    );
    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating silver.crm_prd_info table',
        'Completed',
        GETDATE()
    );
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to silver.crm_prd_info table',
        'Started',
        GETDATE()
    );
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
        TRIM(prd_nm) AS prd_nm,
        ISNULL(prd_cost, 0) AS prd_cost,
        prd_line,
        prd_start_dt,
        -- To deal with the situation where prd_end_dt < prd_start_dt in raw data, there was a way to just swap the values,
        -- but in such case there waould be overlap of data, implies that for a particular product with overlapping date range,
        -- the prd_cost would be different, which is not at all acceptable.
        -- Hence, prd_end_dt is calculated as the next prd_start_dt minus one day.
        -- This ensures that the end date is always after the next start date and avoids overlaps and also fits the situation. 
        (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1) AS prd_end_dt
    FROM bronze.crm_prd_info;
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to silver.crm_prd_info table',
        'Completed',
        GETDATE()
    );





    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating silver.crm_sales_details table',
        'Started',
        GETDATE()
    );
    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating silver.crm_sales_details table',
        'Completed',
        GETDATE()
    );
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to silver.crm_sales_details table',
        'Started',
        GETDATE()
    );
    INSERT INTO silver.crm_sales_details (
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
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt IS NULL OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt IS NULL OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt IS NULL OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != (sls_quantity * ABS(sls_price))
            THEN (sls_quantity * ABS(sls_price))
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE WHEN sls_price <= 0 THEN ABS(sls_price)
            WHEN sls_price IS NULL THEN (sls_sales/NULLIF(sls_quantity, 0))
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to silver.crm_sales_details table',
        'Completed',
        GETDATE()
    );





    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating silver.erp_cust_az12 table',
        'Started',
        GETDATE()
    );
    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating silver.erp_cust_az12 table',
        'Completed',
        GETDATE()
    );
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to silver.erp_cust_az12 table',
        'Started',
        GETDATE()
    );
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        REPLACE(cid, 'NAS', '') AS cid,
        CASE WHEN bdate IS NULL OR bdate = '' OR bdate > GETDATE() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE WHEN gen IS NULL OR LEN(gen) = 0 THEN 'Unknown'
            WHEN gen = 'F' THEN 'Female'
            WHEN gen = 'M' THEN 'Male'
            ELSE gen
        END AS gen
    FROM bronze.erp_cust_az12;
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to silver.erp_cust_az12 table',
        'Completed',
        GETDATE()
    );





    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating silver.erp_loc_a101 table',
        'Started',
        GETDATE()
    );
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating silver.erp_loc_a101 table',
        'Completed',
        GETDATE()
    );
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to silver.erp_loc_a101 table',
        'Started',
        GETDATE()
    );
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT 
        REPLACE(cid, '-', '') AS cid,
        CASE WHEN cntry IN ('USA', 'US', 'United States') THEN 'United States'
            WHEN cntry IN ('UK', 'United Kingdom') THEN 'United Kingdom'
            WHEN cntry IS NULL OR LEN(cntry) = 0 THEN 'Unknown'
            ELSE cntry
        END AS cntry
    FROM bronze.erp_loc_a101;
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to silver.erp_loc_a101 table',
        'Completed',
        GETDATE()
    );





    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating silver.erp_px_cat_g1v2 table',
        'Started',
        GETDATE()
    );
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Truncating silver.erp_px_cat_g1v2 table',
        'Completed',
        GETDATE()
    );
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to silver.erp_px_cat_g1v2 table',
        'Started',
        GETDATE()
    );
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Loading Data to silver.erp_px_cat_g1v2 table',
        'Completed',
        GETDATE()
    );


    PRINT 'Data loading into silver tables completed successfully.';
END TRY
BEGIN CATCH
    INSERT INTO logs.silver_logs VALUES (
        DATEDIFF_BIG(MILLISECOND, '1970-01-01 00:00:00', SYSDATETIME()),
        'Error occurred while loading data into silver tables',
        ERROR_MESSAGE(),
        GETDATE()
    );
    PRINT 'An error occurred while loading data into silver tables. Check logs for details.';
END CATCH
END;