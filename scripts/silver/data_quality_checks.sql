USE Data_Warehouse;

-- Quality check for silver.crm_cust_info
SELECT TOP 100 * FROM silver.crm_cust_info;

SELECT
    cst_id,
    cst_key,
    COUNT(cst_id)
FROM silver.crm_cust_info
GROUP BY cst_id, cst_key
HAVING cst_id IS NULL OR count(cst_id) > 1;

SELECT *
FROM silver.crm_cust_info
WHERE cst_id IN (
    SELECT cst_id
    FROM silver.crm_cust_info
    GROUP BY cst_id, cst_key
    HAVING COUNT(cst_id) > 1
);

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT 
    cst_marital_status,
    COUNT(*) AS marital_status_count
FROM silver.crm_cust_info
GROUP BY cst_marital_status;

SELECT 
    cst_gndr,
    COUNT(*) AS gender_count
FROM silver.crm_cust_info
GROUP BY cst_gndr;


-- Quality check for silver.crm_prd_info
SELECT TOP 100 * FROM silver.crm_prd_info;

SELECT
    prd_id,
    COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;

SELECT
    prd_id,
    prd_key
FROM silver.crm_prd_info
WHERE prd_key IS NULL;

SELECT
    prd_id,
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

SELECT
    *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt OR prd_start_dt IS NULL;



-- Quality check for silver.crm_sales_details
SELECT TOP 100 * FROM silver.crm_sales_details;

SELECT
    sls_ord_num,
    COUNT(*)
FROM silver.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*) > 1;

SELECT
    sls_prd_key,
    COUNT(*)
FROM silver.crm_sales_details
GROUP BY sls_prd_key
HAVING COUNT(*) > 1;

SELECT
    sls_cust_id,
    COUNT(*)
FROM silver.crm_sales_details
GROUP BY sls_cust_id
HAVING sls_cust_id IS NULL;

SELECT
    sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL OR LEN(sls_order_dt) != 8;

SELECT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE
    sls_sales <= 0 OR 
    sls_quantity <= 0 OR
    sls_price <= 0 OR
    sls_sales IS NULL OR
    sls_quantity IS NULL OR
    sls_price IS NULL OR
    sls_sales != sls_quantity * sls_price;






-- Qualtity check for silver.erp_cust_az12
SELECT TOP 100 * FROM silver.erp_cust_az12;

SELECT bdate FROM silver.erp_cust_az12
WHERE bdate IS NULL OR bdate = '' OR bdate > GETDATE();

SELECT DISTINCT gen FROM silver.erp_cust_az12;






-- Quality check for silver.erp_loc_a101
SELECT TOP 100 * FROM silver.erp_loc_a101;

SELECT DISTINCT cntry FROM bronze.erp_loc_a101;