USE Data_Warehouse;

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