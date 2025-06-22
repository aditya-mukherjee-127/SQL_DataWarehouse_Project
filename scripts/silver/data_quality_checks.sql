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