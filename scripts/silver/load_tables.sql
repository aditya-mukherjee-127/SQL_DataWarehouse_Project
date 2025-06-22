USE Data_Warehouse;

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
    (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1) AS prd_end_dt
FROM bronze.crm_prd_info;