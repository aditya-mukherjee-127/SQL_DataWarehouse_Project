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