CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cust_info.cst_id) AS customer_key,
    cust_info.cst_id AS customer_id,
    cust_info.cst_key AS customer_number,
    cust_info.cst_firstname AS first_name,
    cust_info.cst_lastname AS last_name,
    cust_info.cst_marital_status AS marital_status,
    CASE WHEN cust_info.cst_gndr != 'Unknown' THEN cust_info.cst_gndr 
        ELSE COALESCE(cust_bdate_gen.gen, 'Unknown')
    END AS gender,
    cust_bdate_gen.bdate AS birth_date,
    cust_loc.cntry AS country,
    cust_info.cst_create_date AS create_date
FROM silver.crm_cust_info cust_info
LEFT JOIN silver.erp_cust_az12 as cust_bdate_gen
ON cust_info.cst_key = cust_bdate_gen.cid
LEFT JOIN silver.erp_loc_a101 as cust_loc
ON cust_info.cst_key = cust_loc.cid;