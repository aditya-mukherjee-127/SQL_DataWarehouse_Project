USE Data_Warehouse;

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(100),
    cst_firstname NVARCHAR(100),
    cst_lastname NVARCHAR(100),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
    dwh_created_at DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id NVARCHAR(100),
    prd_key NVARCHAR(100),
    prd_nm NVARCHAR(100),
    prd_cost FLOAT,
    prd_line NVARCHAR(10),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_created_at DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(100),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales FLOAT,
    sls_quantity INT,
    sls_price FLOAT,
    dwh_created_at DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid NVARCHAR(100),
    bdate DATE,
    gen NVARCHAR(50),
    dwh_created_at DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid NVARCHAR(100),
    cntry NVARCHAR(100),
    dwh_created_at DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id NVARCHAR(20),
    cat NVARCHAR(100),
    subcat NVARCHAR(100),
    maintenance NVARCHAR(100),
    dwh_created_at DATETIME DEFAULT GETDATE()
);