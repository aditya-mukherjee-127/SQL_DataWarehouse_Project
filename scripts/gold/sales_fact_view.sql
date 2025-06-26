CREATE VIEW gold.fact_sales AS
SELECT
    sales_details.sls_ord_num AS order_number,
    products.product_key AS product_key,
    customers.customer_key AS customer_key,
    sales_details.sls_order_dt AS order_date,
    sales_details.sls_ship_dt AS shipping_date,
    sales_details.sls_due_dt AS due_date,
    sales_details.sls_sales AS sales,
    sales_details.sls_quantity AS quantity,
    sales_details.sls_price AS price
FROM silver.crm_sales_details sales_details
LEFT JOIN gold.dim_customers customers
ON sales_details.sls_cust_id = customers.customer_id
LEFT JOIN gold.dim_products products
ON sales_details.sls_prd_key = products.product_number;