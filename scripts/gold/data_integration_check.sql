SELECT * FROM gold.fact_sales sales
LEFT JOIN gold.dim_customers customers
ON sales.customer_key = customers.customer_key
WHERE customers.customer_key IS NULL;
-- Must return empty dataset

SELECT * FROM gold.fact_sales sales
LEFT JOIN gold.dim_customers customers
ON sales.customer_key = customers.customer_key
LEFT JOIN gold.dim_products products
ON sales.product_key = products.product_key
WHERE products.product_key IS NULL;
-- Must return empty dataset

