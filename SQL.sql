SELECT 
    customer_id,
    SUM(amount) AS total_sales
FROM transportation.gold.fact_orders
GROUP BY customer_id
ORDER BY total_sales DESC;

DROP TABLE IF EXISTS transportation.silver.orders;

CREATE OR REPLACE VIEW transportation.gold.final_sales_view AS
SELECT 
    -- FACT TABLE
    f.order_id,
    f.customer_id,
    f.product_id,
    f.amount,

    -- DATE DIMENSION
    d.order_date,
    d.year,
    d.month,
    d.day

FROM transportation.gold.fact_orders f

LEFT JOIN transportation.gold.dim_date d 
    ON f.date_key = d.date_key;



 SELECT * 
FROM transportation.gold.final_sales_view
LIMIT 10;
