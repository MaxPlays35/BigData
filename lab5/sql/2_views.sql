CREATE OR REPLACE VIEW report_top10_products_by_sales AS
SELECT
    sale_product_id,
    product_name,
    product_category,
    product_brand,
    SUM(sale_quantity)              AS total_quantity_sold,
    SUM(sale_total_price)           AS total_revenue,
    COUNT(*)                        AS number_of_sales,
    ROUND(AVG(sale_total_price), 2) AS avg_sale_amount
FROM mock_data
GROUP BY sale_product_id, product_name, product_category, product_brand
ORDER BY total_quantity_sold DESC
    LIMIT 10;

CREATE OR REPLACE VIEW report_revenue_by_category AS
SELECT
    product_category,
    SUM(sale_total_price)           AS total_revenue,
    SUM(sale_quantity)              AS total_quantity_sold,
    COUNT(*)                        AS number_of_sales,
    ROUND(AVG(sale_total_price), 2) AS avg_sale_amount,
    COUNT(DISTINCT sale_product_id) AS unique_products
FROM mock_data
GROUP BY product_category
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW report_product_ratings AS
SELECT
    sale_product_id,
    product_name,
    product_category,
    product_brand,
    ROUND(AVG(product_rating), 2) AS avg_rating,
    MAX(product_reviews)          AS total_reviews,
    SUM(sale_total_price)         AS total_revenue,
    SUM(sale_quantity)            AS total_quantity_sold
FROM mock_data
GROUP BY sale_product_id, product_name, product_category, product_brand
ORDER BY avg_rating DESC;

CREATE OR REPLACE VIEW report_top10_customers AS
SELECT
    sale_customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_country,
    customer_age,
    SUM(sale_total_price)           AS total_spent,
    COUNT(*)                        AS number_of_orders,
    ROUND(AVG(sale_total_price), 2) AS avg_order_amount,
    MIN(sale_date)                  AS first_purchase,
    MAX(sale_date)                  AS last_purchase
FROM mock_data
GROUP BY sale_customer_id, customer_first_name, customer_last_name,
         customer_email, customer_country, customer_age
ORDER BY total_spent DESC
    LIMIT 10;


CREATE OR REPLACE VIEW report_customers_by_country AS
SELECT
    customer_country,
    COUNT(DISTINCT sale_customer_id) AS unique_customers,
    SUM(sale_total_price)            AS total_revenue,
    ROUND(AVG(sale_total_price), 2)  AS avg_order_amount,
    SUM(sale_quantity)               AS total_items_bought
FROM mock_data
GROUP BY customer_country
ORDER BY unique_customers DESC;

CREATE OR REPLACE VIEW report_customer_avg_check AS
SELECT
    sale_customer_id,
    customer_first_name || ' ' || customer_last_name AS customer_name,
    customer_country,
    COUNT(*)                        AS number_of_orders,
    SUM(sale_total_price)           AS total_spent,
    ROUND(AVG(sale_total_price), 2) AS avg_check,
    ROUND(AVG(sale_quantity), 2)    AS avg_items_per_order
FROM mock_data
GROUP BY sale_customer_id, customer_first_name, customer_last_name, customer_country
ORDER BY avg_check DESC;

CREATE OR REPLACE VIEW report_monthly_sales AS
SELECT
    EXTRACT(YEAR FROM sale_date)::int  AS sale_year,
    EXTRACT(MONTH FROM sale_date)::int AS sale_month,
    TO_CHAR(sale_date, 'YYYY-MM')      AS year_month,
    SUM(sale_total_price)              AS total_revenue,
    SUM(sale_quantity)                 AS total_quantity,
    COUNT(*)                           AS number_of_sales,
    ROUND(AVG(sale_total_price), 2)    AS avg_order_amount,
    COUNT(DISTINCT sale_customer_id)   AS unique_customers
FROM mock_data
GROUP BY EXTRACT(YEAR FROM sale_date), EXTRACT(MONTH FROM sale_date),
         TO_CHAR(sale_date, 'YYYY-MM')
ORDER BY sale_year, sale_month;

CREATE OR REPLACE VIEW report_yearly_sales AS
SELECT
    sale_year,
    total_revenue,
    total_quantity,
    number_of_sales,
    avg_order_amount,
    unique_customers,
    unique_products,
    LAG(total_revenue) OVER (ORDER BY sale_year) AS prev_year_revenue,
    ROUND(
            (total_revenue - LAG(total_revenue) OVER (ORDER BY sale_year))
                / NULLIF(LAG(total_revenue) OVER (ORDER BY sale_year), 0)
                * 100, 2
    ) AS revenue_growth_pct
FROM (
         SELECT
             EXTRACT(YEAR FROM sale_date)::int AS sale_year,
             SUM(sale_total_price)             AS total_revenue,
             SUM(sale_quantity)                AS total_quantity,
             COUNT(*)                          AS number_of_sales,
             ROUND(AVG(sale_total_price), 2)   AS avg_order_amount,
             COUNT(DISTINCT sale_customer_id)  AS unique_customers,
             COUNT(DISTINCT sale_product_id)   AS unique_products
         FROM mock_data
         GROUP BY EXTRACT(YEAR FROM sale_date)
     ) sub
ORDER BY sale_year;

CREATE OR REPLACE VIEW report_avg_order_by_month AS
SELECT
    EXTRACT(MONTH FROM sale_date)::int AS month_number,
    TO_CHAR(sale_date, 'Month')        AS month_name,
    ROUND(AVG(sale_total_price), 2)    AS avg_order_amount,
    ROUND(AVG(sale_quantity), 2)       AS avg_items_per_order,
    SUM(sale_total_price)              AS total_revenue,
    COUNT(*)                           AS number_of_orders
FROM mock_data
GROUP BY EXTRACT(MONTH FROM sale_date), TO_CHAR(sale_date, 'Month')
ORDER BY month_number;

CREATE OR REPLACE VIEW report_top5_stores AS
SELECT
    store_name,
    store_city,
    store_state,
    store_country,
    SUM(sale_total_price)           AS total_revenue,
    SUM(sale_quantity)              AS total_quantity,
    COUNT(*)                        AS number_of_sales,
    ROUND(AVG(sale_total_price), 2) AS avg_sale_amount,
    COUNT(DISTINCT sale_customer_id) AS unique_customers
FROM mock_data
GROUP BY store_name, store_city, store_state, store_country
ORDER BY total_revenue DESC
    LIMIT 5;

CREATE OR REPLACE VIEW report_sales_by_store_location AS
SELECT
    store_country,
    store_city,
    COUNT(DISTINCT store_name)      AS stores_count,
    SUM(sale_total_price)           AS total_revenue,
    SUM(sale_quantity)              AS total_quantity,
    COUNT(*)                        AS number_of_sales,
    ROUND(AVG(sale_total_price), 2) AS avg_sale_amount
FROM mock_data
GROUP BY store_country, store_city
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW report_store_avg_check AS
SELECT
    store_name,
    store_city,
    store_country,
    COUNT(*)                         AS number_of_orders,
    SUM(sale_total_price)            AS total_revenue,
    ROUND(AVG(sale_total_price), 2)  AS avg_check,
    ROUND(AVG(sale_quantity), 2)     AS avg_items_per_order,
    COUNT(DISTINCT sale_customer_id) AS unique_customers
FROM mock_data
GROUP BY store_name, store_city, store_country
ORDER BY avg_check DESC;

CREATE OR REPLACE VIEW report_top5_suppliers AS
SELECT
    supplier_name,
    supplier_city,
    supplier_country,
    supplier_contact,
    SUM(sale_total_price)          AS total_revenue,
    SUM(sale_quantity)             AS total_quantity,
    COUNT(*)                       AS number_of_sales,
    COUNT(DISTINCT sale_product_id) AS unique_products
FROM mock_data
GROUP BY supplier_name, supplier_city, supplier_country, supplier_contact
ORDER BY total_revenue DESC
    LIMIT 5;


CREATE OR REPLACE VIEW report_supplier_avg_price AS
SELECT
    supplier_name,
    supplier_country,
    COUNT(DISTINCT sale_product_id) AS unique_products,
    ROUND(AVG(product_price), 2)   AS avg_product_price,
    ROUND(MIN(product_price), 2)   AS min_product_price,
    ROUND(MAX(product_price), 2)   AS max_product_price,
    SUM(sale_total_price)          AS total_revenue
FROM mock_data
GROUP BY supplier_name, supplier_country
ORDER BY avg_product_price DESC;


CREATE OR REPLACE VIEW report_sales_by_supplier_country AS
SELECT
    supplier_country,
    COUNT(DISTINCT supplier_name)   AS unique_suppliers,
    SUM(sale_total_price)           AS total_revenue,
    SUM(sale_quantity)              AS total_quantity,
    COUNT(*)                        AS number_of_sales,
    ROUND(AVG(product_price), 2)    AS avg_product_price,
    COUNT(DISTINCT sale_product_id) AS unique_products
FROM mock_data
GROUP BY supplier_country
ORDER BY total_revenue DESC;


CREATE OR REPLACE VIEW report_product_rating_extremes AS
SELECT * FROM (
                  SELECT
                      'top'::text AS rating_group,
                      sale_product_id,
                      product_name,
                      product_category,
                      product_brand,
                      ROUND(AVG(product_rating), 2) AS avg_rating,
                      MAX(product_reviews)          AS total_reviews,
                      SUM(sale_total_price)         AS total_revenue
                  FROM mock_data
                  GROUP BY sale_product_id, product_name, product_category, product_brand
                  ORDER BY avg_rating DESC
                      LIMIT 10
              ) top_rated
UNION ALL
SELECT * FROM (
                  SELECT
                      'bottom'::text AS rating_group,
                      sale_product_id,
                      product_name,
                      product_category,
                      product_brand,
                      ROUND(AVG(product_rating), 2) AS avg_rating,
                      MAX(product_reviews)          AS total_reviews,
                      SUM(sale_total_price)         AS total_revenue
                  FROM mock_data
                  GROUP BY sale_product_id, product_name, product_category, product_brand
                  ORDER BY avg_rating ASC
                      LIMIT 10
              ) bottom_rated;

CREATE OR REPLACE VIEW report_rating_vs_sales AS
SELECT
    CASE
        WHEN product_rating < 1 THEN '0.0 - 0.9'
        WHEN product_rating < 2 THEN '1.0 - 1.9'
        WHEN product_rating < 3 THEN '2.0 - 2.9'
        WHEN product_rating < 4 THEN '3.0 - 3.9'
        ELSE '4.0 - 5.0'
        END                             AS rating_bucket,
    MIN(product_rating)             AS bucket_min_rating,
    COUNT(*)                        AS number_of_sales,
    SUM(sale_quantity)              AS total_quantity,
    SUM(sale_total_price)           AS total_revenue,
    ROUND(AVG(sale_total_price), 2) AS avg_sale_amount,
    COUNT(DISTINCT sale_product_id) AS unique_products
FROM mock_data
GROUP BY
    CASE
        WHEN product_rating < 1 THEN '0.0 - 0.9'
        WHEN product_rating < 2 THEN '1.0 - 1.9'
        WHEN product_rating < 3 THEN '2.0 - 2.9'
        WHEN product_rating < 4 THEN '3.0 - 3.9'
        ELSE '4.0 - 5.0'
        END
ORDER BY rating_bucket;


CREATE OR REPLACE VIEW report_most_reviewed_products AS
SELECT
    sale_product_id,
    product_name,
    product_category,
    product_brand,
    MAX(product_reviews)          AS total_reviews,
    ROUND(AVG(product_rating), 2) AS avg_rating,
    SUM(sale_total_price)         AS total_revenue,
    SUM(sale_quantity)            AS total_quantity_sold
FROM mock_data
GROUP BY sale_product_id, product_name, product_category, product_brand
ORDER BY total_reviews DESC
    LIMIT 20;