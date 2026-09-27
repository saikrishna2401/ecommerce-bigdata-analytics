-- ==============================================================================
-- weekly_report.sql - Generate Weekly Analytics Datasets
-- Parameters to substitute:
--   __DB_NAME__      : Hive database name
--   __TABLE_NAME__   : Hive transactions table name
--   __BATCH_ID__     : Batch/week ID (e.g. 2026-W05)
--   __STAGING_DIR__  : Local staging directory for output parts
-- ==============================================================================

USE __DB_NAME__;

-- 1. Sales & Transaction Overview
INSERT OVERWRITE LOCAL DIRECTORY '__STAGING_DIR__/sales_overview'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    COUNT(*) AS total_records,
    SUM(quantity) AS total_quantity,
    ROUND(SUM(total_amount), 2) AS total_sales,
    COUNT(DISTINCT order_id) AS distinct_orders,
    COUNT(DISTINCT customer_id) AS distinct_customers,
    COUNT(DISTINCT product_id) AS distinct_products,
    ROUND(SUM(total_amount) / COUNT(DISTINCT order_id), 2) AS avg_order_value,
    COUNT(transaction_id) - COUNT(DISTINCT transaction_id) AS duplicate_tx_count
FROM __TABLE_NAME__
WHERE batch_id = '__BATCH_ID__';

-- 2. Category Summary (Sorted by revenue DESC)
INSERT OVERWRITE LOCAL DIRECTORY '__STAGING_DIR__/category_summary'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    category,
    SUM(quantity) AS quantity,
    ROUND(SUM(total_amount), 2) AS sales,
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(*) AS tx_count
FROM __TABLE_NAME__
WHERE batch_id = '__BATCH_ID__'
GROUP BY category
ORDER BY sales DESC, category ASC;

-- 3. Product Summary (Sorted by revenue DESC)
INSERT OVERWRITE LOCAL DIRECTORY '__STAGING_DIR__/product_summary'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    product_id,
    product_name,
    category,
    SUM(quantity) AS quantity_sold,
    ROUND(SUM(total_amount), 2) AS sales,
    COUNT(DISTINCT order_id) AS order_count
FROM __TABLE_NAME__
WHERE batch_id = '__BATCH_ID__'
GROUP BY product_id, product_name, category
ORDER BY sales DESC, quantity_sold DESC, product_id ASC;

-- 4. Payment Method Summary
INSERT OVERWRITE LOCAL DIRECTORY '__STAGING_DIR__/payment_summary'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    payment_method,
    COUNT(*) AS tx_count,
    ROUND(SUM(total_amount), 2) AS sales
FROM __TABLE_NAME__
WHERE batch_id = '__BATCH_ID__'
GROUP BY payment_method
ORDER BY sales DESC, payment_method ASC;

-- 5. Order Status Distribution & Revenue Audit
INSERT OVERWRITE LOCAL DIRECTORY '__STAGING_DIR__/order_status_summary'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    order_status,
    COUNT(*) AS status_count,
    COUNT(DISTINCT order_id) AS order_count,
    ROUND(SUM(total_amount), 2) AS sales
FROM __TABLE_NAME__
WHERE batch_id = '__BATCH_ID__'
GROUP BY order_status
ORDER BY status_count DESC, order_status ASC;

-- 6. Customer Summary (Top customers by sales)
INSERT OVERWRITE LOCAL DIRECTORY '__STAGING_DIR__/customer_summary'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    customer_id,
    COUNT(DISTINCT order_id) AS orders,
    SUM(quantity) AS quantity,
    ROUND(SUM(total_amount), 2) AS sales,
    city
FROM __TABLE_NAME__
WHERE batch_id = '__BATCH_ID__'
GROUP BY customer_id, city
ORDER BY sales DESC, orders DESC, customer_id ASC;
