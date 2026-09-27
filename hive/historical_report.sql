-- ==============================================================================
-- historical_report.sql - Generate Multi-Week Longitudinal Analytics
-- Parameters to substitute:
--   __DB_NAME__      : Hive database name
--   __TABLE_NAME__   : Hive transactions table name
--   __STAGING_DIR__  : Local staging directory for output parts
-- ==============================================================================

USE __DB_NAME__;

-- 1. Historical Weekly Trend (Chronological by batch_id)
INSERT OVERWRITE LOCAL DIRECTORY '__STAGING_DIR__/historical_weekly'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    batch_id,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT customer_id) AS customers,
    SUM(quantity) AS quantity,
    ROUND(SUM(total_amount), 2) AS sales,
    ROUND(SUM(total_amount) / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM __TABLE_NAME__
GROUP BY batch_id
ORDER BY batch_id ASC;

-- 2. Historical Category Trend across Batches
INSERT OVERWRITE LOCAL DIRECTORY '__STAGING_DIR__/historical_category'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    batch_id,
    category,
    SUM(quantity) AS quantity,
    ROUND(SUM(total_amount), 2) AS sales
FROM __TABLE_NAME__
GROUP BY batch_id, category
ORDER BY batch_id ASC, sales DESC;

-- 3. Historical Overall Stack Totals
INSERT OVERWRITE LOCAL DIRECTORY '__STAGING_DIR__/historical_overall'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    COUNT(DISTINCT batch_id) AS total_batches,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS total_customers,
    COUNT(DISTINCT product_id) AS total_products,
    SUM(quantity) AS total_quantity,
    ROUND(SUM(total_amount), 2) AS total_sales,
    ROUND(SUM(total_amount) / COUNT(DISTINCT order_id), 2) AS overall_aov
FROM __TABLE_NAME__;
