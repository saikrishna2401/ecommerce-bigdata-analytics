-- ==============================================================================
-- recommendations.sql - Rule-Based Product Association Recommender
-- Parameters to substitute:
--   __DB_NAME__           : Hive database name
--   __TABLE_NAME__        : Hive transactions table name
--   __BATCH_ID__          : Batch/week ID
--   __STAGING_DIR__       : Local staging directory
--   __MIN_PAIR_COUNT__    : Minimum co-purchase count threshold
--   __MIN_CONFIDENCE__    : Minimum confidence threshold (0.0 - 1.0)
-- ==============================================================================

USE __DB_NAME__;

-- 1. Batch Order Metadata
INSERT OVERWRITE LOCAL DIRECTORY '__STAGING_DIR__/rec_metadata'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT product_id) AS total_products
FROM __TABLE_NAME__
WHERE batch_id = '__BATCH_ID__';

-- 2. Association Rules (Order-level Co-occurrences)
WITH order_items AS (
    SELECT DISTINCT order_id, product_id, product_name
    FROM __TABLE_NAME__
    WHERE batch_id = '__BATCH_ID__'
),
batch_totals AS (
    SELECT COUNT(DISTINCT order_id) AS total_orders
    FROM __TABLE_NAME__
    WHERE batch_id = '__BATCH_ID__'
),
product_counts AS (
    SELECT product_id, COUNT(DISTINCT order_id) AS orders_a
    FROM order_items
    GROUP BY product_id
),
pair_counts AS (
    SELECT 
        a.product_id AS prod_a,
        a.product_name AS prod_a_name,
        b.product_id AS prod_b,
        b.product_name AS prod_b_name,
        COUNT(DISTINCT a.order_id) AS pair_count
    FROM order_items a
    JOIN order_items b 
      ON a.order_id = b.order_id AND a.product_id != b.product_id
    GROUP BY a.product_id, a.product_name, b.product_id, b.product_name
)
INSERT OVERWRITE LOCAL DIRECTORY '__STAGING_DIR__/recommendations'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    p.prod_a,
    p.prod_a_name,
    p.prod_b,
    p.prod_b_name,
    p.pair_count,
    ROUND(p.pair_count / t.total_orders, 4) AS support,
    ROUND(p.pair_count / c.orders_a, 4) AS confidence
FROM pair_counts p
CROSS JOIN batch_totals t
JOIN product_counts c ON p.prod_a = c.product_id
WHERE p.pair_count >= __MIN_PAIR_COUNT__
  AND (p.pair_count / c.orders_a) >= __MIN_CONFIDENCE__
ORDER BY p.pair_count DESC, confidence DESC, p.prod_a, p.prod_b;
