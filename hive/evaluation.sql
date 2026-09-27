-- ==============================================================================
-- evaluation.sql - Historical Holdout Recommendation Evaluation
-- Parameters to substitute:
--   __DB_NAME__           : Hive database name
--   __TABLE_NAME__        : Hive transactions table name
--   __TRAIN_BATCH_ID__    : Previous historical batch used to train rules
--   __EVAL_BATCH_ID__     : Holdout batch used for evaluation
--   __STAGING_DIR__       : Local staging directory
--   __MIN_PAIR_COUNT__    : Minimum co-purchase count threshold
--   __MIN_CONFIDENCE__    : Minimum confidence threshold
-- ==============================================================================

USE __DB_NAME__;

-- 1. Derive Trained Association Rules from Historical Batch
-- 2. Test Rules against Holdout Evaluation Batch Orders
WITH train_items AS (
    SELECT DISTINCT order_id, product_id
    FROM __TABLE_NAME__
    WHERE batch_id = '__TRAIN_BATCH_ID__'
),
train_product_counts AS (
    SELECT product_id, COUNT(DISTINCT order_id) AS orders_a
    FROM train_items
    GROUP BY product_id
),
train_pairs AS (
    SELECT 
        a.product_id AS prod_a,
        b.product_id AS prod_b,
        COUNT(DISTINCT a.order_id) AS pair_cnt
    FROM train_items a
    JOIN train_items b ON a.order_id = b.order_id AND a.product_id != b.product_id
    GROUP BY a.product_id, b.product_id
),
train_rules AS (
    SELECT 
        p.prod_a,
        p.prod_b,
        p.pair_cnt
    FROM train_pairs p
    JOIN train_product_counts c ON p.prod_a = c.product_id
    WHERE p.pair_cnt >= __MIN_PAIR_COUNT__
      AND (p.pair_cnt / c.orders_a) >= __MIN_CONFIDENCE__
),
train_rule_count AS (
    SELECT COUNT(*) AS total_rules FROM train_rules
),
eval_orders AS (
    SELECT DISTINCT order_id, product_id
    FROM __TABLE_NAME__
    WHERE batch_id = '__EVAL_BATCH_ID__'
),
test_instances AS (
    -- Every time an evaluation order contains rule antecedent prod_a, a recommendation for prod_b was tested
    SELECT 
        r.prod_a,
        r.prod_b,
        eo_a.order_id,
        CASE WHEN eo_b.product_id IS NOT NULL THEN 1 ELSE 0 END AS is_hit
    FROM train_rules r
    JOIN eval_orders eo_a ON r.prod_a = eo_a.product_id
    LEFT JOIN eval_orders eo_b ON r.prod_b = eo_b.product_id AND eo_a.order_id = eo_b.order_id
)
INSERT OVERWRITE LOCAL DIRECTORY '__STAGING_DIR__/evaluation_summary'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    rc.total_rules AS rules_evaluated,
    COUNT(t.order_id) AS recommendations_tested,
    COALESCE(SUM(t.is_hit), 0) AS hits,
    CASE 
        WHEN COUNT(t.order_id) > 0 THEN ROUND((SUM(COALESCE(t.is_hit, 0)) / COUNT(t.order_id)) * 100.0, 2)
        ELSE 0.00 
    END AS hit_rate_pct
FROM train_rule_count rc
LEFT JOIN test_instances t ON 1=1
GROUP BY rc.total_rules;
