-- ==============================================================================
-- validation.sql - Hive Verification & Summary Queries
-- Preparation for Block 3 Analytics and Reporting
-- ==============================================================================

USE project2;

-- 1. Total records in table
SELECT COUNT(*) AS total_records FROM ecommerce_transactions;

-- 2. Total quantity sold
SELECT SUM(quantity) AS total_quantity FROM ecommerce_transactions;

-- 3. Total sales amount
SELECT ROUND(SUM(total_amount), 2) AS total_sales FROM ecommerce_transactions;

-- 4. Number of unique customers
SELECT COUNT(DISTINCT customer_id) AS unique_customers FROM ecommerce_transactions;

-- 5. Number of unique products
SELECT COUNT(DISTINCT product_id) AS unique_products FROM ecommerce_transactions;

-- 6. Order status distribution
SELECT order_status, COUNT(*) AS status_count 
FROM ecommerce_transactions 
GROUP BY order_status 
ORDER BY status_count DESC;

-- 7. Category distribution
SELECT category, COUNT(*) AS category_records, ROUND(SUM(total_amount), 2) AS category_revenue 
FROM ecommerce_transactions 
GROUP BY category 
ORDER BY category_revenue DESC;

-- 8. Duplicate transactions verification (must return 0)
SELECT COUNT(*) AS duplicate_transaction_count
FROM (
    SELECT transaction_id
    FROM ecommerce_transactions
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
) d;
