-- ==============================================================================
-- setup_table.sql - Hive Database and Table Setup
-- E-Commerce Customer Purchase Analytics
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS project2;

USE project2;

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_transactions (
    order_id STRING,
    transaction_id STRING,
    customer_id STRING,
    transaction_date STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    quantity INT,
    unit_price DOUBLE,
    total_amount DOUBLE,
    payment_method STRING,
    customer_age INT,
    customer_gender STRING,
    city STRING,
    order_status STRING,
    rating DOUBLE
)
PARTITIONED BY (batch_id STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
