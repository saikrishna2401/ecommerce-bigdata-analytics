-- ==============================================================================
-- clean_transactions.pig
-- Apache Pig script for E-Commerce Transaction Data Cleaning and Validation
-- Execution: Pig on Apache Tez (YARN)
-- ==============================================================================

-- Register Piggybank for RFC-4180 compliant CSV parsing
REGISTER /home/hdp/apache-pig-0.18.0/lib/piggybank.jar;

-- Load raw CSV transactions using RFC-4180 compliant CSVExcelStorage
-- Supports embedded commas, quoted fields, unquoted fields, and skips input header
raw_input = LOAD '$INPUT_PATH' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (
    raw_order_id:chararray,
    raw_transaction_id:chararray,
    raw_customer_id:chararray,
    raw_transaction_date:chararray,
    raw_product_id:chararray,
    raw_product_name:chararray,
    raw_category:chararray,
    raw_quantity:chararray,
    raw_unit_price:chararray,
    raw_total_amount:chararray,
    raw_payment_method:chararray,
    raw_customer_age:chararray,
    raw_customer_gender:chararray,
    raw_city:chararray,
    raw_order_status:chararray,
    raw_rating:chararray
);

-- Defensive filter for any duplicate/concatenated CSV header rows across multi-part inputs
-- Requires matching multiple header column names so legitimate transactions with order_id='order_id' are never dropped
records = FILTER raw_input BY NOT (
    (raw_order_id IS NOT NULL) AND (raw_transaction_id IS NOT NULL) AND
    (LOWER(TRIM(raw_order_id)) == 'order_id') AND
    (LOWER(TRIM(raw_transaction_id)) == 'transaction_id')
);

-- Trim basic text fields and normalize surrounding double quotes from numeric fields before validation
trimmed = FOREACH records GENERATE
    (raw_order_id IS NULL ? '' : TRIM(raw_order_id)) AS order_id:chararray,
    (raw_transaction_id IS NULL ? '' : TRIM(raw_transaction_id)) AS transaction_id:chararray,
    (raw_customer_id IS NULL ? '' : TRIM(raw_customer_id)) AS customer_id:chararray,
    (raw_transaction_date IS NULL ? '' : TRIM(raw_transaction_date)) AS transaction_date:chararray,
    (raw_product_id IS NULL ? '' : TRIM(raw_product_id)) AS product_id:chararray,
    (raw_product_name IS NULL ? '' : TRIM(raw_product_name)) AS product_name:chararray,
    (raw_category IS NULL ? '' : TRIM(raw_category)) AS category:chararray,
    (raw_quantity IS NULL ? '' : TRIM(REPLACE(TRIM(raw_quantity), '^"+|"+$', ''))) AS quantity_str:chararray,
    (raw_unit_price IS NULL ? '' : TRIM(REPLACE(TRIM(raw_unit_price), '^"+|"+$', ''))) AS unit_price_str:chararray,
    (raw_total_amount IS NULL ? '' : TRIM(REPLACE(TRIM(raw_total_amount), '^"+|"+$', ''))) AS total_amount_str:chararray,
    (raw_payment_method IS NULL ? '' : TRIM(raw_payment_method)) AS payment_method:chararray,
    (raw_customer_age IS NULL ? '' : TRIM(REPLACE(TRIM(raw_customer_age), '^"+|"+$', ''))) AS customer_age_str:chararray,
    (raw_customer_gender IS NULL ? '' : TRIM(raw_customer_gender)) AS customer_gender:chararray,
    (raw_city IS NULL ? '' : TRIM(raw_city)) AS city:chararray,
    (raw_order_status IS NULL ? '' : TRIM(raw_order_status)) AS order_status:chararray,
    (raw_rating IS NULL ? '' : TRIM(REPLACE(TRIM(raw_rating), '^"+|"+$', ''))) AS rating_str:chararray;

-- Split records into valid and rejected sets based on data quality rules:
-- 1. Essential identifier fields must not be empty
-- 2. Quantity must be a valid positive integer
-- 3. Unit price and Total amount must be valid positive numeric values
-- 4. Transaction total consistency: total_amount must match quantity * unit_price within 0.01 currency tolerance
SPLIT trimmed INTO
    valid_raw IF (
        order_id != '' AND
        transaction_id != '' AND
        customer_id != '' AND
        product_id != '' AND
        transaction_date != '' AND
        (quantity_str matches '^[0-9]+$') AND ((int)quantity_str > 0) AND
        (unit_price_str matches '^[0-9]+(\\.[0-9]+)?$') AND ((double)unit_price_str > 0.0) AND
        (total_amount_str matches '^[0-9]+(\\.[0-9]+)?$') AND ((double)total_amount_str > 0.0) AND
        (ABS((double)total_amount_str - (double)quantity_str * (double)unit_price_str) < 0.01)
    ),
    reject_records OTHERWISE;

-- Rank valid records to establish a unique deterministic row index for auditable deduplication
ranked_valid = RANK valid_raw;

-- Score valid records by data completeness (prioritizing records with present optional fields: customer_age and rating)
scored_valid = FOREACH ranked_valid GENERATE
    rank_valid_raw,
    order_id,
    transaction_id,
    customer_id,
    transaction_date,
    product_id,
    product_name,
    category,
    quantity_str,
    unit_price_str,
    total_amount_str,
    payment_method,
    customer_age_str,
    customer_gender,
    city,
    order_status,
    rating_str,
    ((customer_age_str != '' ? 1 : 0) + (rating_str != '' ? 1 : 0)) AS quality_score:int;

-- Group valid records by transaction_id (the primary commercial transaction business key)
grouped_by_txn = GROUP scored_valid BY transaction_id;

-- For each transaction group, select exactly one best/complete record deterministically
best_with_rank = FOREACH grouped_by_txn {
    sorted_recs = ORDER scored_valid BY quality_score DESC, rank_valid_raw ASC;
    top_one = LIMIT sorted_recs 1;
    GENERATE FLATTEN(top_one);
};

-- Format accepted business records with strict target types for Hive ingestion (16,500 records)
clean_records = FOREACH best_with_rank GENERATE
    order_id,
    transaction_id,
    customer_id,
    transaction_date,
    product_id,
    product_name,
    category,
    (int)quantity_str AS quantity:int,
    (double)unit_price_str AS unit_price:double,
    (double)total_amount_str AS total_amount:double,
    payment_method,
    (customer_age_str matches '^[0-9]+$' ? (int)customer_age_str : null) AS customer_age:int,
    customer_gender,
    city,
    order_status,
    (rating_str matches '^[0-9]+(\\.[0-9]+)?$' ? (double)rating_str : null) AS rating:double;

-- Identify duplicate occurrences excluded from the final business dataset via LEFT OUTER JOIN on rank
joined_for_dups = JOIN scored_valid BY rank_valid_raw LEFT OUTER, best_with_rank BY top_one::rank_valid_raw;
dup_recs_filtered = FILTER joined_for_dups BY best_with_rank::top_one::rank_valid_raw IS NULL;

-- Format duplicate/excluded records for audit quarantine
duplicate_records = FOREACH dup_recs_filtered GENERATE
    scored_valid::order_id AS order_id:chararray,
    scored_valid::transaction_id AS transaction_id:chararray,
    scored_valid::customer_id AS customer_id:chararray,
    scored_valid::transaction_date AS transaction_date:chararray,
    scored_valid::product_id AS product_id:chararray,
    scored_valid::product_name AS product_name:chararray,
    scored_valid::category AS category:chararray,
    scored_valid::quantity_str AS quantity_str:chararray,
    scored_valid::unit_price_str AS unit_price_str:chararray,
    scored_valid::total_amount_str AS total_amount_str:chararray,
    scored_valid::payment_method AS payment_method:chararray,
    scored_valid::customer_age_str AS customer_age_str:chararray,
    scored_valid::customer_gender AS customer_gender:chararray,
    scored_valid::city AS city:chararray,
    scored_valid::order_status AS order_status:chararray,
    scored_valid::rating_str AS rating_str:chararray;

-- Format rejected/invalid records with original fields for auditing (e.g., 54 records for W05 reference dataset)
formatted_rejects = FOREACH reject_records GENERATE
    order_id,
    transaction_id,
    customer_id,
    transaction_date,
    product_id,
    product_name,
    category,
    quantity_str,
    unit_price_str,
    total_amount_str,
    payment_method,
    customer_age_str,
    customer_gender,
    city,
    order_status,
    rating_str;

-- Store clean records into HDFS clean directory using tab delimiter (e.g., 16,500 accepted business records for W05 reference dataset)
STORE clean_records INTO '$CLEAN_OUTPUT' USING PigStorage('\t');

-- Store rejected invalid records into HDFS reject invalid directory using tab delimiter (e.g., 54 records for W05 reference dataset)
STORE formatted_rejects INTO '$REJECT_OUTPUT' USING PigStorage('\t');

-- Store duplicate excluded records into HDFS reject duplicates directory using tab delimiter (e.g., 146 records for W05 reference dataset)
STORE duplicate_records INTO '$DUPLICATE_OUTPUT' USING PigStorage('\t');

