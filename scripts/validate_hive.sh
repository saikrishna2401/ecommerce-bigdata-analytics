#!/usr/bin/env bash
# ==============================================================================
# scripts/validate_hive.sh - Run Hive Data Validation Queries
# ==============================================================================

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${PROJECT_ROOT}/config/project.conf"

if [[ ! "${HIVE_DATABASE}" =~ ^[a-zA-Z0-9_]+$ ]] || [[ ! "${HIVE_TABLE}" =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "ERROR: Invalid database or table name in configuration." >&2
    exit 1
fi

echo "================================================================================"
echo "RUNNING HIVE DATA VALIDATION QUERIES (${HIVE_DATABASE}.${HIVE_TABLE})"
echo "================================================================================"

hive -e "
USE ${HIVE_DATABASE};

SELECT 
    COUNT(*) AS total_records,
    SUM(quantity) AS total_quantity,
    ROUND(SUM(total_amount), 2) AS total_sales,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT product_id) AS unique_products
FROM ${HIVE_TABLE};

SELECT 
    batch_id,
    COUNT(*) AS batch_records,
    ROUND(SUM(total_amount), 2) AS batch_sales
FROM ${HIVE_TABLE}
GROUP BY batch_id
ORDER BY batch_id;

SELECT 
    category,
    COUNT(*) AS record_count,
    ROUND(SUM(total_amount), 2) AS category_sales
FROM ${HIVE_TABLE}
GROUP BY category
ORDER BY category_sales DESC;

SELECT 
    order_status,
    COUNT(*) AS order_count
FROM ${HIVE_TABLE}
GROUP BY order_status
ORDER BY order_count DESC;

SELECT 
    COUNT(*) AS duplicate_transaction_count
FROM (
    SELECT transaction_id
    FROM ${HIVE_TABLE}
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
) d;
"
