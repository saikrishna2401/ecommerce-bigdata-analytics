# E-Commerce Big Data Analytics — User & Operational Guide

## 1. Overview

The **E-Commerce Customer Purchase Analysis and Product Recommendation System** is an enterprise big data solution built to process, clean, analyze, and generate product recommendations from weekly transaction batches.

The pipeline runs on:
- **Hadoop 3.4.1** (HDFS storage, YARN resource manager)
- **Pig 0.18.0 on Apache Tez 0.10.5** (Data validation and quality ETL)
- **Apache Hive 4.2.1 on Apache Tez** (External partitioned tables and analytical aggregations)
- **Bash Orchestrator** (`./ecommerce`) with full security hardening and idempotency protections

---

## 2. Command Line Interface

The primary interface is `./ecommerce`, located in the project root:

```bash
cd ~/ecommerce-bigdata-project
```

### Basic Syntax
```bash
./ecommerce [OPTIONS] [HDFS_INPUT_PATH]
```

### Supported Modes

| Command | Description |
| :--- | :--- |
| `./ecommerce --check` | Pre-flight check of Hadoop, HDFS, Tez, Pig, Hive, directory permissions, and configuration. |
| `./ecommerce --batch-status` | Displays the persistent batch processing registry (`state/registry.tsv`) showing all historical batches, counts, and statuses. |
| `./ecommerce --help` | Displays command syntax and usage instructions. |
| `./ecommerce <hdfs_path>` | Processes a weekly batch file (e.g. `/project-2/raw/2026-W05/transactions.csv`). |

---

## 3. Pre-Flight Health Check

Before running processing jobs, verify that all cluster components are online and healthy:

```bash
./ecommerce --check
```

**Expected Output:**
```text
========================================
E-COMMERCE BIG DATA ANALYTICS
=============================

Hadoop:             [OK]
HDFS Connectivity:  [OK]
Hive:               [OK]
Pig:                [OK]
Tez Engine:         [OK]
Project Structure:  [OK]
Configuration:      [OK]

Pre-flight checks passed successfully.
```

---

## 4. Processing a Weekly Batch

### Step 1: Upload Raw Batch to HDFS
Ensure the raw transaction CSV file is placed under `/project-2/raw/<batch_id>/`:
```bash
# Example for week 2026-W05
hdfs dfs -mkdir -p /project-2/raw/2026-W05
hdfs dfs -put local_transactions.csv /project-2/raw/2026-W05/transactions.csv
```

> **Note on Batch ID Format**: The batch ID must follow the `YYYY-WNN` format (e.g., `2026-W01` through `2026-W52`, or custom weekly test numbering like `2026-W98`).

### Step 2: Execute the Pipeline
Run `./ecommerce` passing the HDFS path:
```bash
./ecommerce /project-2/raw/2026-W05/transactions.csv
```

### Step 3: Observe Pipeline Stages
The orchestrator executes the following stages automatically:
1. **Validation**: Enforces path boundary restrictions (`/project-2/raw/`), verifies file existence, and validates batch ID format.
2. **Registry Check**: Enforces idempotency (if already processed and reports exist, skips redundant computation).
3. **Stage 1 (Pig Cleaning on Tez)**: Validates schema, discards corrupt/invalid records, and routes clean data to `/project-2/clean/2026-W05/` and rejected rows to `/project-2/reject/2026-W05/`.
4. **Stage 2 (Record Auditing)**: Logs input count, clean count, rejected count, and clean percentage.
5. **Stage 3 (Hive Partition Loading)**: Registers the partition in `project2.ecommerce_transactions` and reconciles counts.
6. **Stage 4 (Weekly Analytical Report)**: Generates sales, category, top products, payment methods, order status, and customer summaries.
7. **Stage 5 (Historical Analytics & Trends)**: Computes cross-batch longitudinal trends and Week-over-Week growth.
8. **Stage 6 (Product Recommendations)**: Derives order-level market basket association rules (`support`, `confidence`).
9. **Stage 7 (Recommendation Evaluation)**: Performs holdout evaluation against historical rules.

---

## 5. Interpreting Reports

All reports are published in HDFS under `/project-2/reports/`.

### A. Weekly Analytical Reports
Located at: `/project-2/reports/weekly/<batch_id>/`

Files:
- `weekly_summary.txt`: Formatted human-readable report covering:
  - Batch Info & Record Hygiene (Input, Clean, Rejected, Clean %)
  - Sales Overview (Orders, Customers, Products, Quantity, Sales, Average Order Value)
  - Category Breakdown (Sales, Quantity, Share %)
  - Top Products by Revenue
  - Payment Methods & Fulfillment Status
  - Top Spending Customers
- Individual CSV exports:
  - `category_summary.csv`
  - `product_summary.csv`
  - `payment_summary.csv`
  - `order_status_summary.csv`
  - `customer_summary.csv`

To view the weekly text summary:
```bash
hdfs dfs -cat /project-2/reports/weekly/2026-W05/weekly_summary.txt
```

### B. Historical Longitudinal Reports
Located at: `/project-2/reports/historical/`

Files:
- `historical_summary.txt`: Overall longitudinal metrics spanning all batches.
- `weekly_trend.csv`: Longitudinal table with Week-over-Week (WoW) growth percentages for:
  - Orders & WoW %
  - Unique Customers & WoW %
  - Quantity Sold & WoW %
  - Total Revenue & WoW %
  - Average Order Value
- `historical_category_trend.csv`: Category performance across consecutive weeks.

To view the longitudinal trend:
```bash
hdfs dfs -cat /project-2/reports/historical/weekly_trend.csv
```

### C. Product Recommendation Reports
Located at: `/project-2/reports/recommendations/<batch_id>/`

Files:
- `recommendations.txt`: Explanatory report detailing discovered association rules:
  - Antecedent Product ($A$) $\to$ Recommended Consequent Product ($B$)
  - Pair Count (Number of co-purchasing orders)
  - Support: $\frac{\text{pair\_count}}{\text{total\_orders}}$
  - Confidence: $\frac{\text{pair\_count}}{\text{orders\_containing\_A}}$
- `recommendations.csv`: Raw rules in standard comma-separated format.

> **Note on Single-Item Batches**: If all transactions in a batch are single-item orders, or no pairs meet the minimum support/confidence thresholds, `recommendations.txt` explicitly documents that no pairs met thresholds.

To view recommendations:
```bash
hdfs dfs -cat /project-2/reports/recommendations/2026-W05/recommendations.txt
```

### D. Recommendation Evaluation Reports
Located at: `/project-2/reports/evaluation/<batch_id>/`

Files:
- `evaluation.txt`: Formatted audit report of the temporal holdout evaluation.
  - Compares rules derived from the preceding historical batch against orders in the current batch.
  - Reports: Historical Rules Evaluated, Recommendations Tested, Hits, and Hit Rate %.
- `evaluation.csv`: Evaluation metrics in tabular format.

To view evaluation results:
```bash
hdfs dfs -cat /project-2/reports/evaluation/2026-W05/evaluation.txt
```

---

## 6. Batch Status & Tracking

To view the current status of all processed batches:
```bash
./ecommerce --batch-status
```

**Example Output:**
```text
========================================================================================================================
BATCH PROCESSING REGISTRY
========================================================================================================================
BATCH_ID   STATUS    START_TIME            END_TIME              INPUT  CLEAN  REJECT  CLEAN%   INPUT_PATH
------------------------------------------------------------------------------------------------------------------------
2026-W98   SUCCESS   2026-09-22 21:28:10   2026-09-22 21:30:15   8      5      3       62.50%   /project-2/raw/_test/2026-W98/transactions.csv
2026-W99   SUCCESS   2026-09-22 21:30:15   2026-09-22 21:30:38   7      4      3       57.14%   /project-2/raw/_test/2026-W99/transactions.csv
========================================================================================================================
```

---

## 7. Troubleshooting & Common Issues

### 1. "ERROR: Input path contains invalid or forbidden characters"
- **Cause**: Input contains shell metacharacters (`;`, `&`, `|`, `$`, `` ` ``, space).
- **Remedy**: Pass clean HDFS paths without special shell characters.

### 2. "ERROR: Path traversal (..) is strictly prohibited"
- **Cause**: Path contains relative traversal sequences (`..`).
- **Remedy**: Use canonical absolute paths under `/project-2/raw/`.

### 3. "ERROR: Input path must reside inside /project-2/raw/"
- **Cause**: Path is outside the designated raw landing zone (e.g. `/tmp/` or `/user/`).
- **Remedy**: Place input files in `/project-2/raw/<batch_id>/`.

### 4. "ERROR: Hive execution failed"
- **Check**: Inspect HiveServer2 status and recent log file in `logs/YYYY-MM-DD_HH-MM-SS.log`. Ensure YARN NodeManager and ResourceManager are running (`yarn node -list`).

### 5. Tez Out of Memory / Container Preemption
- **Remedy**: Verify YARN memory allocations in `yarn-site.xml` or ensure no background orphaned Java processes are hogging memory. Check running applications with `yarn application -list`.
