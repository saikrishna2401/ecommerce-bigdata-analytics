# E-Commerce Big Data Analytics Architecture

## 1. System Overview

The **E-Commerce Customer Purchase Analytics and Product Recommendation System** is designed for automated, robust, and scalable batch processing of e-commerce transaction data. The underlying infrastructure runs on an enterprise Ubuntu 24.04 LTS Big Data environment leveraging Apache Hadoop 3.4.1 (HDFS & YARN), Apache Hive 4.2.1, Apache Tez 0.10.5, and Apache Pig 0.18.0.

### Environment Specification
- **Operating System**: Ubuntu 24.04 LTS (`data-lab`)
- **System User**: `hdp`
- **Hadoop**: 3.4.1 (HDFS, YARN NodeManager/ResourceManager)
- **Hive**: 4.2.1 (Running with Java 21)
- **Tez Execution Engine**: 0.10.5
- **Pig**: 0.18.0 (Running with Java 17)
- **Kafka**: 4.3.1 (Infrastructure / Future real-time streaming event ingestion; batch ETL path is powered by Hadoop HDFS + Pig on Tez + Hive)
- **Java Runtimes**: OpenJDK 17 (Default / Hadoop / Tez / Pig / Kafka), OpenJDK 21 (Hive)

---

## 2. End-to-End Pipeline Workflow

The complete architecture integrates Blocks 1, 2, and 3 into a single unified CLI orchestrator:

```
USER
  │
  ▼
./ecommerce [HDFS_CSV_PATH]
  │
  ├─► [BLOCK 1: FOUNDATION & VALIDATION]
  │     ├── Pre-flight big data stack health check (--check)
  │     ├── Centralized configuration loading (config/project.conf)
  │     ├── HDFS input path existence and security boundary validation (/project-2/raw/)
  │     ├── Path traversal defense (no ../ allowed) & shell injection defense
  │     └── Weekly batch identifier validation (YYYY-WNN format, e.g. 2026-W05)
  │
  ├─► [BLOCK 2: PIG CLEANING, DEDUPLICATION & HIVE INGESTION]
  │     ├── Batch Registry & Idempotency check (state/registry.tsv)
  │     ├── Pig Latin quality cleaning on Tez engine (pig/clean_transactions.pig)
  │     │     ├── RFC-4180 CSV parsing via Piggybank CSVExcelStorage
  │     │     ├── CSV header stripping & field trimming
  │     │     ├── Data quality split: Valid Schema vs Fatal Invalid Rejects (54 rows)
  │     │     ├── Deterministic deduplication by transaction_id via RANK & quality score
  │     │     └── 3-Way Storage Routing:
  │     │           ├── Accepted Business Data -> /project-2/clean/<batch>/ (16,500 records)
  │     │           ├── Invalid Fatal Rejects  -> /project-2/reject/<batch>/invalid/ (54 records)
  │     │           └── Duplicate Exclusions   -> /project-2/reject/<batch>/duplicates/ (146 records)
  │     ├── Record auditing (16,700 = 16,500 + 54 + 146, 0 unaccounted)
  │     ├── Hive external partitioned table management (project2.ecommerce_transactions)
  │     ├── Dynamic partition auto-sync (sync_hive_partitions) preventing metastore orphan crashes
  │     └── Dynamic partition registration & count reconciliation
  │
  └─► [BLOCK 3: ANALYTICS, HISTORICAL TRENDS & RECOMMENDATIONS]
        ├── STAGE 4: Weekly Analytical Report
        │     ├── Sales overview, category breakdown, top products, payments, order status, customer spending
        │     ├── Local staging and export to formatted CSVs
        │     ├── Formatted human-readable summary (weekly_summary.txt)
        │     └── Atomic publication to HDFS -> /project-2/reports/weekly/<batch_id>/
        ├── STAGE 5: Historical Analytics & Longitudinal Trends
        │     ├── Cross-batch longitudinal analysis across all processed batches in Hive
        │     ├── Week-over-Week (WoW) growth computation (Orders, Sales, Customers, Quantity)
        │     ├── Category performance across batches
        │     ├── Formatted summary (historical_summary.txt) & CSV exports (weekly_trend.csv, historical_category_trend.csv)
        │     └── Atomic publication to HDFS -> /project-2/reports/historical/
        ├── STAGE 6: Rule-Based Product Association Recommender
        │     ├── Order-level Market Basket Association Rule Mining in Apache Hive
        │     ├── Co-occurrence pair counting: pair_count(A, B)
        │     ├── Support and confidence threshold filtering (min_pair_count, min_confidence)
        │     ├── Formatted summary (recommendations.txt) & CSV export (recommendations.csv)
        │     └── Atomic publication to HDFS -> /project-2/reports/recommendations/<batch_id>/
        └── STAGE 7: Recommendation Holdout Evaluation
              ├── Temporal holdout validation: Evaluates earlier batch rules against current batch orders
              ├── Counts recommendations tested and hits (co-purchased antecedent & consequent)
              ├── Hit rate calculation and transparent edge-case reporting
              ├── Formatted summary (evaluation.txt) & CSV export (evaluation.csv)
              └── Atomic publication to HDFS -> /project-2/reports/evaluation/<batch_id>/
```

---

## 3. Mathematical Formulas & Methodologies

All metrics are derived directly from actual Hive data in `project2.ecommerce_transactions`. No metrics are fabricated or synthetically altered.

### A. Business Record Accounting & Deduplication Invariant
$$\text{Raw Input Records} = \text{Accepted Business Records} + \text{Rejected Invalid Records} + \text{Excluded Duplicate Records}$$
$$\mathbf{16,700 = 16,500 + 54 + 146} \quad (\mathbf{16,700 - 54 - 146 = 16,500})$$
1. **Raw Source**: 16,700 physical data rows.
2. **Accepted Business Records (16,500)**: Exactly matches the verified reference business dataset (`ecommerce_transactions_clean_16500.csv`). Deduplication groups by `transaction_id` and selects the single most complete row using a deterministic quality score (bonuses for `customer_age` and `rating`) and `rank_valid_raw ASC`.
3. **Rejected Invalid Records (54)**: Fatal schema violations (missing mandatory identifiers, malformed quantities/prices, or total amount $\ne$ quantity $\times$ unit price). Quarantined in `/project-2/reject/<batch>/invalid/`.
4. **Excluded Duplicate Records (146)**: Secondary occurrences of valid transactions sharing an already-selected commercial `transaction_id`. Preserved in `/project-2/reject/<batch>/duplicates/` ensuring zero silent data loss.

### B. Sales Overview & Customer Metrics
- **Total Orders**: $\text{COUNT}(\text{DISTINCT } \text{order\_id})$
- **Total Customers**: $\text{COUNT}(\text{DISTINCT } \text{customer\_id})$
- **Total Products**: $\text{COUNT}(\text{DISTINCT } \text{product\_id})$
- **Total Quantity**: $\sum \text{quantity}$
- **Total Sales**: $\sum \text{total\_amount}$
- **Average Order Value (AOV)**:
  $$\text{AOV} = \frac{\text{Total Sales}}{\text{Total Orders}}$$

### C. Week-over-Week (WoW) Growth Trends
Computed across chronologically sorted batches using Hive window functions (`LAG`):
$$\text{WoW Growth \%} = \frac{\text{Current Metric} - \text{Previous Metric}}{\text{Previous Metric}} \times 100$$
- If no previous batch exists (e.g. initial baseline batch), WoW is reported as `N/A`.
- If previous metric is zero, division by zero is avoided and reported as `N/A`.

### D. Market Basket Association Rules
Given transactions grouped by order:
1. **Pair Count**: Number of distinct orders containing both Product $A$ and Product $B$ ($A \ne B$):
   $$\text{pair\_count}(A, B) = \text{COUNT}(\text{DISTINCT } \text{order\_id} \text{ where } A \in \text{order} \land B \in \text{order})$$
2. **Support**: Fraction of all batch orders that contain both Product $A$ and Product $B$:
   $$\text{support}(A, B) = \frac{\text{pair\_count}(A, B)}{\text{Total Batch Orders}}$$
3. **Confidence**: Likelihood that an order containing Product $A$ also contains Product $B$:
   $$\text{confidence}(A \to B) = \frac{\text{pair\_count}(A, B)}{\text{orders\_containing}(A)}$$
4. **Filtering**: Rules are filtered using configurable thresholds in `config/project.conf`:
   - $\text{pair\_count}(A, B) \ge \text{MIN\_PAIR\_COUNT}$ (default: 1)
   - $\text{confidence}(A \to B) \ge \text{MIN\_CONFIDENCE}$ (default: 0.10)

### E. Temporal Holdout Recommendation Evaluation
Evaluation assesses real predictive utility across time without leakage:
1. **Training Baseline**: Association rules derived from the preceding historical batch (e.g., $B_{n-1}$).
2. **Holdout Evaluation**: Tested against actual multi-product orders in the subsequent batch ($B_n$).
3. **Tested Instances**: Number of times an order in $B_n$ contains antecedent product $A$.
4. **Hit**: An instance where the same order in $B_n$ also contains recommended consequent product $B$.
5. **Hit Rate**:
   $$\text{Hit Rate} = \frac{\text{Recommendation Hits}}{\text{Recommendations Tested}} \times 100$$
- If no historical rules met thresholds or 0 recommendations are tested, Hit Rate is reported cleanly as `N/A` with explicit explanation.

---

## 4. HDFS Directory Architecture

The system maintains strict isolation under the `/project-2/` HDFS namespace:

```
/project-2/
├── raw/                                 # Raw weekly transaction CSV files
│   └── <batch_id>/transactions.csv      # (16,700 data records)
├── clean/                               # Validated, cleaned tab-delimited records
│   └── <batch_id>/part-*                # (16,500 accepted business records)
├── reject/                              # Rejected/corrupted & duplicate records
│   └── <batch_id>/
│       ├── invalid/part-*               # (54 fatal schema/calculation rejects)
│       └── duplicates/part-*            # (146 excluded valid duplicate occurrences)
└── reports/                             # Production analytical reports
    ├── weekly/                          # Per-batch analytical reports
    │   └── <batch_id>/
    │       ├── weekly_summary.txt       # Human-readable weekly summary report
    │       ├── category_summary.csv     # Category-level breakdown
    │       ├── product_summary.csv      # Top product performance
    │       ├── payment_summary.csv      # Payment method breakdown
    │       ├── order_status_summary.csv # Order fulfillment status breakdown
    │       └── customer_summary.csv     # Top spending customers
    ├── historical/                      # Longitudinal multi-batch reports
    │   ├── historical_summary.txt       # Overall historical KPI summary
    │   ├── weekly_trend.csv             # Longitudinal weekly trend with WoW metrics
    │   └── historical_category_trend.csv# Longitudinal category trends across batches
    ├── recommendations/                 # Rule-based association recommendations
    │   └── <batch_id>/
    │       ├── recommendations.txt      # Formatted recommendation report
    │       └── recommendations.csv      # Exported recommendation rules
    └── evaluation/                      # Temporal holdout model evaluation
        └── <batch_id>/
            ├── evaluation.txt           # Formatted evaluation report
            └── evaluation.csv           # Evaluation metrics table
```

### Unrelated HDFS Directories (Protected / Untouched)
The following directories belong to system infrastructure and other workloads:
- `/apps` & `/apps/tez` (Tez runtime libraries)
- `/lab-test`, `/pig-test`, `/wordcount-input`, `/wordcount-output`, `/tez-wordcount-output`
- `/tmp`, `/user`

---

## 5. Local Project Structure

The project is organized in `~/ecommerce-bigdata-project`:

```
~/ecommerce-bigdata-project/
├── config/
│   └── project.conf             # Central configuration (paths, currency, thresholds)
├── scripts/
│   ├── common.sh                # Core library: logging, health checks, ETL, report generators
│   └── validate_hive.sh         # Helper script for Hive partition inspection
├── pig/
│   └── clean_transactions.pig   # Pig Latin cleaning script running on Apache Tez
├── hive/
│   ├── setup_table.sql          # DDL for database and external partitioned table
│   ├── validation.sql           # SQL validation queries for data auditing
│   ├── weekly_report.sql        # Block 3 Weekly report multi-query SQL template
│   ├── historical_report.sql    # Block 3 Historical longitudinal SQL template
│   ├── recommendations.sql      # Block 3 Association rule mining SQL template
│   └── evaluation.sql           # Block 3 Holdout evaluation SQL template
├── state/
│   └── registry.tsv             # Persistent batch state tracking registry
├── security/
│   ├── audit.md                 # Security audit specifications and scope
│   ├── findings.md              # Hardening findings and remediation log
│   └── test-results.md          # Verification and security regression test results
├── documentation/
│   ├── architecture.md          # Architectural specification and workflow documentation
│   └── project-usage.md         # Comprehensive operational and user guide
├── logs/
│   └── YYYY-MM-DD_HH-MM-SS.log  # Comprehensive execution log per run
└── ecommerce                    # Main unified CLI entrypoint
```

---

## 6. Hive Data Model & Partition Strategy

- **Database**: `project2`
- **Table**: `ecommerce_transactions` (External Table)
- **Partition Key**: `batch_id STRING`
- **Location Mapping**: `PARTITION (batch_id = '<id>') LOCATION '/project-2/clean/<id>'`
- **Storage Format**: Delimited by `\t`, TextFile format
- **Incremental Processing**: Each batch is independently loaded into its dedicated partition without reprocessing historical partitions.
- **Query Optimization**: Analytics queries leverage partition pruning (`WHERE batch_id = '<id>'`) to minimize I/O and optimize Tez container allocation.

---

## 7. Edge Cases & Resilience Engineering

| Edge Case | Risk | Architecture Mitigation |
| :--- | :--- | :--- |
| **First Batch (No Prior History)** | Window function `LAG()` returns NULL for previous week. | WoW % displays `N/A` instead of failing or producing division-by-zero errors. Historical report outputs explicit baseline notice. Evaluation cleanly identifies first batch and reports baseline status. |
| **Single-Item Baskets** | Orders have only 1 line item; co-purchase join produces 0 rows. | Handled gracefully: `recommendations.txt` displays informative "No recommendation pairs met threshold" message with exact order count and 0 qualifying pairs; `recommendations.csv` contains headers with 0 rows; evaluation reports 0 tested instances. |
| **Orphaned Metastore Partitions** | Out-of-band HDFS deletions leave dangling Hive metastore partition references, causing full-table longitudinal queries to crash with `InvalidInputException`. | Automated partition synchronization (`sync_hive_partitions`) inspects physical HDFS paths and cleanly drops orphaned partition entries prior to query execution. |
| **Transient VM Pauses During HDFS Put** | Hypervisor host pauses or heavy I/O spikes can cause brief socket timeouts during bulk multi-file uploads to HDFS. | Atomic publishing engine (`safe_publish_hdfs_dir`) wraps upload operations in a resilient 3-attempt retry loop with staging directory cleanup and backoff delays. |
| **Hive Tez AM Session Overhead** | Separate Hive invocations create redundant Tez ApplicationMasters. | Multi-query SQL templates bundle staging queries within single Hive execution sessions (`hive -e "$(< script.sql)"`), reducing latency from ~10 mins to ~90s. |
| **Trailing Newline Absence in TSV** | Hive `INSERT OVERWRITE LOCAL DIRECTORY` omits trailing newlines. Under `set -e`, `read` returns exit code 1. | Handled via `|| true` on line reader expressions and `tr '\t' ','` conversion pipelines. |
| **Hidden File Collisions** | Hive creates hidden `.crc` checksum files in output directories. | Staging extraction filters with `find ... ! -name '.*'` to prevent binary checksum collisions in parsed reports. |
| **Idempotency & Duplicate Re-runs** | Running same batch multiple times could cause duplicate rows. | Script checks HDFS reports first; if already generated, reports are preserved and CLI displays summary instantly without modifying tables or duplicate metrics. |
