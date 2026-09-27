# L&T E-Commerce Big Data Analytics and Product Recommendation System

[![Hadoop 3.4.1](https://img.shields.io/badge/Hadoop-3.4.1-yellow?logo=apachehadoop)](https://hadoop.apache.org/)
[![Hive 4.2.1](https://img.shields.io/badge/Hive-4.2.1-orange?logo=apachehive)](https://hive.apache.org/)
[![Tez 0.10.5](https://img.shields.io/badge/Tez-0.10.5-blue)](https://tez.apache.org/)
[![Pig 0.18.0](https://img.shields.io/badge/Pig-0.18.0-red)](https://pig.apache.org/)
[![Kafka 4.3.1](https://img.shields.io/badge/Kafka-4.3.1-black?logo=apachekafka)](https://kafka.apache.org/)
[![Status](https://img.shields.io/badge/Production-Verified-brightgreen)](#)

---

## 1. Executive Summary

The **L&T E-Commerce Big Data Analytics and Product Recommendation System** is an enterprise-grade, distributed big data platform designed for high-throughput transactional data ingestion, cleaning, multi-dimensional business analytics, longitudinal trend forecasting, and market basket product recommendations.

Engineered for strict academic and industrial rigor, the platform guarantees:
- **Zero-Loss Accounting Invariant**: Raw Input = Clean Data + Rejected Records.
- **Strict Idempotency & Safe Retryability**: Safe replay without duplicate rows, ghost files, or data contamination.
- **Fail-Safe Orchestration**: Non-blocking atomic locks (`flock`), safe staging directories, and automated rollback.
- **Enterprise Security**: Complete path-traversal prevention, strict regex-based batch identifiers (`YYYY-WNN`), and defense against shell injection.
- **Production-Scale Verification**: Verified end-to-end on both edge test batches (500 records) and the full production dataset (**16,700 transactions**).

---

## 2. Technology Stack & Cluster Architecture

| Component | Version | Role in Platform | Execution Runtime |
| :--- | :--- | :--- | :--- |
| **Hadoop HDFS** | 3.4.1 | Distributed raw, clean, reject, and report storage | Java 17 |
| **Hadoop YARN** | 3.4.1 | Distributed resource negotiation & container scheduling | Java 17 |
| **Apache Tez** | 0.10.5 | Directed Acyclic Graph (DAG) distributed compute engine | Java 17 |
| **Apache Pig** | 0.18.0 | High-throughput schema validation and data hygiene ETL | Pig on Tez (Java 17) |
| **Apache Hive** | 4.2.1 | External partitioned analytics warehouse & SQL aggregations | Hive on Tez (Java 21) |
| **Apache Kafka** | 4.3.1 | Event-streaming backbone architecture for real-time ingest | KRaft Mode (Java 17) |
| **Bash Orchestrator**| 5.2 | Unified CLI with concurrency controls & automated rollback | Linux POSIX |

---

## 3. Directory Structure

```text
~/ecommerce-bigdata-project/
├── ecommerce                     # Primary CLI and orchestration entrypoint
├── config/
│   └── project.conf              # Central configuration (HDFS paths, thresholds, Hive DB)
├── documentation/
│   ├── architecture.md           # Detailed architectural specification & design patterns
│   └── project-usage.md          # User operational runbook & troubleshooting guide
├── hive/
│   └── (HQL analytical scripts)
├── logs/                         # Execution logs timestamped per pipeline run
├── pig/
│   ├── clean_transactions.pig    # Pig Latin data hygiene, validation, and rejection ETL
│   └── (Pig macros & test scripts)
├── reports/
│   ├── weekly/                   # Local staging for weekly business analytics
│   ├── historical/               # Local staging for longitudinal trends & WoW growth
│   ├── recommendations/          # Local staging for association rule mining
│   └── evaluation/               # Local staging for holdout recommendation evaluation
├── scripts/
│   ├── common.sh                 # Reusable shell functions, locks, and validation logic
│   └── validate_hive.sh          # Cross-partition Hive data validation query suite
├── security/
│   └── (Security audit controls)
└── state/
    └── registry.tsv              # Append-only persistent batch audit log
```

---

## 4. Key Engineering Invariants & Architecture

### A. End-to-End Processing Flow
```text
ANY RAW DATASET (/project-2/raw/<batch_id>/transactions.csv)
       │
       ▼
[Pig on Apache Tez: Data Validation & Deduplication]
       ├── Accepted Business Records  ──> /project-2/clean/<batch_id>/
       ├── Rejected Invalid Records   ──> /project-2/reject/<batch_id>/invalid/
       └── Excluded Duplicate Records ──> /project-2/reject/<batch_id>/duplicates/
       │
       ▼
[Apache Hive: External Partitioned Storage] (project2.ecommerce_transactions)
       │
       ├── Weekly Analytics Report     ──> /project-2/reports/weekly/<batch_id>/
       ├── Longitudinal Trends (WoW)   ──> /project-2/reports/historical/
       ├── Market Basket Recommender   ──> /project-2/reports/recommendations/<batch_id>/
       └── Recommendation Evaluation   ──> /project-2/reports/evaluation/<batch_id>/
```

### B. Universal Dynamic Accounting Invariant
For any input dataset, the pipeline dynamically discovers and enforces the record accounting invariant with zero silent data loss:
$$\text{Raw Input Records} = \text{Accepted Business Records} + \text{Rejected Invalid Records} + \text{Excluded Duplicate Records}$$

- **Generic Pipeline Dynamic Rule**: The counts are computed at runtime from physical HDFS output files (`count_batch_records`). If $\text{Raw} \ne \text{Clean} + \text{Invalid} + \text{Duplicates}$, the pipeline halts immediately with an audit failure.
- **W05 Reference Benchmark Example**:
  $$\mathbf{16,700 = 16,500 + 54 + 146} \quad (\mathbf{16,700 - 54 - 146 = 16,500})$$
  *(Note: 16,700, 16,500, 54, and 146 are expected values for the reference dataset, not hard-coded pipeline constraints.)*
  - **Accepted Business Records (16,500)**: Exactly matches the verified reference business dataset (`ecommerce_transactions_clean_16500.csv`). Pig Latin deduplicates by commercial key (`transaction_id`) via `RANK` and a data-completeness score (bonus for present optional fields: `customer_age` and `rating`), outputting distinct commercial transactions to `/project-2/clean/<batch_id>/`.
  - **Rejected Invalid Records (54)**: Fatal schema and calculation errors (missing mandatory identifiers, malformed quantities/prices, or total amount $\ne$ quantity $\times$ unit price). Quarantined in `/project-2/reject/<batch_id>/invalid/`.
  - **Excluded Duplicate Records (146)**: Secondary occurrences of valid transactions sharing an already-selected `transaction_id`. Preserved in `/project-2/reject/<batch_id>/duplicates/` for full auditability with zero silent data loss.

### C. Strict Batch ID Format Enforcement
Batch IDs must strictly conform to `YYYY-WNN` ($NN \in [01, 99]$):
- Enforced with boundary-anchored regular expressions: `^([0-9]{4}-W[0-9]{2})$`.
- Eliminates prefix-matching bugs (e.g., prevents `2026-W100` from being truncated to `2026-W10`).
- Dynamically extracted from input HDFS path or prompted interactively with format normalization.

### D. Concurrency & Idempotency
- **Batch Locking**: Process locks are acquired via non-blocking Linux kernel `flock` on file descriptor 200, guaranteeing that duplicate triggers fail safely without race conditions. Lock release is bound into an automated `EXIT` trap.
- **Staging Isolation & Resilience**: All Pig outputs and Hive reports are written into isolated staging directories before atomic promotion into target HDFS locations. HDFS publication features automatic retry logic with backoff to handle transient VM host pauses.
- **Partition Freshness & Auto-Sync**: The pipeline automatically synchronizes Hive metastore partitions with physical HDFS directories (`sync_hive_partitions`), cleanly dropping orphaned metastore references to avoid Hadoop `InvalidInputException` crashes.

### E. Longitudinal Analytics & Baseline Handling
- A first-ever batch (e.g., `2026-W05`) is an initial enterprise ingestion where no prior historical data exists.
- The pipeline dynamically handles this baseline state: Week-over-Week (WoW) metrics display `N/A`, and the longitudinal report outputs a clear baseline notice without failing. Subsequent batches automatically compute full chronological WoW trends across all ingested weeks.

### F. Recommendation Engine & Edge-Case Handling
- The product recommendation engine derives association rules using market basket co-occurrence analysis: $\text{Support}(A \rightarrow B)$ and $\text{Confidence}(A \rightarrow B)$ across distinct orders containing $\ge 2$ distinct products.
- **Data-Specific Edge Case**: The W05 reference dataset contains strictly single-item orders ($1$ item per order basket). The engine dynamically recognizes that no multi-item pairs meet the threshold, generating 0 rules and cleanly documenting the reason (`"No qualifying multi-item pairs"`) rather than fabricating recommendations. When multi-item baskets are present, rules meeting thresholds are derived dynamically.

### G. Temporal Holdout Evaluation & Baseline Constraint
- The evaluation engine validates recommendation rules against holdout orders in chronologically subsequent batches.
- For an initial batch (e.g., `2026-W05`), no prior historical training batch exists. The evaluation report dynamically documents this as `Baseline / Warm-up (Insufficient historical training data)` without fabricating synthetic hit rates. For subsequent batches, association rules derived from prior weeks are evaluated against actual holdout orders.

### H. Apache Kafka's Architectural Role
- **Apache Kafka (4.3.1)** operates in KRaft mode as the distributed event-streaming backbone for real-time transaction ingestion from external frontends.
- **Batch vs. Streaming Distinction**: Kafka serves as the continuous real-time ingestion layer landing events into HDFS `/project-2/raw/`. It is decoupled from, and not executed as part of, the scheduled batch ETL pipeline (`ecommerce` CLI orchestrating Pig on Tez and Hive).

---

## 5. Quick Start & Execution Guide

### 1. Pre-Flight Health Check
Verify the operational health of all services across Hadoop, HDFS, Tez, Pig, and Hive:
```bash
cd ~/ecommerce-bigdata-project
./ecommerce --check
```

### 2. Inspect Batch Status Registry
Review historical batch audit records, clean/reject counts, and processing status:
```bash
./ecommerce --batch-status
```

### 3. Ingest and Process a Batch
Place the raw transaction CSV file in HDFS under `/project-2/raw/<batch_id>/` and execute the orchestrator:
```bash
# 1. Upload dataset to raw landing zone
hdfs dfs -mkdir -p /project-2/raw/2026-W05
hdfs dfs -put /media/sf_shared/ecommerce_transactions_raw_16700.csv /project-2/raw/2026-W05/transactions.csv

# 2. Trigger pipeline execution
./ecommerce /project-2/raw/2026-W05/transactions.csv
```

### 4. Cross-Batch Hive Validation Queries
Execute the automated analytical verification suite:
```bash
./scripts/validate_hive.sh
```

---

## 6. Report Deliverables & HDFS Layout

All analytical products are automatically published to HDFS under `/project-2/reports/`:

| Report Category | HDFS Path | Contents |
| :--- | :--- | :--- |
| **Weekly Analytics** | `/project-2/reports/weekly/<batch_id>/` | `weekly_summary.txt`, sales overview, category breakdowns, customer summaries |
| **Longitudinal Trends** | `/project-2/reports/historical/` | `historical_summary.txt`, `weekly_trend.csv` (WoW metrics), category trends |
| **Product Recommendations** | `/project-2/reports/recommendations/<batch_id>/` | `recommendations.txt`, `recommendations.csv` (Support & Confidence rules) |
| **Recommendation Evaluation** | `/project-2/reports/evaluation/<batch_id>/` | `evaluation.txt`, `evaluation.csv` (Temporal holdout evaluation & Hit Rate) |

---

## 7. Submission Verification Summary

| Metric | Verified Production Value | Description / Source of Truth |
| :--- | :--- | :--- |
| **Batch ID** | `2026-W05` | Production demonstration batch |
| **Raw Records Ingested** | **16,700** | `/project-2/raw/2026-W05/transactions.csv` |
| **Accepted Business Records** | **16,500** | `/project-2/clean/2026-W05` (100% parity with reference dataset) |
| **Rejected Records (Invalid)** | **54** | `/project-2/reject/2026-W05/invalid` (Schema / math violations) |
| **Excluded Duplicates** | **146** | `/project-2/reject/2026-W05/duplicates` (Secondary duplicate occurrences) |
| **Accounting Invariant** | **16,700 = 16,500 + 54 + 146** | Exactly zero silent data loss |
| **Hive Table Records** | **16,500** | `project2.ecommerce_transactions` (`batch_id='2026-W05'`) |
| **Distinct Order IDs** | **16,500** | Verified via `COUNT(DISTINCT order_id)` in Hive |
| **Distinct Transaction IDs** | **16,500** | Verified via `COUNT(DISTINCT transaction_id)` in Hive |
| **Pipeline Status** | **SUCCESS** | Exit Code `0` across all Stages 1 through 7 |

