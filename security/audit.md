# Comprehensive Security, Reliability, Data-Integrity, and Code-Quality Audit
**Project**: Customer Purchase Analysis and Product Recommendation System  
**Date**: 2026-09-23  
**Auditor**: Antigravity AI Assistant  
**Environment**: `data-lab` (Ubuntu 24.04 LTS), User: `hdp`  
**Milestones Covered**: Block 1 (Foundation & Ingestion Baseline) & Block 2 (Pig on Tez Cleaning & Hive Partition Ingestion)

---

## 1. Executive Summary

A comprehensive pre-Block 3 security, reliability, data-integrity, and code-quality audit was performed across all artifacts produced during Blocks 1 and 2 of the E-Commerce Big Data Analytics project. The audit encompassed environment sanity checks, source code static analysis, dynamic boundary validation, shell injection assessment, path traversal verification, batch ID format enforcement, Hive SQL injection scrutiny, Pig parameter safety, temporary file hygiene, and file permission evaluation.

While the core functionality of Block 1 and Block 2 successfully executes the expected happy-path workflows, multiple critical and moderate security vulnerabilities and robustness issues were identified in input validation, path sanitization, and temporary file handling. Specifically:
1. **HDFS Path Traversal**: Input path validation did not constrain input files to the designated raw ingestion root (`/project-2/raw/`) and allowed directory traversal (`../`), introducing risks of arbitrary HDFS read or unintentional data deletion.
2. **Command & Parameter Injection Surface**: Unvalidated input strings containing shell metacharacters (`;`, `&`, `|`, `$`, backticks, single quotes) were accepted by CLI parsing before being passed into HDFS DFS and Pig command invocations.
3. **Batch ID Validation Gaps**: Batch IDs were loosely validated against generic alphanumeric strings (`^[A-Za-z0-9_-]+$`) without enforcing the canonical ISO week format (`YYYY-WNN`), allowing potentially ambiguous or malformed identifiers.
4. **HDFS Output Path Safety**: Deletion logic on retries lacked strict assertion that target paths strictly match `/project-2/clean/<YYYY-WNN>` and `/project-2/reject/<YYYY-WNN>` prior to executing `hdfs dfs -rm -r -skipTrash`.
5. **Hive Dynamic SQL Risk**: Partition registration strings relied on string interpolation into `hive -e` without identifier whitelisting.
6. **Insecure Temporary File Creation**: The batch state registry update used predictable PID-based temporary files (`registry.tsv.tmp.$$`) rather than secure atomic temporary files (`mktemp`).
7. **Permissive Permissions**: Library script `common.sh` had executable bit (`755`), and `registry.tsv` had group-write permissions (`664`).

All safe vulnerabilities have been isolated and remediated with zero impact on existing Hadoop, HDFS, Hive, Pig, and Tez operational configurations.

---

## 2. Infrastructure & Environment Baseline

A non-destructive read-only audit confirmed the operational integrity of the underlying Big Data platform:

| Component | Target Version | Detected Version / Status | Host / Location |
| :--- | :--- | :--- | :--- |
| **Operating System** | Ubuntu 24.04 LTS | Ubuntu 24.04.5 LTS (Kernel `7.0.0-31-generic`) | `data-lab` |
| **Primary User** | `hdp` | `uid=1001(hdp) gid=1001(hdp) groups=1001(hdp),27(sudo),100(users)` | `/home/hdp` |
| **Hadoop Core** | 3.4.1 | Apache Hadoop 3.4.1 (HDFS NameNode + DataNode active) | `/home/hdp/hadoop-3.4.1` |
| **YARN** | 3.4.1 | 1 Active NodeManager (`data-lab:39717`), ResourceManager active | Port 8032 / 8042 |
| **Hive** | 4.2.1 | Apache Hive 4.2.1, Metastore Derby active, JDBC functional | `/home/hdp/apache-hive-4.2.1-bin` |
| **Pig** | 0.18.0 | Apache Pig 0.18.0, Tez execution engine enabled | `/home/hdp/apache-pig-0.18.0` |
| **Tez** | 0.10.5 | Apache Tez 0.10.5, HDFS archive verified at `/apps/tez` | `/home/hdp/apache-tez-0.10.5-bin` |
| **Java (Default)** | Java 17 | OpenJDK 64-Bit Server VM (build 17.0.20+8-1-24.04-Ubuntu) | `/usr/lib/jvm/java-17-openjdk-amd64` |
| **Java (Hive)** | Java 21 | OpenJDK 64-Bit Server VM (build 21.0.8+7-Ubuntu) | `/usr/lib/jvm/java-21-openjdk-amd64` |

### HDFS Cluster Health
- **Capacity**: 72.24 GB configured, 48.78 GB remaining (DFS Used: 0.21%).
- **Replication Status**: 0 under-replicated blocks, 0 corrupt blocks, 0 missing blocks.
- **Root Protection**: Unrelated directories (`/apps`, `/apps/tez`, `/tmp`, `/user`) verified intact and undisturbed.

### HDFS Project Structure (`/project-2`)
- `/project-2` (`drwxr-xr-x`, owner: `hdp:supergroup`)
  - `clean/` (`2026-W98`, `2026-W99` existing partitions with part files and `_SUCCESS`)
  - `raw/` (`_test/2026-W98`, `_test/2026-W99` raw test transactions)
  - `reject/` (`2026-W98`, `2026-W99` rejected records)
  - `reports/` (`evaluation/`, `historical/`, `recommendations/`, `weekly/`)

### Hive Metadata Inspection
- Database `project2` exists.
- Table `project2.ecommerce_transactions` exists:
  - Table Type: `EXTERNAL_TABLE`
  - Storage Location: `hdfs://localhost:9000/user/hive/warehouse/project2.db/ecommerce_transactions`
  - Partition Scheme: `batch_id STRING`
  - Storage SerDe: `LazySimpleSerDe` with `field.delim = \t`
  - Registered Partitions: 2 (`batch_id=2026-W98`, `batch_id=2026-W99`)

---

## 3. Inventory of Audited Files

The following files and directories in `~/ecommerce-bigdata-project/` were thoroughly inspected:

| File / Path | Type | Original Perms | Description |
| :--- | :--- | :--- | :--- |
| `ecommerce` | Bash script | `rwxr-xr-x` (755) | Main orchestrator CLI entrypoint for Blocks 1 & 2 |
| `config/project.conf` | Configuration | `rw-r--r--` (644) | Central HDFS root paths and Hive table/database configuration |
| `scripts/common.sh` | Shell Library | `rwxr-xr-x` (755) | Validation routines, logging, Pig execution, Hive partition loader, registry |
| `scripts/validate_hive.sh` | Bash script | `rwxr-xr-x` (755) | Validation script running analytical verification SQL queries |
| `pig/clean_transactions.pig` | Pig Latin | `rw-r--r--` (644) | Data cleansing, schema casting, and reject routing on Tez |
| `hive/setup_table.sql` | Hive DDL | `rw-r--r--` (644) | DDL script for database and external table creation |
| `hive/validation.sql` | Hive DQL | `rw-r--r--` (644) | Reference validation queries for summary and audit metrics |
| `state/registry.tsv` | TSV Data | `rw-rw-r--` (664) | State registry tracking batch status, record counts, and timestamps |
| `logs/*.log` | Logs | `rw-rw-r--` (664) | Timestamped execution audit logs |
| `documentation/architecture.md` | Markdown | `rw-r--r--` (644) | System architecture and design documentation |

---

## 4. In-Depth Security & Code-Quality Audit Details

### 4.1 Shell Command Injection Assessment
- **Vulnerability**: The main orchestrator script `ecommerce` accepted user input via CLI parameter `$1` or interactive `read -r input_path`. Prior to remediation, `validate_hdfs_input` checked only `[ -z "${input_path}" ]` and `hdfs dfs -test -e "${input_path}"`. If a caller passed metacharacters (e.g. `;`, `&&`, `$()`, backticks), although the variable was double-quoted in bash, no character validation was enforced. If passed to tools or scripts that evaluate arguments (e.g., Pig `-param`), metacharacters posed a severe injection hazard.
- **Remediation**: Implemented strict character allowlisting on all inputs (`^[a-zA-Z0-9_./-]+$`). Any character outside alphanumeric, slash, period, underscore, and hyphen is rejected immediately with exit code 1 before interacting with any external commands or HDFS clients.

### 4.2 HDFS Path Traversal Assessment
- **Vulnerability**: `validate_hdfs_input` verified file existence on HDFS using `hdfs dfs -test -e "${input_path}"`. However, HDFS DFS client resolves relative path tokens like `..`. An input such as `/project-2/raw/../clean/2026-W98/part-v000-o000-r-00000` or `/tmp/transactions.csv` evaluated as existent. When `ecommerce` subsequently extracted the batch ID, it would target existing clean partitions, causing `run_pig_cleaning` to delete and overwrite production clean data.
- **Remediation**:
  1. Enforced strict landing zone boundary check: `input_path` must strictly begin with `${RAW_ROOT}/` (`/project-2/raw/`).
  2. Forbidden traversal sequences: Explicitly rejected any path containing `..`, `/../`, or `../`.
  3. Validated that the path references an authorized `.csv` file or directory inside the raw landing zone.

### 4.3 Batch ID Format & Validation
- **Vulnerability**: `ecommerce` used a loose regular expression `^[A-Za-z0-9_-]+$` to validate batch IDs. This allowed arbitrary strings (e.g., `evil`, `test_batch`, `foo-123`) that do not conform to the project standard. Additionally, `extract_batch_id` did not normalize single-digit weeks to two digits (e.g., `2026-W5` remained `2026-W5` instead of `2026-W05`).
- **Remediation**:
  1. Standardized batch ID format to strict ISO week notation `YYYY-WNN` (`^[0-9]{4}-W[0-9]{2}$`).
  2. Enhanced `extract_batch_id` to automatically zero-pad single-digit week numbers (e.g., `2026-W5` -> `2026-W05`).
  3. Added explicit validator `validate_batch_id` rejecting any value not conforming to `^[0-9]{4}-W[0-9]{2}$`.

### 4.4 HDFS Output Path Derivation Safety
- **Vulnerability**: In `run_pig_cleaning`, `clean_out` and `reject_out` were constructed as `${CLEAN_ROOT}/${batch_id}` and `${REJECT_ROOT}/${batch_id}`. If variables were blank or corrupted, `hdfs dfs -rm -r -skipTrash "${clean_out}"` could target `/project-2/clean` or root `/`.
- **Remediation**: Added `validate_output_paths()` helper. It asserts that:
  - `clean_out` strictly matches `^/project-2/clean/[0-9]{4}-W[0-9]{2}$`.
  - `reject_out` strictly matches `^/project-2/reject/[0-9]{4}-W[0-9]{2}$`.
  If either path does not match the exact pattern, the script immediately aborts before invoking any HDFS deletion or Pig execution.

### 4.5 Hive Dynamic SQL Injection
- **Vulnerability**: `load_hive_partition` constructed DDL using string interpolation:
  `hive -e "USE ${HIVE_DATABASE}; ALTER TABLE ${HIVE_TABLE} ADD IF NOT EXISTS PARTITION (batch_id = '${batch_id}') LOCATION '${clean_out}';"`
  If `batch_id`, `HIVE_DATABASE`, or `HIVE_TABLE` contained SQL delimiters (e.g. `'; DROP TABLE ...`), arbitrary DDL could be executed.
- **Remediation**:
  - Validated `HIVE_DATABASE` and `HIVE_TABLE` against identifier allowlist `^[a-zA-Z0-9_]+$`.
  - Validated `batch_id` against `^[0-9]{4}-W[0-9]{2}$`.
  - Validated `clean_out` against `^/project-2/clean/[0-9]{4}-W[0-9]{2}$`.
  - Validated `expected_clean_records` against integer allowlist `^[0-9]+$`.
  - Applied the same identifier validations in `scripts/validate_hive.sh`.

### 4.6 Temporary File Security
- **Vulnerability**: In `scripts/common.sh`, `update_batch_status()` used `local tmp_file="${reg_file}.tmp.$$"`. The PID `$$` is predictable, making the temporary file susceptible to pre-creation symlink attacks or race conditions if multiple processes run concurrently.
- **Remediation**:
  - Replaced predictable filename with `mktemp "${state_dir}/registry.tmp.XXXXXX"`.
  - Enforced restrictive permissions `chmod 600 "${tmp_file}"` during update.
  - Implemented cleanup logic to ensure temporary files are cleanly removed on completion or error.

### 4.7 File Permissions
- **Vulnerability**:
  - `scripts/common.sh` had executable mode `755` despite being a library file designed exclusively to be sourced.
  - `state/registry.tsv` had permissions `664` allowing group write access.
- **Remediation**:
  - Adjusted `scripts/common.sh` permissions to `644`.
  - Maintained `755` on executables `ecommerce` and `scripts/validate_hive.sh`.
  - Adjusted `state/registry.tsv` to `644`.
  - Verified no files or directories in the project are world-writable.

---

## 5. Summary Matrix of Findings & Actions

See companion document `security/findings.md` for full technical breakdown and severity classifications, and `security/test-results.md` for dynamic verification results.
