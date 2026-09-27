# Security, Reliability, and Code-Quality Audit Findings
**Project**: Customer Purchase Analysis and Product Recommendation System  
**Date**: 2026-09-23  
**Status**: All Safe Problems Remediated & Verified  

---

## 1. Findings Overview & Severity Distribution

| Severity | Count | Remediated | Pending |
| :--- | :---: | :---: | :---: |
| **Critical** | 2 | 2 | 0 |
| **High** | 3 | 3 | 0 |
| **Medium** | 2 | 2 | 0 |
| **Low** | 2 | 2 | 0 |
| **Total** | **9** | **9** | **0** |

---

## 2. Detailed Findings

---

### FINDING-01: HDFS Path Traversal & Landing Zone Boundary Bypass
- **Severity**: Critical
- **Component**: `scripts/common.sh` -> `validate_hdfs_input()`
- **Impact**: Arbitrary HDFS path ingestion, unintended deletion of production partitions during retry/reprocessing.
- **Description**:
  The function `validate_hdfs_input()` checked only whether the input path exists via `hdfs dfs -test -e "${input_path}"`. It failed to verify that the path was located inside the designated raw data landing zone (`/project-2/raw/`). Furthermore, it did not check for path traversal patterns (`..`). Because HDFS resolves relative traversal sequences, a user could supply `/project-2/raw/../clean/2026-W98/part-v000-o000-r-00000` or `/tmp/evil.csv`. If an existing clean partition was targeted, subsequent logic in `run_pig_cleaning` could delete clean data (`hdfs dfs -rm -r -skipTrash /project-2/clean/...`) during cleanup!
- **Remediation**:
  1. Enforced strict character allowlisting (`^[a-zA-Z0-9_./-]+$`).
  2. Prohibited any occurrence of `..` (including `/../`, `../`, `..`).
  3. Enforced boundary constraint that `input_path` must strictly start with `${RAW_ROOT}/` (`/project-2/raw/`).
- **Status**: Remediated & Verified.

---

### FINDING-02: Shell Metacharacter & Command Injection Hazard on Input Paths
- **Severity**: Critical
- **Component**: `ecommerce`, `scripts/common.sh` -> `validate_hdfs_input()`
- **Impact**: Potential shell or subcommand injection if user inputs are expanded or passed to unquoted commands, subshells, or logging sinks.
- **Description**:
  The `ecommerce` CLI accepted input via command-line arguments (`$1`) or `read -r input_path`. While arguments were passed inside quotes in several shell commands, raw unvalidated input could still be passed into `-param` in Pig, or into logging functions. Malicious inputs like `/project-2/raw/test.csv; echo INJECTION_TEST` or `/project-2/raw/test.csv$(echo INJECTION_TEST)` were not rejected at the entrypoint, allowing dangerous metacharacters through to downstream operations.
- **Remediation**:
  Added immediate regex validation for `input_path` at the start of `validate_hdfs_input`:
  ```bash
  if [[ ! "${input_path}" =~ ^[a-zA-Z0-9_./-]+$ ]]; then
      log_error "Input path contains forbidden characters."
      return 1
  fi
  ```
  Any metacharacters (`;`, `&`, `|`, `$`, `` ` ``, `(`, `)`, `{`, `}`, `\`, `'`, `"`, whitespace, control chars) are immediately rejected with an explicit error.
- **Status**: Remediated & Verified.

---

### FINDING-03: Arbitrary and Malformed Batch ID Format Acceptance
- **Severity**: High
- **Component**: `ecommerce`, `scripts/common.sh` -> `extract_batch_id()`
- **Impact**: Data corruption, malformed HDFS partition folders, failure of downstream reporting queries.
- **Description**:
  The validation regex for user-supplied batch IDs in `ecommerce` was `^[A-Za-z0-9_-]+$`. This allowed arbitrary strings such as `evil`, `test_batch`, or `12345`. Furthermore, `extract_batch_id` matched `([0-9]{4}[-_]?[wW][0-9]{1,2})` but did not normalize single-digit weeks to standard two digits (e.g. `2026-W5` remained `2026-W5` instead of `2026-W05`).
- **Remediation**:
  1. Updated `extract_batch_id` to normalize year and week to standard two-digit week representation (`printf "%04d-W%02d"`).
  2. Added dedicated validator `validate_batch_id()` enforcing the strict ISO week pattern `^[0-9]{4}-W[0-9]{2}$`.
  3. Prohibited any non-conforming batch IDs from entering the pipeline.
- **Status**: Remediated & Verified.

---

### FINDING-04: Unasserted HDFS Output Path Safety Prior to Deletion
- **Severity**: High
- **Component**: `scripts/common.sh` -> `run_pig_cleaning()`
- **Impact**: Accidental deletion of HDFS clean root, project root, or filesystem root.
- **Description**:
  In `run_pig_cleaning()`, lines 537 and 541 executed:
  `hdfs dfs -rm -r -skipTrash "${clean_out}"`
  `hdfs dfs -rm -r -skipTrash "${reject_out}"`
  where `clean_out="${CLEAN_ROOT}/${batch_id}"`. If `CLEAN_ROOT` or `batch_id` were corrupted, empty, or misconfigured (e.g. `CLEAN_ROOT=""` or `batch_id="/"`), the command would resolve to `hdfs dfs -rm -r -skipTrash /`, causing catastrophic data loss.
- **Remediation**:
  Created a mandatory guard `validate_output_paths()` called before any HDFS deletion or output creation:
  - Asserts `CLEAN_ROOT` is strictly `/project-2/clean`.
  - Asserts `REJECT_ROOT` is strictly `/project-2/reject`.
  - Asserts `clean_out` strictly matches `^/project-2/clean/[0-9]{4}-W[0-9]{2}$`.
  - Asserts `reject_out` strictly matches `^/project-2/reject/[0-9]{4}-W[0-9]{2}$`.
  Execution is aborted immediately if paths do not strictly match this pattern.
- **Status**: Remediated & Verified.

---

### FINDING-05: Dynamic Hive SQL Injection Risk via Table, Database, and Partition Parameters
- **Severity**: High
- **Component**: `scripts/common.sh` -> `load_hive_partition()`, `scripts/validate_hive.sh`
- **Impact**: SQL injection / DDL alteration via malicious batch IDs or configuration injection.
- **Description**:
  `load_hive_partition()` and `validate_hive.sh` interpolated shell variables directly into `hive -e` strings:
  `hive -e "USE ${HIVE_DATABASE}; ALTER TABLE ${HIVE_TABLE} ADD IF NOT EXISTS PARTITION (batch_id = '${batch_id}') LOCATION '${clean_out}';"`
  If `batch_id` contained single quotes or SQL command terminators (e.g. `'; DROP TABLE project2.ecommerce_transactions; --`), arbitrary SQL statements could be executed by Hive.
- **Remediation**:
  1. Enforced strict identifier allowlist `^[a-zA-Z0-9_]+$` on `HIVE_DATABASE` and `HIVE_TABLE`.
  2. Enforced strict allowlist `^[0-9]{4}-W[0-9]{2}$` on `batch_id`.
  3. Enforced integer allowlist `^[0-9]+$` on record counts.
  4. Enforced strict path allowlist on `clean_out`.
- **Status**: Remediated & Verified.

---

### FINDING-06: Pig Parameter Injection Risk
- **Severity**: Medium
- **Component**: `scripts/common.sh` -> `run_pig_cleaning()`
- **Impact**: Parameter escaping, syntax errors, or Pig Latin command alteration.
- **Description**:
  Pig was invoked with `-param INPUT_PATH="${input_path}"` where `$INPUT_PATH` is placed inside quotes in `clean_transactions.pig` (`LOAD '$INPUT_PATH'`). If an input path contained single quotes, spaces, or macro characters, Pig's preprocessor could be tricked into parsing injected Pig Latin commands.
- **Remediation**:
  Because `input_path`, `clean_out`, and `reject_out` are strictly validated against restrictive allowlists prohibiting single quotes, double quotes, spaces, and metacharacters, the Pig parameter injection vector is fully closed.
- **Status**: Remediated & Verified.

---

### FINDING-07: Insecure Predictable Temporary File Generation
- **Severity**: Medium
- **Component**: `scripts/common.sh` -> `update_batch_status()`
- **Impact**: Race conditions, symlink overwrite attacks, file collision.
- **Description**:
  `update_batch_status()` generated temporary files using:
  `local tmp_file="${reg_file}.tmp.$$"`
  Because PID `$$` is easily predictable, a local attacker could pre-create symlinks targeting other files, leading to unauthorized overwrites or race conditions.
- **Remediation**:
  Updated `update_batch_status()` to use `mktemp`:
  ```bash
  local tmp_file
  tmp_file="$(mktemp "${state_dir}/registry.tmp.XXXXXX")"
  chmod 600 "${tmp_file}"
  ```
  Guaranteed cleanup using a trap in the event of an abnormal exit.
- **Status**: Remediated & Verified.

---

### FINDING-08: Excess File and Directory Permissions
- **Severity**: Low
- **Component**: `scripts/common.sh`, `state/registry.tsv`
- **Impact**: Unnecessary executable permissions on sourced library; group-write access on state registry.
- **Description**:
  - `scripts/common.sh` had executable bit (`755`), although it is a bash library intended solely to be sourced by other scripts.
  - `state/registry.tsv` had permissions `664`, permitting group modification.
- **Remediation**:
  - Adjusted `scripts/common.sh` to `644`.
  - Adjusted `state/registry.tsv` to `644`.
  - Executable permissions were confirmed intact on entrypoints `ecommerce` (`755`) and `scripts/validate_hive.sh` (`755`).
- **Status**: Remediated & Verified.

---

### FINDING-09: Unbound Variable / Empty Count Arithmetic Risk
- **Severity**: Low
- **Component**: `scripts/common.sh` -> `count_batch_records()`
- **Impact**: Bash arithmetic error under `set -e` if HDFS count yields an empty string.
- **Description**:
  In `count_batch_records()`, line 592 performed `input_cnt=$((clean_cnt + reject_cnt))`. If either `clean_cnt` or `reject_cnt` returned an unexpected empty output, the arithmetic expansion `$(( + ))` would fail with a syntax error, terminating the script prematurely.
- **Remediation**:
  Supplied defensive parameter defaults:
  `input_cnt=$(( ${clean_cnt:-0} + ${reject_cnt:-0} ))`
- **Status**: Remediated & Verified.

---

## 3. Summary of Remediations Applied

All identified issues have been fixed safely in `scripts/common.sh`, `ecommerce`, `scripts/validate_hive.sh`, and filesystem permissions. See `security/test-results.md` for full test logs.
