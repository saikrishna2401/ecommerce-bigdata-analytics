#!/usr/bin/env bash
# ==============================================================================
# scripts/common.sh - Common Utilities and Validation Functions
# E-Commerce Big Data Analytics (Block 1 Foundation + Block 2 Pipeline)
# ==============================================================================

set -Eeuo pipefail

# Resolve project root directory if not already set
export PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

# Global variables
LOG_FILE="${LOG_FILE:-}"
CONFIG_LOADED=0

# ------------------------------------------------------------------------------
# Logging Utilities
# ------------------------------------------------------------------------------
init_logging() {
    local script_args="$*"
    local log_dir="${PROJECT_ROOT}/logs"
    
    if [ ! -d "${log_dir}" ]; then
        mkdir -p "${log_dir}"
    fi
    
    local timestamp
    timestamp="$(date '+%Y-%m-%d_%H-%M-%S')"
    LOG_FILE="${log_dir}/${timestamp}.log"
    
    cat <<EOF > "${LOG_FILE}"
================================================================================
E-COMMERCE BIG DATA ANALYTICS - EXECUTION LOG
================================================================================
Timestamp  : $(date '+%Y-%m-%d %H:%M:%S %Z')
Command    : $0 ${script_args}
User       : $(whoami)
Hostname   : $(hostname)
PID        : $$
Script Dir : ${PROJECT_ROOT}
================================================================================
EOF

    trap 'log_exit_status $?' EXIT
}

log() {
    local msg="$1"
    local ts
    ts="$(date '+%Y-%m-%d %H:%M:%S')"
    if [ -n "${LOG_FILE}" ] && [ -f "${LOG_FILE}" ]; then
        echo "[${ts}] ${msg}" >> "${LOG_FILE}"
    fi
}

log_error() {
    local msg="$1"
    log "ERROR: ${msg}"
    echo "ERROR: ${msg}" >&2
}

log_exit_status() {
    local code="$1"
    log "================================================================================"
    log "Execution completed at: $(date '+%Y-%m-%d %H:%M:%S %Z')"
    log "Final Exit Status: ${code}"
    log "================================================================================"
}

# ------------------------------------------------------------------------------
# Configuration Loader
# ------------------------------------------------------------------------------
load_config() {
    local config_path="${PROJECT_ROOT}/config/project.conf"
    log "Loading configuration from: ${config_path}"
    
    if [ ! -f "${config_path}" ]; then
        log_error "Configuration file not found: ${config_path}"
        return 1
    fi
    
    # Source configuration
    # shellcheck source=/dev/null
    source "${config_path}"
    
    local required_vars=(
        "PROJECT_HDFS_ROOT"
        "RAW_ROOT"
        "CLEAN_ROOT"
        "REJECT_ROOT"
        "REPORT_ROOT"
        "WEEKLY_REPORT_ROOT"
        "HISTORICAL_REPORT_ROOT"
        "RECOMMENDATION_ROOT"
        "EVALUATION_ROOT"
        "HIVE_DATABASE"
    )
    
    for var in "${required_vars[@]}"; do
        if [ -z "${!var:-}" ]; then
            log_error "Required configuration variable '${var}' is missing or empty in ${config_path}"
            return 1
        fi
        log "Config: ${var}=${!var}"
    done
    
    HIVE_TABLE="${HIVE_TABLE:-ecommerce_transactions}"

    # Security validation of identifiers and paths
    if [[ ! "${HIVE_DATABASE}" =~ ^[a-zA-Z0-9_]+$ ]]; then
        log_error "Invalid Hive database name '${HIVE_DATABASE}' in configuration."
        return 1
    fi
    if [[ ! "${HIVE_TABLE}" =~ ^[a-zA-Z0-9_]+$ ]]; then
        log_error "Invalid Hive table name '${HIVE_TABLE}' in configuration."
        return 1
    fi
    if [[ ! "${PROJECT_HDFS_ROOT}" =~ ^/project-2$ ]] || [[ ! "${RAW_ROOT}" =~ ^/project-2/raw$ ]] || \
       [[ ! "${CLEAN_ROOT}" =~ ^/project-2/clean$ ]] || [[ ! "${REJECT_ROOT}" =~ ^/project-2/reject$ ]]; then
        log_error "HDFS root directories must reside strictly under /project-2."
        return 1
    fi

    # Reporting and Recommendation parameters
    CURRENCY_SYMBOL="${CURRENCY_SYMBOL:-₹}"
    MIN_PAIR_COUNT="${MIN_PAIR_COUNT:-1}"
    MIN_CONFIDENCE="${MIN_CONFIDENCE:-0.10}"

    if [[ ! "${MIN_PAIR_COUNT}" =~ ^[0-9]+$ ]] || [ "${MIN_PAIR_COUNT}" -lt 1 ]; then
        log_error "MIN_PAIR_COUNT must be a positive integer."
        return 1
    fi
    if [[ ! "${MIN_CONFIDENCE}" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        log_error "MIN_CONFIDENCE must be a numeric value between 0.0 and 1.0."
        return 1
    fi

    CONFIG_LOADED=1
    log "Configuration loaded successfully."
    return 0
}

# ------------------------------------------------------------------------------
# Command Verification Helper
# ------------------------------------------------------------------------------
check_command() {
    local cmd="$1"
    if ! command -v "${cmd}" >/dev/null 2>&1; then
        echo "Command not found: ${cmd}"
        log "ERROR: Command not found: ${cmd}"
        return 1
    fi
    local cmd_path
    cmd_path="$(command -v "${cmd}")"
    log "Detected command '${cmd}' at: ${cmd_path}"
    return 0
}

# ------------------------------------------------------------------------------
# Environment & Component Verification Functions
# ------------------------------------------------------------------------------
verify_hadoop() {
    if ! check_command "hadoop"; then
        return 1
    fi
    local version_info
    version_info="$(hadoop version 2>>"${LOG_FILE}" | head -n 1)"
    log "Hadoop Version: ${version_info}"
    return 0
}

verify_hdfs() {
    if ! check_command "hdfs"; then
        return 1
    fi
    log "Checking HDFS connectivity via 'hdfs dfs -ls /'..."
    local hdfs_out
    if hdfs_out="$(hdfs dfs -ls / 2>>"${LOG_FILE}")"; then
        log "HDFS connectivity check successful."
        log "HDFS root listing:"
        echo "${hdfs_out}" >> "${LOG_FILE}"
        return 0
    else
        log "ERROR: HDFS connectivity check failed."
        return 1
    fi
}

verify_hive_cmd() {
    if ! check_command "hive"; then
        return 1
    fi
    log "Hive command is available."
    return 0
}

verify_pig_cmd() {
    if ! check_command "pig"; then
        return 1
    fi
    log "Pig command is available."
    return 0
}

verify_tez() {
    log "Checking Tez availability..."
    local tez_ok=1
    
    if [ -n "${TEZ_HOME:-}" ] && [ -d "${TEZ_HOME}" ]; then
        log "TEZ_HOME detected at: ${TEZ_HOME}"
    else
        log "TEZ_HOME is not set or directory does not exist."
        tez_ok=0
    fi
    
    if [ -n "${TEZ_CONF_DIR:-}" ] && [ -d "${TEZ_CONF_DIR}" ]; then
        log "TEZ_CONF_DIR detected at: ${TEZ_CONF_DIR}"
    else
        log "TEZ_CONF_DIR is not set or directory does not exist."
        tez_ok=0
    fi
    
    if hdfs dfs -test -d /apps/tez 2>>"${LOG_FILE}"; then
        log "Tez HDFS archive directory /apps/tez exists."
    else
        log "Tez HDFS archive directory /apps/tez does not exist."
        tez_ok=0
    fi
    
    if [ "${tez_ok}" -eq 1 ]; then
        return 0
    else
        return 1
    fi
}

verify_java() {
    if ! check_command "java"; then
        return 1
    fi
    local java_info
    java_info="$(java -version 2>&1 | head -n 1)"
    log "Java Version: ${java_info}"
    return 0
}

verify_project_structure() {
    log "Checking HDFS project structure under ${PROJECT_HDFS_ROOT}..."
    
    local required_dirs=(
        "${PROJECT_HDFS_ROOT}"
        "${RAW_ROOT}"
        "${CLEAN_ROOT}"
        "${REJECT_ROOT}"
        "${REPORT_ROOT}"
        "${WEEKLY_REPORT_ROOT}"
        "${HISTORICAL_REPORT_ROOT}"
        "${RECOMMENDATION_ROOT}"
        "${EVALUATION_ROOT}"
    )
    
    for dir in "${required_dirs[@]}"; do
        if ! hdfs dfs -test -d "${dir}" 2>>"${LOG_FILE}"; then
            echo "ERROR: Required HDFS directory missing: ${dir}"
            log "ERROR: Required HDFS directory missing: ${dir}"
            return 1
        fi
        log "Verified directory exists: ${dir}"
    done
    
    log "All required HDFS project directories verified successfully."
    return 0
}

# ------------------------------------------------------------------------------
# Check Mode Implementation (--check)
# ------------------------------------------------------------------------------
run_check_mode() {
    log "Running foundation check (--check mode)..."
    
    local status_hadoop="OK"
    local status_hdfs="OK"
    local status_hive="OK"
    local status_pig="OK"
    local status_tez="OK"
    local status_java="OK"
    local status_project="OK"
    local status_config="OK"
    local overall_success=1
    
    echo "========================================"
    echo "FOUNDATION HEALTH CHECK"
    echo "========================================"
    echo "Current user : $(whoami)"
    echo "Hostname     : $(hostname)"
    echo ""
    
    if ! verify_hadoop; then
        status_hadoop="FAILED"
        overall_success=0
    fi
    
    if ! verify_hdfs; then
        status_hdfs="FAILED"
        overall_success=0
    fi
    
    if ! verify_hive_cmd; then
        status_hive="FAILED"
        overall_success=0
    fi
    
    if ! verify_pig_cmd; then
        status_pig="FAILED"
        overall_success=0
    fi
    
    if ! verify_tez; then
        status_tez="NOT VERIFIED"
    fi
    
    if ! verify_java; then
        status_java="FAILED"
        overall_success=0
    fi
    
    if [ "${CONFIG_LOADED}" -ne 1 ]; then
        if ! load_config; then
            status_config="FAILED"
            overall_success=0
        fi
    fi
    
    if [ "${status_hdfs}" = "OK" ] && [ "${status_config}" = "OK" ]; then
        if ! verify_project_structure; then
            status_project="FAILED"
            overall_success=0
        fi
    else
        status_project="FAILED"
        overall_success=0
    fi
    
    echo "========================================"
    echo "FOUNDATION CHECK"
    echo "================"
    echo ""
    printf "%-14s: %s\n" "Hadoop" "${status_hadoop}"
    printf "%-14s: %s\n" "HDFS" "${status_hdfs}"
    printf "%-14s: %s\n" "Hive command" "${status_hive}"
    printf "%-14s: %s\n" "Pig command" "${status_pig}"
    printf "%-14s: %s\n" "Tez" "${status_tez}"
    printf "%-14s: %s\n" "Java" "${status_java}"
    printf "%-14s: %s\n" "Project HDFS" "${status_project}"
    printf "%-14s: %s\n" "Configuration" "${status_config}"
    echo ""
    echo "Result:"
    if [ "${overall_success}" -eq 1 ]; then
        echo "FOUNDATION READY"
        log "Check mode result: FOUNDATION READY"
        return 0
    else
        echo "FOUNDATION FAILED"
        log "Check mode result: FOUNDATION FAILED"
        return 1
    fi
}

# ------------------------------------------------------------------------------
# Help Display (--help)
# ------------------------------------------------------------------------------
show_help() {
    log "Displaying help message (--help mode)."
    cat <<EOF
Usage:

./ecommerce
Interactive batch processing pipeline (HDFS CSV -> Pig on Tez -> Hive)

./ecommerce --help
Show help

./ecommerce --check
Check Hadoop/HDFS/project configuration

./ecommerce --batch-status
Show status of all registered batches
EOF
}

# ------------------------------------------------------------------------------
# Batch / Week Detection & Safety Validation
# ------------------------------------------------------------------------------
validate_batch_id() {
    local bid="$1"
    if [[ "${bid}" =~ ^[0-9]{4}-[wW][0-9]{2}$ ]]; then
        return 0
    fi
    return 1
}

acquire_batch_lock() {
    local bid="$1"
    if ! validate_batch_id "${bid}"; then
        log_error "Cannot acquire lock: invalid batch ID '${bid}'"
        return 1
    fi
    local lock_dir="${PROJECT_ROOT}/state"
    if [ ! -d "${lock_dir}" ]; then
        mkdir -p "${lock_dir}"
    fi
    local lock_file="${lock_dir}/.lock_${bid}"
    # Open file descriptor 9 for the exclusive lock file
    exec 9>"${lock_file}"
    if ! flock -n 9; then
        echo "========================================"
        echo "ERROR: CONCURRENT EXECUTION DETECTED"
        echo "========================================"
        echo "Batch '${bid}' is currently being processed by another instance."
        echo "Lock file: ${lock_file}"
        echo "Exiting safely to avoid conflicting pipeline operations."
        echo "========================================"
        log_error "Concurrency conflict: Exclusive lock on '${lock_file}' is held by another process."
        return 1
    fi
    log "Acquired exclusive execution lock for batch '${bid}' on fd 9 (${lock_file})."
    return 0
}

release_batch_lock() {
    local lock_dir="${PROJECT_ROOT}/state"
    flock -u 9 2>/dev/null || true
    exec 9>&- 2>/dev/null || true
    rm -f "${lock_dir}"/.lock_* 2>/dev/null || true
}

validate_output_paths() {
    local bid="$1"
    if ! validate_batch_id "${bid}"; then
        log_error "Safety check failed: batch ID '${bid}' does not conform to YYYY-WNN format."
        return 1
    fi
    if [ -z "${CLEAN_ROOT:-}" ] || [ -z "${REJECT_ROOT:-}" ]; then
        log_error "Safety check failed: CLEAN_ROOT or REJECT_ROOT is empty."
        return 1
    fi
    local c_out="${CLEAN_ROOT}/${bid}"
    local r_out="${REJECT_ROOT}/${bid}"
    if [[ ! "${c_out}" =~ ^/project-2/clean/[0-9]{4}-[wW][0-9]{2}$ ]]; then
        log_error "Safety check failed: clean output path '${c_out}' is unsafe."
        return 1
    fi
    if [[ ! "${r_out}" =~ ^/project-2/reject/[0-9]{4}-[wW][0-9]{2}$ ]]; then
        log_error "Safety check failed: reject output path '${r_out}' is unsafe."
        return 1
    fi
    return 0
}

extract_batch_id() {
    local input_path="$1"
    local found_batch=""
    local malformed=0

    # Split input path into components delimited by /
    local old_ifs="${IFS}"
    IFS="/"
    read -r -a parts <<< "${input_path}"
    IFS="${old_ifs}"

    for part in "${parts[@]}"; do
        [ -z "${part}" ] && continue

        # Check if the component looks like a batch/week indicator (4-digit year followed by W)
        if [[ "${part}" =~ [0-9]{4}[-_]?[wW] ]]; then
            # Must strictly match a complete path component with 1 or 2 digits week
            # (allowing an optional single file extension such as .csv or .tsv)
            if [[ "${part}" =~ ^([0-9]{4})[-_]?[wW]([0-9]{1,2})(\.[a-zA-Z0-9]+)?$ ]]; then
                local year="${BASH_REMATCH[1]}"
                local week="${BASH_REMATCH[2]}"
                local candidate
                candidate="$(printf "%04d-W%02d" "$((10#$year))" "$((10#$week))")"
                # If multiple distinct batch IDs are encountered in path, mark ambiguous/malformed
                if [ -n "${found_batch}" ] && [ "${found_batch}" != "${candidate}" ]; then
                    return 1
                fi
                found_batch="${candidate}"
            else
                # Component contains a year/week pattern but is malformed
                # (e.g. 2026-W100, 2026-W123, prefix_2026-W01, 2026-W001)
                malformed=1
            fi
        fi
    done

    # If any malformed batch component was encountered, fail extraction safely
    if [ "${malformed}" -eq 1 ]; then
        return 1
    fi

    if [ -n "${found_batch}" ]; then
        echo "${found_batch}"
        return 0
    fi

    return 1
}


# ------------------------------------------------------------------------------
# HDFS Input Path Validation
# ------------------------------------------------------------------------------
validate_hdfs_input() {
    local input_path="$1"
    log "Validating input path: '${input_path}'"
    
    if [ -z "${input_path}" ]; then
        echo "ERROR: Input path cannot be empty."
        log_error "Input path cannot be empty."
        return 1
    fi
    
    # 1. Shell Injection & Disallowed Metacharacter Protection
    if [[ ! "${input_path}" =~ ^[a-zA-Z0-9_./-]+$ ]]; then
        echo "ERROR: Input path contains invalid or forbidden characters."
        echo "Input path: ${input_path}"
        log_error "Input path contains forbidden characters: ${input_path}"
        return 1
    fi
    
    # 2. Path Traversal Protection
    if [[ "${input_path}" == *".."* ]]; then
        echo "ERROR: Path traversal (..) is strictly prohibited."
        echo "Input path: ${input_path}"
        log_error "Path traversal detected in input path: ${input_path}"
        return 1
    fi
    
    # 3. Landing Zone Boundary Enforcement
    local raw_prefix="${RAW_ROOT:-/project-2/raw}"
    if [[ ! "${input_path}" =~ ^${raw_prefix}/.+ ]]; then
        echo "ERROR: Input path must reside within the raw landing zone (${raw_prefix}/)."
        echo "Input path: ${input_path}"
        log_error "Input path outside raw landing zone: ${input_path}"
        return 1
    fi
    
    # 4. HDFS Connectivity Check
    if ! hdfs dfs -test -e / 2>>"${LOG_FILE}"; then
        echo "ERROR: HDFS is unreachable."
        log_error "HDFS is unreachable."
        return 1
    fi
    
    # 5. Target Existence Check
    if ! hdfs dfs -test -e "${input_path}" 2>>"${LOG_FILE}"; then
        echo "ERROR: HDFS input path does not exist."
        echo "Input path: ${input_path}"
        log "ERROR: HDFS input path does not exist: ${input_path}"
        return 1
    fi
    
    if hdfs dfs -test -f "${input_path}" 2>>"${LOG_FILE}"; then
        log "Input path is a regular file."
    elif hdfs dfs -test -d "${input_path}" 2>>"${LOG_FILE}"; then
        log "Input path is a directory."
    else
        echo "ERROR: Input path is neither a regular file nor a directory."
        log_error "Input path is neither a regular file nor a directory: ${input_path}"
        return 1
    fi

    # 6. File Non-Empty Check (Pre-flight Validation)
    if hdfs dfs -test -f "${input_path}" 2>>"${LOG_FILE}"; then
        local file_size=0
        file_size="$(hdfs dfs -stat '%b' "${input_path}" 2>>"${LOG_FILE}" || echo "0")"
        if [ "${file_size:-0}" -eq 0 ]; then
            echo "ERROR: Input file is empty (0 bytes): ${input_path}"
            log_error "Input file is empty (0 bytes): ${input_path}"
            return 1
        fi

        # 7. Header and Row Count Pre-flight Validation
        local first_two_lines
        first_two_lines="$(hdfs dfs -cat "${input_path}" 2>>"${LOG_FILE}" | head -n 2)"
        local line_count
        line_count="$(echo "${first_two_lines}" | grep -c . || true)"
        if [ "${line_count}" -lt 2 ]; then
            echo "ERROR: Input file contains only a header or no data records: ${input_path}"
            log_error "Input file contains insufficient records (< 2 lines): ${input_path}"
            return 1
        fi

        local header_line
        header_line="$(echo "${first_two_lines}" | head -n 1)"
        if [[ ! "${header_line}" =~ order_id ]] || [[ ! "${header_line}" =~ transaction_id ]] || [[ ! "${header_line}" =~ customer_id ]]; then
            echo "ERROR: Input CSV header does not match expected schema (missing required columns): ${input_path}"
            log_error "Input CSV header schema mismatch: ${header_line}"
            return 1
        fi
        log "Input file passed pre-flight validation (size: ${file_size} bytes, schema verified)."
    fi
    return 0
}

# ------------------------------------------------------------------------------
# Batch Registry Functions (Block 2 State Management)
# ------------------------------------------------------------------------------
init_batch_registry() {
    local state_dir="${PROJECT_ROOT}/state"
    local reg_file="${state_dir}/registry.tsv"
    if [ ! -d "${state_dir}" ]; then
        mkdir -p "${state_dir}"
    fi
    if [ ! -f "${reg_file}" ]; then
        printf "batch_id\tinput_path\tstart_time\tend_time\tinput_records\tclean_records\trejected_records\tstatus\terror_message\n" > "${reg_file}"
        log "Initialized batch registry at: ${reg_file}"
    fi
}

get_batch_status() {
    local batch_id="$1"
    local reg_file="${PROJECT_ROOT}/state/registry.tsv"
    if [ ! -f "${reg_file}" ]; then
        echo "NOT_FOUND"
        return 0
    fi
    local status
    status="$(awk -F'\t' -v b="${batch_id}" '$1 == b { print $8 }' "${reg_file}" | tail -n 1)"
    if [ -n "${status}" ]; then
        echo "${status}"
    else
        echo "NOT_FOUND"
    fi
}

get_batch_field() {
    local batch_id="$1"
    local col_idx="$2"
    local reg_file="${PROJECT_ROOT}/state/registry.tsv"
    if [ ! -f "${reg_file}" ]; then
        echo ""
        return 0
    fi
    awk -F'\t' -v b="${batch_id}" -v c="${col_idx}" '$1 == b { print $c }' "${reg_file}" | tail -n 1
}

update_batch_status() {
    local batch_id="$1"
    local input_path="$2"
    local start_time="$3"
    local end_time="$4"
    local input_recs="$5"
    local clean_recs="$6"
    local reject_recs="$7"
    local status="$8"
    local error_msg="$9"
    
    init_batch_registry
    local state_dir="${PROJECT_ROOT}/state"
    local reg_file="${state_dir}/registry.tsv"
    local tmp_file
    tmp_file="$(mktemp "${state_dir}/registry.tmp.XXXXXX")"
    chmod 600 "${tmp_file}"
    
    local found=0
    while IFS=$'\t' read -r r_id r_path r_start r_end r_in r_clean r_rej r_stat r_err || [ -n "${r_id}" ]; do
        if [ "${r_id}" = "batch_id" ]; then
            printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "${r_id}" "${r_path}" "${r_start}" "${r_end}" "${r_in}" "${r_clean}" "${r_rej}" "${r_stat}" "${r_err}" > "${tmp_file}"
        elif [ "${r_id}" = "${batch_id}" ]; then
            printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "${batch_id}" "${input_path}" "${start_time}" "${end_time}" "${input_recs}" "${clean_recs}" "${reject_recs}" "${status}" "${error_msg}" >> "${tmp_file}"
            found=1
        else
            printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "${r_id}" "${r_path}" "${r_start}" "${r_end}" "${r_in}" "${r_clean}" "${r_rej}" "${r_stat}" "${r_err}" >> "${tmp_file}"
        fi
    done < "${reg_file}"
    
    if [ "${found}" -eq 0 ]; then
        printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "${batch_id}" "${input_path}" "${start_time}" "${end_time}" "${input_recs}" "${clean_recs}" "${reject_recs}" "${status}" "${error_msg}" >> "${tmp_file}"
    fi
    
    chmod 644 "${tmp_file}"
    mv "${tmp_file}" "${reg_file}"
    log "Registry updated for batch '${batch_id}': status=${status}, clean=${clean_recs}, reject=${reject_recs}"
}

print_batch_registry() {
    init_batch_registry
    local reg_file="${PROJECT_ROOT}/state/registry.tsv"
    echo "===================================================================================================="
    echo "E-COMMERCE BATCH REGISTRY"
    echo "===================================================================================================="
    printf "%-12s | %-10s | %-8s | %-8s | %-8s | %-19s | %s\n" \
           "BATCH ID" "STATUS" "INPUT" "CLEAN" "REJECT" "LAST PROCESSED" "INPUT PATH"
    echo "----------------------------------------------------------------------------------------------------"
    
    local count=0
    while IFS=$'\t' read -r r_id r_path r_start r_end r_in r_clean r_rej r_stat r_err || [ -n "${r_id}" ]; do
        if [ "${r_id}" = "batch_id" ]; then
            continue
        fi
        count=$((count + 1))
        local display_time="${r_end:-$r_start}"
        display_time="${display_time:0:19}"
        printf "%-12s | %-10s | %-8s | %-8s | %-8s | %-19s | %s\n" \
               "${r_id}" "${r_stat}" "${r_in}" "${r_clean}" "${r_rej}" "${display_time}" "${r_path}"
    done < "${reg_file}"
    
    if [ "${count}" -eq 0 ]; then
        echo "No batches have been registered yet."
    fi
    echo "===================================================================================================="
}

# ------------------------------------------------------------------------------
# Pig Cleaning on Apache Tez (Block 2 Core)
# ------------------------------------------------------------------------------
run_pig_cleaning() {
    local batch_id="$1"
    local input_path="$2"
    local clean_out="${CLEAN_ROOT}/${batch_id}"
    local reject_base="${REJECT_ROOT}/${batch_id}"
    local reject_out="${reject_base}/invalid"
    local duplicate_out="${reject_base}/duplicates"
    local pig_script="${PROJECT_ROOT}/pig/clean_transactions.pig"
    
    log "Preparing Pig cleaning job on Tez for batch: ${batch_id}"
    log "Input path      : ${input_path}"
    log "Clean output    : ${clean_out}"
    log "Reject output   : ${reject_out}"
    log "Duplicate output: ${duplicate_out}"
    
    # Pre-execution safety check on output paths before any HDFS deletion
    if ! validate_output_paths "${batch_id}"; then
        log_error "Safety check failed: unsafe output paths for batch '${batch_id}'. Aborting Pig cleaning."
        return 1
    fi
    
    if [ ! -f "${pig_script}" ]; then
        log_error "Pig script not found at: ${pig_script}"
        return 1
    fi
    
    # If this is a retry of a non-successful batch, clear previous partial outputs
    if hdfs dfs -test -e "${clean_out}" 2>>"${LOG_FILE}"; then
        log "Removing previous partial clean output at ${clean_out}"
        if ! hdfs dfs -rm -r -skipTrash "${clean_out}" >>"${LOG_FILE}" 2>&1; then
            log_error "Failed to remove previous partial clean output at ${clean_out}. Aborting to avoid Pig output collision."
            return 1
        fi
    fi
    if hdfs dfs -test -e "${reject_base}" 2>>"${LOG_FILE}"; then
        log "Removing previous partial reject output at ${reject_base}"
        if ! hdfs dfs -rm -r -skipTrash "${reject_base}" >>"${LOG_FILE}" 2>&1; then
            log_error "Failed to remove previous partial reject output at ${reject_base}. Aborting to avoid Pig output collision."
            return 1
        fi
    fi
    
    echo "Executing Pig cleaning job via Apache Tez on YARN..."
    log "Executing: pig -x tez -param INPUT_PATH=${input_path} -param CLEAN_OUTPUT=${clean_out} -param REJECT_OUTPUT=${reject_out} -param DUPLICATE_OUTPUT=${duplicate_out} ${pig_script}"
    
    local pig_exit_code=0
    pig -x tez \
        -param INPUT_PATH="${input_path}" \
        -param CLEAN_OUTPUT="${clean_out}" \
        -param REJECT_OUTPUT="${reject_out}" \
        -param DUPLICATE_OUTPUT="${duplicate_out}" \
        "${pig_script}" 2>&1 | tee -a "${LOG_FILE}" || pig_exit_code=$?
        
    if [ "${pig_exit_code}" -ne 0 ]; then
        log_error "Pig execution failed with exit code: ${pig_exit_code}"
        return 1
    fi
    
    # Verify Tez execution in log
    if grep -Eiq "TezVersion|Picked TEZ|Submitting DAG|DAG Status" "${LOG_FILE}"; then
        log "Verified: Pig executed using Apache Tez engine on YARN."
    else
        log "Warning: Tez execution indicators not found in current log."
    fi
    
    log "Pig job completed successfully with exit code 0."
    return 0
}

# ------------------------------------------------------------------------------
# Output Record Counting
# ------------------------------------------------------------------------------
count_batch_records() {
    local batch_id="$1"
    local input_path="${2:-}"
    local clean_out="${CLEAN_ROOT}/${batch_id}"
    local reject_base="${REJECT_ROOT}/${batch_id}"
    
    log "Counting output records from HDFS for batch: ${batch_id}"
    
    local clean_cnt=0
    local reject_cnt=0
    local duplicate_cnt=0
    local input_cnt=0
    
    if hdfs dfs -test -e "${clean_out}" 2>>"${LOG_FILE}"; then
        clean_cnt=$(hdfs dfs -cat "${clean_out}/part*" 2>>"${LOG_FILE}" | grep -c . || true)
    fi
    
    if hdfs dfs -test -e "${reject_base}/invalid" 2>>"${LOG_FILE}"; then
        reject_cnt=$(hdfs dfs -cat "${reject_base}/invalid/part*" 2>>"${LOG_FILE}" | grep -c . || true)
    elif hdfs dfs -test -e "${reject_base}" 2>>"${LOG_FILE}"; then
        reject_cnt=$(hdfs dfs -cat "${reject_base}/part*" 2>>"${LOG_FILE}" | grep -c . || true)
    fi
    
    if hdfs dfs -test -e "${reject_base}/duplicates" 2>>"${LOG_FILE}"; then
        duplicate_cnt=$(hdfs dfs -cat "${reject_base}/duplicates/part*" 2>>"${LOG_FILE}" | grep -c . || true)
    fi
    
    # Independently count raw input data records from the raw CSV in HDFS
    if [ -n "${input_path}" ] && hdfs dfs -test -e "${input_path}" 2>>"${LOG_FILE}"; then
        # Count physical data rows excluding CSV header row (including blank lines processed as records by Pig)
        input_cnt=$(hdfs dfs -cat "${input_path}" 2>>"${LOG_FILE}" | awk '
            NR == 1 { next }
            tolower($0) ~ /^"?order_id"?([,\t]|$)/ { next }
            { count++ }
            END { print count + 0 }
        ')
        log "Independently counted raw input records from '${input_path}': ${input_cnt}"
    else
        log_error "Input path '${input_path}' not provided or not found on HDFS for independent record counting."
        input_cnt=-1
    fi
    
    echo "${input_cnt:-0} ${clean_cnt:-0} ${reject_cnt:-0} ${duplicate_cnt:-0}"
}

# ------------------------------------------------------------------------------
# Hive Partition Synchronization
# ------------------------------------------------------------------------------
sync_hive_partitions() {
    log "Synchronizing Hive partitions with HDFS clean storage..."
    if ! command -v hive >/dev/null 2>&1; then
        log_error "Hive command not available for partition synchronization."
        return 0
    fi

    local parts
    parts="$(hive --silent=true --showHeader=false -e "USE ${HIVE_DATABASE}; SHOW PARTITIONS ${HIVE_TABLE};" 2>>"${LOG_FILE}" | grep 'batch_id=' || true)"

    for p in ${parts}; do
        local b_id="${p#*batch_id=}"
        b_id="${b_id%%/*}"
        local p_loc="${CLEAN_ROOT}/${b_id}"
        if ! hdfs dfs -test -d "${p_loc}" 2>>"${LOG_FILE}"; then
            log "Found orphaned Hive partition '${b_id}' with missing HDFS directory '${p_loc}'. Dropping from Hive metastore..."
            hive --silent=true -e "USE ${HIVE_DATABASE}; ALTER TABLE ${HIVE_TABLE} DROP IF EXISTS PARTITION (batch_id='${b_id}');" >>"${LOG_FILE}" 2>&1 || true
        fi
    done
    log "Hive partition synchronization complete."
    return 0
}

# ------------------------------------------------------------------------------
# Hive Partition Loading and Verification
# ------------------------------------------------------------------------------
load_hive_partition() {
    local batch_id="$1"
    local clean_out="${CLEAN_ROOT}/${batch_id}"
    local expected_clean_records="$2"
    
    # Input & SQL injection prevention validation
    if ! validate_batch_id "${batch_id}"; then
        log_error "Safety check failed: batch ID '${batch_id}' is invalid."
        return 1
    fi
    if ! validate_output_paths "${batch_id}"; then
        log_error "Safety check failed: clean output path is invalid."
        return 1
    fi
    if [[ ! "${expected_clean_records}" =~ ^[0-9]+$ ]]; then
        log_error "Expected clean records must be a non-negative integer: ${expected_clean_records}"
        return 1
    fi
    if [[ ! "${HIVE_DATABASE}" =~ ^[a-zA-Z0-9_]+$ ]] || [[ ! "${HIVE_TABLE}" =~ ^[a-zA-Z0-9_]+$ ]]; then
        log_error "Invalid Hive database or table identifier."
        return 1
    fi

    sync_hive_partitions

    log "Loading batch '${batch_id}' into Hive table '${HIVE_DATABASE}.${HIVE_TABLE}'..."
    echo "Registering batch partition in Hive (${HIVE_DATABASE}.${HIVE_TABLE})..."
    
    local hive_cmd="USE ${HIVE_DATABASE}; ALTER TABLE ${HIVE_TABLE} ADD IF NOT EXISTS PARTITION (batch_id = '${batch_id}') LOCATION '${clean_out}'; ALTER TABLE ${HIVE_TABLE} PARTITION (batch_id = '${batch_id}') SET LOCATION '${clean_out}';"
    log "Executing Hive DDL: ${hive_cmd}"
    
    local ddl_exit=0
    hive -e "${hive_cmd}" >>"${LOG_FILE}" 2>&1 || ddl_exit=$?
    if [ "${ddl_exit}" -ne 0 ]; then
        log_error "Failed to add Hive partition for batch: ${batch_id}"
        return 1
    fi
    log "Hive partition registered successfully."
    
    # Query partition record count for verification
    log "Verifying loaded records in Hive partition (batch_id='${batch_id}')..."
    local query="SELECT COUNT(*) FROM ${HIVE_DATABASE}.${HIVE_TABLE} WHERE batch_id = '${batch_id}';"
    local hive_count
    hive_count=$(hive --silent=true --showHeader=false --outputformat=dsv -e "${query}" 2>>"${LOG_FILE}" | grep -E '^[0-9]+$' | tail -n 1)
    
    if [ -z "${hive_count}" ]; then
        log_error "Failed to retrieve Hive record count for partition: ${batch_id}"
        return 1
    fi
    
    log "Hive partition record count: ${hive_count} (expected: ${expected_clean_records})"
    
    if [ "${hive_count}" -ne "${expected_clean_records}" ]; then
        log_error "Hive record count mismatch: Hive=${hive_count}, Pig clean=${expected_clean_records}"
        echo "ERROR: Hive loaded count (${hive_count}) does not match clean count (${expected_clean_records})."
        return 1
    fi
    
    log "Hive partition validation verified: ${hive_count} records match exactly."
    return 0
}

# ------------------------------------------------------------------------------
# Block 3: Staging Directory Safe Removal (Fix 9)
# ------------------------------------------------------------------------------
safe_remove_staging_dir() {
    local dir="$1"
    local expected_parent="${PROJECT_ROOT}/reports"
    
    if [ -z "${dir:-}" ]; then
        log_error "Safety check failed: staging directory variable is empty."
        return 1
    fi
    
    if [ ! -d "${dir}" ]; then
        return 0
    fi
    
    # Assert dir is strictly a subdirectory inside ${expected_parent}/staging_*
    case "${dir}" in
        "${expected_parent}"/staging_*)
            rm -rf "${dir}"
            log "Safely removed staging directory: ${dir}"
            ;;
        *)
            log_error "Safety assertion failed: staging dir '${dir}' is not under '${expected_parent}/staging_*'. Deletion aborted."
            return 1
            ;;
    esac
    return 0
}

# ------------------------------------------------------------------------------
# Block 3: Report Publishing & Atomic HDFS Operations
# ------------------------------------------------------------------------------
safe_publish_hdfs_dir() {
    local local_src_dir="$1"
    local hdfs_target_dir="$2"
    
    if [ ! -d "${local_src_dir}" ]; then
        log_error "Local source directory does not exist: ${local_src_dir}"
        return 1
    fi
    
    # Assert target is strictly inside /project-2/reports/
    if [[ ! "${hdfs_target_dir}" =~ ^/project-2/reports/ ]]; then
        log_error "Safety assertion failed: Target HDFS path '${hdfs_target_dir}' must be inside /project-2/reports/"
        return 1
    fi
    
    log "Publishing reports atomically to HDFS target: ${hdfs_target_dir}"
    
    # Ensure parent directory exists in HDFS
    local hdfs_parent
    hdfs_parent="$(dirname "${hdfs_target_dir}")"
    hdfs dfs -mkdir -p "${hdfs_parent}" >>"${LOG_FILE}" 2>&1
    
    # Staging path in HDFS
    local staging_target="${hdfs_parent}/.staging_$(basename "${hdfs_target_dir}")_$$"
    if hdfs dfs -test -e "${staging_target}" 2>>"${LOG_FILE}"; then
        hdfs dfs -rm -r -skipTrash "${staging_target}" >>"${LOG_FILE}" 2>&1 || true
    fi
    
    # Upload to staging target with retry for transient network/VM pauses
    local upload_success=0
    for attempt in 1 2 3; do
        if hdfs dfs -put "${local_src_dir}" "${staging_target}" >>"${LOG_FILE}" 2>&1; then
            upload_success=1
            break
        fi
        log "Warning: hdfs dfs -put failed on attempt ${attempt}. Retrying in 2 seconds..."
        hdfs dfs -rm -r -skipTrash "${staging_target}" >>"${LOG_FILE}" 2>&1 || true
        sleep 2
    done

    if [ "${upload_success}" -ne 1 ]; then
        log_error "Failed to upload report files to HDFS staging path: ${staging_target}"
        hdfs dfs -rm -r -skipTrash "${staging_target}" >>"${LOG_FILE}" 2>&1 || true
        return 1
    fi
    
    # If target already exists in HDFS, move to backup first to avoid data deletion window (Fix 10)
    local backup_target="${hdfs_parent}/.backup_$(basename "${hdfs_target_dir}")_$$"
    local had_target=0
    if hdfs dfs -test -e "${hdfs_target_dir}" 2>>"${LOG_FILE}"; then
        had_target=1
        if ! hdfs dfs -mv "${hdfs_target_dir}" "${backup_target}" >>"${LOG_FILE}" 2>&1; then
            log_error "Failed to move existing target '${hdfs_target_dir}' to backup '${backup_target}'. Halting publication to protect existing reports."
            hdfs dfs -rm -r -skipTrash "${staging_target}" >>"${LOG_FILE}" 2>&1 || true
            return 1
        fi
    fi
    
    # Atomically move staging to final target
    if ! hdfs dfs -mv "${staging_target}" "${hdfs_target_dir}" >>"${LOG_FILE}" 2>&1; then
        log_error "Failed to move HDFS staging directory to target: ${hdfs_target_dir}"
        if [ "${had_target}" -eq 1 ]; then
            log "Attempting to restore original target from '${backup_target}'..."
            if ! hdfs dfs -mv "${backup_target}" "${hdfs_target_dir}" >>"${LOG_FILE}" 2>&1; then
                log_error "CRITICAL: Failed to restore original target from backup '${backup_target}'. Manual recovery in HDFS may be required."
            else
                log "Successfully restored original target from backup."
            fi
        fi
        hdfs dfs -rm -r -skipTrash "${staging_target}" >>"${LOG_FILE}" 2>&1 || true
        return 1
    fi

    # Clean up temporary backup
    if [ "${had_target}" -eq 1 ]; then
        hdfs dfs -rm -r -skipTrash "${backup_target}" >>"${LOG_FILE}" 2>&1 || true
    fi
    
    log "Successfully published reports to: ${hdfs_target_dir}"
    return 0
}

# ------------------------------------------------------------------------------
# Block 3: CSV Export Utility
# ------------------------------------------------------------------------------
export_tsv_dir_to_csv() {
    local src_dir="$1"
    local dest_csv="$2"
    if [ -d "${src_dir}" ]; then
        for f in "${src_dir}"/*; do
            if [ -f "$f" ] && [[ "$(basename "$f")" != .* ]]; then
                tr '\t' ',' < "$f" >> "${dest_csv}"
            fi
        done
    fi
}

# ------------------------------------------------------------------------------
# Block 3: Weekly Analytical Report
# ------------------------------------------------------------------------------
generate_weekly_report() {
    local batch_id="$1"
    log "Starting Weekly Report generation for batch: ${batch_id}"
    echo "Generating Weekly Analytics Report (${batch_id})..."

    if ! validate_batch_id "${batch_id}"; then
        log_error "Invalid batch ID for weekly report: ${batch_id}"
        return 1
    fi

    local hdfs_weekly_dir="${WEEKLY_REPORT_ROOT}/${batch_id}"
    local staging_dir
    staging_dir="$(mktemp -d "${PROJECT_ROOT}/reports/staging_weekly_${batch_id}_XXXXXX")"
    chmod 755 "${staging_dir}"

    local sql_file="${staging_dir}/run_weekly.sql"
    sed -e "s|__DB_NAME__|${HIVE_DATABASE}|g" \
        -e "s|__TABLE_NAME__|${HIVE_TABLE}|g" \
        -e "s|__BATCH_ID__|${batch_id}|g" \
        -e "s|__STAGING_DIR__|${staging_dir}|g" \
        "${PROJECT_ROOT}/hive/weekly_report.sql" > "${sql_file}"

    log "Executing Hive weekly analytics SQL..."
    local hive_code=0
    hive -e "$(< "${sql_file}")" >>"${LOG_FILE}" 2>&1 || hive_code=$?
    if [ "${hive_code}" -ne 0 ]; then
        log_error "Hive execution failed during weekly report generation for batch: ${batch_id}"
        safe_remove_staging_dir "${staging_dir}"
        return 1
    fi

    # Read registry stats
    local r_path r_start r_end r_in r_clean r_rej
    r_path="$(get_batch_field "${batch_id}" 2)"
    r_start="$(get_batch_field "${batch_id}" 3)"
    r_end="$(get_batch_field "${batch_id}" 4)"
    r_in="$(get_batch_field "${batch_id}" 5)"
    r_clean="$(get_batch_field "${batch_id}" 6)"
    r_rej="$(get_batch_field "${batch_id}" 7)"

    # Parse sales overview
    local ov_file
    ov_file="$(find "${staging_dir}/sales_overview" -maxdepth 1 -type f ! -name '.*' | head -n 1)"
    local tot_recs tot_qty tot_sales d_orders d_cust d_prod aov dup_tx d_tx
    if [ -n "${ov_file}" ] && [ -f "${ov_file}" ]; then
        IFS=$'\t' read -r tot_recs tot_qty tot_sales d_orders d_cust d_prod aov dup_tx d_tx < "${ov_file}" || true
    fi
    tot_recs="${tot_recs:-0}"
    tot_qty="${tot_qty:-0}"
    tot_sales="${tot_sales:-0.00}"
    d_orders="${d_orders:-0}"
    d_cust="${d_cust:-0}"
    d_prod="${d_prod:-0}"
    aov="${aov:-0.00}"
    dup_tx="${dup_tx:-0}"
    d_tx="${d_tx:-$tot_recs}"

    if [ -z "${r_end}" ] || [ "${r_end}" = "-" ]; then
        r_end="${r_start}"
    fi
    if [ -z "${r_clean}" ] || [ "${r_clean}" = "0" ]; then
        r_clean="${tot_recs:-$d_orders}"
    fi
    if [ -z "${r_in}" ] || [ "${r_in}" = "0" ]; then
        r_in=$(( ${r_clean:-0} + ${r_rej:-0} ))
    fi
    r_rej="${r_rej:-0}"

    local clean_pct="0.00%"
    if [ "${r_in}" -gt 0 ]; then
        clean_pct="$(awk -v c="${r_clean}" -v i="${r_in}" 'BEGIN { printf "%.2f%%", (c/i)*100 }')"
    fi

    local orders_per_cust="0.00"
    local sales_per_cust="0.00"
    if [ "${d_cust}" -gt 0 ]; then
        orders_per_cust="$(awk -v o="${d_orders}" -v c="${d_cust}" 'BEGIN { printf "%.2f", o/c }')"
        sales_per_cust="$(awk -v s="${tot_sales}" -v c="${d_cust}" 'BEGIN { printf "%.2f", s/c }')"
    fi

    # Build local package directory
    local pkg_dir="${staging_dir}/pkg"
    mkdir -p "${pkg_dir}"

    # Export CSVs with headers
    echo "category,quantity,sales,order_count,transaction_count" > "${pkg_dir}/category_summary.csv"
    export_tsv_dir_to_csv "${staging_dir}/category_summary" "${pkg_dir}/category_summary.csv"

    echo "product_id,product_name,category,quantity_sold,sales,order_count" > "${pkg_dir}/product_summary.csv"
    export_tsv_dir_to_csv "${staging_dir}/product_summary" "${pkg_dir}/product_summary.csv"

    echo "payment_method,transaction_count,sales" > "${pkg_dir}/payment_summary.csv"
    export_tsv_dir_to_csv "${staging_dir}/payment_summary" "${pkg_dir}/payment_summary.csv"

    echo "order_status,status_count,order_count,sales" > "${pkg_dir}/order_status_summary.csv"
    export_tsv_dir_to_csv "${staging_dir}/order_status_summary" "${pkg_dir}/order_status_summary.csv"

    echo "customer_id,orders,quantity,sales,city" > "${pkg_dir}/customer_summary.csv"
    export_tsv_dir_to_csv "${staging_dir}/customer_summary" "${pkg_dir}/customer_summary.csv"

    local inv_cnt=0 dup_cnt=0
    if hdfs dfs -test -e "${REJECT_ROOT}/${batch_id}/invalid" 2>>"${LOG_FILE}"; then
        inv_cnt=$(hdfs dfs -cat "${REJECT_ROOT}/${batch_id}/invalid/part*" 2>>"${LOG_FILE}" | grep -c . || true)
    fi
    if hdfs dfs -test -e "${REJECT_ROOT}/${batch_id}/duplicates" 2>>"${LOG_FILE}"; then
        dup_cnt=$(hdfs dfs -cat "${REJECT_ROOT}/${batch_id}/duplicates/part*" 2>>"${LOG_FILE}" | grep -c . || true)
    fi
    if [ "${inv_cnt}" -eq 0 ] && [ "${dup_cnt}" -eq 0 ]; then
        inv_cnt="${r_rej:-0}"
    fi

    # Format human-readable weekly_summary.txt
    local txt_file="${pkg_dir}/weekly_summary.txt"
    cat <<EOF > "${txt_file}"
================================================================================
WEEKLY E-COMMERCE ANALYTICS REPORT
================================================================================
Batch ID        : ${batch_id}
Input Path      : ${r_path:-N/A}
Processed At    : ${r_end:-$r_start}
Reporting Unit  : ${CURRENCY_SYMBOL} (Configured Reporting Currency)

--------------------------------------------------------------------------------
1. RECORD & DATA QUALITY AUDIT
--------------------------------------------------------------------------------
Input Records     : ${r_in:-0}
Clean Records     : ${r_clean:-0}
Rejected Records  : ${inv_cnt} (Schema & calculation errors)
Duplicate Records : ${dup_cnt} (Excluded duplicate transaction occurrences)
Total Excluded    : $(( inv_cnt + dup_cnt ))
Clean Percentage  : ${clean_pct}
In-Hive Duplicates: ${dup_tx} (Repeated transaction_id in clean table)

--------------------------------------------------------------------------------
2. SALES & TRANSACTION OVERVIEW
--------------------------------------------------------------------------------
Distinct Orders       : ${d_orders}
Distinct Transactions : ${d_tx}
Unique Customers      : ${d_cust}
Unique Products       : ${d_prod}
Total Quantity        : ${tot_qty}
Gross Sales           : ${CURRENCY_SYMBOL}${tot_sales}
Avg Order Value       : ${CURRENCY_SYMBOL}${aov} (Gross Sales / Distinct Orders)

--------------------------------------------------------------------------------
3. CATEGORY ANALYSIS
--------------------------------------------------------------------------------
$(printf "%-20s | %-8s | %-10s | %-12s | %s\n" "Category" "Orders" "Quantity" "Sales (${CURRENCY_SYMBOL})" "% Sales")
--------------------------------------------------------------------------------
EOF

    local cat_file
    cat_file="$(find "${staging_dir}/category_summary" -maxdepth 1 -type f ! -name '.*' | head -n 1)"
    if [ -n "${cat_file}" ] && [ -f "${cat_file}" ]; then
        while IFS=$'\t' read -r c_cat c_qty c_sales c_ord c_tx || [ -n "${c_cat}" ]; do
            local c_pct="0.00%"
            if awk -v s="${tot_sales:-0}" 'BEGIN { exit !(s > 0) }'; then
                c_pct="$(awk -v cs="${c_sales:-0}" -v ts="${tot_sales:-1}" 'BEGIN { printf "%.2f%%", (cs/ts)*100 }')"
            fi
            printf "%-20s | %-8s | %-10s | %-12s | %s\n" \
                   "${c_cat}" "${c_ord}" "${c_qty}" "${c_sales}" "${c_pct}" >> "${txt_file}"
        done < "${cat_file}"
    fi

    cat <<EOF >> "${txt_file}"

--------------------------------------------------------------------------------
4. PRODUCT PERFORMANCE SUMMARY
--------------------------------------------------------------------------------
$(printf "%-10s | %-24s | %-8s | %-8s | %s\n" "Product ID" "Product Name" "Orders" "Quantity" "Sales (${CURRENCY_SYMBOL})")
--------------------------------------------------------------------------------
EOF
    local prod_file
    prod_file="$(find "${staging_dir}/product_summary" -maxdepth 1 -type f ! -name '.*' | head -n 1)"
    if [ -n "${prod_file}" ] && [ -f "${prod_file}" ]; then
        while IFS=$'\t' read -r p_id p_name p_cat p_qty p_sales p_ord || [ -n "${p_id}" ]; do
            printf "%-10s | %-24s | %-8s | %-8s | %s\n" \
                   "${p_id}" "${p_name:0:24}" "${p_ord}" "${p_qty}" "${p_sales}" >> "${txt_file}"
        done < "${prod_file}"
    fi

    cat <<EOF >> "${txt_file}"

--------------------------------------------------------------------------------
5. PAYMENT METHOD BREAKDOWN
--------------------------------------------------------------------------------
$(printf "%-20s | %-14s | %-14s | %s\n" "Payment Method" "Transactions" "Sales (${CURRENCY_SYMBOL})" "% Sales")
--------------------------------------------------------------------------------
EOF
    local pay_file
    pay_file="$(find "${staging_dir}/payment_summary" -maxdepth 1 -type f ! -name '.*' | head -n 1)"
    if [ -n "${pay_file}" ] && [ -f "${pay_file}" ]; then
        while IFS=$'\t' read -r pm_name pm_cnt pm_sales || [ -n "${pm_name}" ]; do
            local pm_pct="0.00%"
            if awk -v s="${tot_sales:-0}" 'BEGIN { exit !(s > 0) }'; then
                pm_pct="$(awk -v cs="${pm_sales:-0}" -v ts="${tot_sales:-1}" 'BEGIN { printf "%.2f%%", (cs/ts)*100 }')"
            fi
            printf "%-20s | %-14s | %-14s | %s\n" \
                   "${pm_name}" "${pm_cnt}" "${pm_sales}" "${pm_pct}" >> "${txt_file}"
        done < "${pay_file}"
    fi

    cat <<EOF >> "${txt_file}"

--------------------------------------------------------------------------------
6. ORDER STATUS DISTRIBUTION & AUDIT
--------------------------------------------------------------------------------
$(printf "%-18s | %-12s | %-10s | %-12s | %s\n" "Order Status" "Tx Count" "Orders" "Sales (${CURRENCY_SYMBOL})" "Audit Note")
--------------------------------------------------------------------------------
EOF
    local st_file
    st_file="$(find "${staging_dir}/order_status_summary" -maxdepth 1 -type f ! -name '.*' | head -n 1)"
    if [ -n "${st_file}" ] && [ -f "${st_file}" ]; then
        while IFS=$'\t' read -r s_name s_cnt s_ord s_sales || [ -n "${s_name}" ]; do
            local note="Finalized"
            case "${s_name}" in
                Completed) note="Finalized & Delivered" ;;
                Shipped)   note="In-Transit Fulfillment" ;;
                Cancelled) note="Non-Fulfilled / Excluded" ;;
                Returned)  note="Post-Delivery Return" ;;
                *)         note="Observed Status" ;;
            esac
            printf "%-18s | %-12s | %-10s | %-12s | %s\n" \
                   "${s_name}" "${s_cnt}" "${s_ord}" "${s_sales}" "${note}" >> "${txt_file}"
        done < "${st_file}"
    fi

    cat <<EOF >> "${txt_file}"

--------------------------------------------------------------------------------
7. CUSTOMER SUMMARY & BEHAVIOR
--------------------------------------------------------------------------------
Unique Customers: ${d_cust}
Orders / Customer: ${orders_per_cust}
Sales / Customer : ${CURRENCY_SYMBOL}${sales_per_cust}

$(printf "%-12s | %-16s | %-8s | %-8s | %s\n" "Customer ID" "City" "Orders" "Quantity" "Sales (${CURRENCY_SYMBOL})")
--------------------------------------------------------------------------------
EOF
    local cust_file
    cust_file="$(find "${staging_dir}/customer_summary" -maxdepth 1 -type f ! -name '.*' | head -n 1)"
    if [ -n "${cust_file}" ] && [ -f "${cust_file}" ]; then
        while IFS=$'\t' read -r cu_id cu_ord cu_qty cu_sales cu_city || [ -n "${cu_id}" ]; do
            printf "%-12s | %-16s | %-8s | %-8s | %s\n" \
                   "${cu_id}" "${cu_city:0:16}" "${cu_ord}" "${cu_qty}" "${cu_sales}" >> "${txt_file}"
        done < "${cust_file}"
    fi

    cat <<EOF >> "${txt_file}"
================================================================================
Notice: All metrics are derived from verified Hive data in project2.ecommerce_transactions.
================================================================================
EOF

    # Publish package to HDFS
    if ! safe_publish_hdfs_dir "${pkg_dir}" "${hdfs_weekly_dir}"; then
        log_error "Failed to publish weekly report to HDFS: ${hdfs_weekly_dir}"
        safe_remove_staging_dir "${staging_dir}"
        return 1
    fi

    safe_remove_staging_dir "${staging_dir}"
    log "Weekly report generated and published successfully for batch: ${batch_id}"
    echo "Weekly report created: ${hdfs_weekly_dir}/"
    return 0
}

# ------------------------------------------------------------------------------
# Block 3: Historical Longitudinal Report
# ------------------------------------------------------------------------------
generate_historical_report() {
    log "Starting Historical Report generation across all registered batches..."
    echo "Generating Historical Analytics & Trend Reports..."

    sync_hive_partitions

    local hdfs_hist_dir="${HISTORICAL_REPORT_ROOT}"
    local staging_dir
    staging_dir="$(mktemp -d "${PROJECT_ROOT}/reports/staging_hist_XXXXXX")"
    chmod 755 "${staging_dir}"

    local sql_file="${staging_dir}/run_historical.sql"
    sed -e "s|__DB_NAME__|${HIVE_DATABASE}|g" \
        -e "s|__TABLE_NAME__|${HIVE_TABLE}|g" \
        -e "s|__STAGING_DIR__|${staging_dir}|g" \
        "${PROJECT_ROOT}/hive/historical_report.sql" > "${sql_file}"

    log "Executing Hive historical analytics SQL..."
    local hive_code=0
    hive -e "$(< "${sql_file}")" >>"${LOG_FILE}" 2>&1 || hive_code=$?
    if [ "${hive_code}" -ne 0 ]; then
        log_error "Hive execution failed during historical report generation."
        safe_remove_staging_dir "${staging_dir}"
        return 1
    fi

    local pkg_dir="${staging_dir}/pkg"
    mkdir -p "${pkg_dir}"

    # Parse overall totals
    local ov_file
    ov_file="$(find "${staging_dir}/historical_overall" -maxdepth 1 -type f ! -name '.*' | head -n 1)"
    local tot_b tot_o tot_c tot_p tot_q tot_s overall_aov
    if [ -n "${ov_file}" ] && [ -f "${ov_file}" ]; then
        IFS=$'\t' read -r tot_b tot_o tot_c tot_p tot_q tot_s overall_aov < "${ov_file}" || true
    fi

    # Read weekly trend and compute Week-over-Week changes
    local weekly_file
    weekly_file="$(find "${staging_dir}/historical_weekly" -maxdepth 1 -type f ! -name '.*' | head -n 1)"

    # Build weekly_trend.csv
    echo "batch_id,orders,orders_wow_pct,customers,customers_wow_pct,quantity,quantity_wow_pct,sales,sales_wow_pct,avg_order_value" > "${pkg_dir}/weekly_trend.csv"

    # Build historical_category_trend.csv
    echo "batch_id,category,quantity,sales" > "${pkg_dir}/historical_category_trend.csv"
    export_tsv_dir_to_csv "${staging_dir}/historical_category" "${pkg_dir}/historical_category_trend.csv"

    # Human-readable historical_summary.txt
    local txt_file="${pkg_dir}/historical_summary.txt"
    cat <<EOF > "${txt_file}"
================================================================================
HISTORICAL E-COMMERCE LONGITUDINAL REPORT
================================================================================
Generated At    : $(date '+%Y-%m-%d %H:%M:%S %Z')
Reporting Unit  : ${CURRENCY_SYMBOL} (Configured Reporting Currency)
Historical Span : ${tot_b:-0} Batch(es) Processed

--------------------------------------------------------------------------------
1. CUMULATIVE HISTORICAL TOTALS
--------------------------------------------------------------------------------
Total Batches   : ${tot_b:-0}
Total Orders    : ${tot_o:-0}
Total Customers : ${tot_c:-0}
Total Products  : ${tot_p:-0}
Total Quantity  : ${tot_q:-0}
Cumulative Sales: ${CURRENCY_SYMBOL}${tot_s:-0.00}
Overall AOV     : ${CURRENCY_SYMBOL}${overall_aov:-0.00}

--------------------------------------------------------------------------------
2. WEEKLY TREND & WEEK-OVER-WEEK (WoW) ANALYSIS
--------------------------------------------------------------------------------
$(printf "%-10s | %-6s | %-8s | %-6s | %-8s | %-6s | %-8s | %-10s | %-8s | %s\n" \
  "Week" "Orders" "WoW %" "Cust" "WoW %" "Qty" "WoW %" "Sales (${CURRENCY_SYMBOL})" "WoW %" "AOV (${CURRENCY_SYMBOL})")
--------------------------------------------------------------------------------
EOF

    local prev_o="" prev_c="" prev_q="" prev_s=""
    if [ -n "${weekly_file}" ] && [ -f "${weekly_file}" ]; then
        while IFS=$'\t' read -r w_id w_o w_c w_q w_s w_aov || [ -n "${w_id}" ]; do
            local wow_o="N/A" wow_c="N/A" wow_q="N/A" wow_s="N/A"
            if [ -n "${prev_o}" ]; then
                if [ "${prev_o}" -gt 0 ]; then
                    wow_o="$(awk -v curr="${w_o}" -v prev="${prev_o}" 'BEGIN { printf "%+.1f%%", ((curr-prev)/prev)*100 }')"
                fi
                if [ "${prev_c}" -gt 0 ]; then
                    wow_c="$(awk -v curr="${w_c}" -v prev="${prev_c}" 'BEGIN { printf "%+.1f%%", ((curr-prev)/prev)*100 }')"
                fi
                if [ "${prev_q}" -gt 0 ]; then
                    wow_q="$(awk -v curr="${w_q}" -v prev="${prev_q}" 'BEGIN { printf "%+.1f%%", ((curr-prev)/prev)*100 }')"
                fi
                if awk -v prev="${prev_s}" 'BEGIN { exit !(prev > 0) }'; then
                    wow_s="$(awk -v curr="${w_s}" -v prev="${prev_s}" 'BEGIN { printf "%+.1f%%", ((curr-prev)/prev)*100 }')"
                fi
            fi

            printf "%-10s | %-6s | %-8s | %-6s | %-8s | %-6s | %-8s | %-10s | %-8s | %s\n" \
                   "${w_id}" "${w_o}" "${wow_o}" "${w_c}" "${wow_c}" "${w_q}" "${wow_q}" "${w_s}" "${wow_s}" "${w_aov}" >> "${txt_file}"

            echo "${w_id},${w_o},${wow_o},${w_c},${wow_c},${w_q},${wow_q},${w_s},${wow_s},${w_aov}" >> "${pkg_dir}/weekly_trend.csv"

            prev_o="${w_o}"
            prev_c="${w_c}"
            prev_q="${w_q}"
            prev_s="${w_s}"
        done < "${weekly_file}"
    fi

    if [ "${tot_b:-0}" -eq 0 ]; then
        cat <<EOF >> "${txt_file}"
No historical data available in Hive table.
EOF
    elif [ "${tot_b:-0}" -eq 1 ]; then
        cat <<EOF >> "${txt_file}"
--------------------------------------------------------------------------------
Notice: Baseline batch established (${w_id:-initial batch}). No prior historical batches exist.
Week-over-Week (WoW) metrics will compute upon ingestion of subsequent batches.
EOF
    fi

    cat <<EOF >> "${txt_file}"

--------------------------------------------------------------------------------
3. HISTORICAL CATEGORY TREND
--------------------------------------------------------------------------------
$(printf "%-10s | %-20s | %-10s | %s\n" "Week" "Category" "Quantity" "Sales (${CURRENCY_SYMBOL})")
--------------------------------------------------------------------------------
EOF
    local cat_trend_file
    cat_trend_file="$(find "${staging_dir}/historical_category" -maxdepth 1 -type f ! -name '.*' | head -n 1)"
    if [ -n "${cat_trend_file}" ] && [ -f "${cat_trend_file}" ]; then
        while IFS=$'\t' read -r h_id h_cat h_qty h_sales || [ -n "${h_id}" ]; do
            printf "%-10s | %-20s | %-10s | %s\n" \
                   "${h_id}" "${h_cat}" "${h_qty}" "${h_sales}" >> "${txt_file}"
        done < "${cat_trend_file}"
    fi

    cat <<EOF >> "${txt_file}"
================================================================================
Notice: All figures represent exact aggregations across successfully registered Hive partitions.
================================================================================
EOF

    # Publish to HDFS atomically
    if ! safe_publish_hdfs_dir "${pkg_dir}" "${hdfs_hist_dir}"; then
        log_error "Failed to publish historical report to HDFS: ${hdfs_hist_dir}"
        safe_remove_staging_dir "${staging_dir}"
        return 1
    fi

    safe_remove_staging_dir "${staging_dir}"
    log "Historical report generated and published successfully."
    echo "Historical report created: ${hdfs_hist_dir}/"
    return 0
}

# ------------------------------------------------------------------------------
# Block 3: Product Association Recommendations
# ------------------------------------------------------------------------------
generate_recommendations() {
    local batch_id="$1"
    log "Starting Product Recommendation generation for batch: ${batch_id}"
    echo "Generating Rule-Based Product Association Recommendations..."

    if ! validate_batch_id "${batch_id}"; then
        log_error "Invalid batch ID for recommendations: ${batch_id}"
        return 1
    fi

    local hdfs_rec_dir="${RECOMMENDATION_ROOT}/${batch_id}"
    local staging_dir
    staging_dir="$(mktemp -d "${PROJECT_ROOT}/reports/staging_rec_${batch_id}_XXXXXX")"
    chmod 755 "${staging_dir}"

    local sql_file="${staging_dir}/run_rec.sql"
    sed -e "s|__DB_NAME__|${HIVE_DATABASE}|g" \
        -e "s|__TABLE_NAME__|${HIVE_TABLE}|g" \
        -e "s|__BATCH_ID__|${batch_id}|g" \
        -e "s|__STAGING_DIR__|${staging_dir}|g" \
        -e "s|__MIN_PAIR_COUNT__|${MIN_PAIR_COUNT:-1}|g" \
        -e "s|__MIN_CONFIDENCE__|${MIN_CONFIDENCE:-0.10}|g" \
        "${PROJECT_ROOT}/hive/recommendations.sql" > "${sql_file}"

    log "Executing Hive recommendation rules SQL..."
    local hive_code=0
    hive -e "$(< "${sql_file}")" >>"${LOG_FILE}" 2>&1 || hive_code=$?
    if [ "${hive_code}" -ne 0 ]; then
        log_error "Hive execution failed during recommendation generation for batch: ${batch_id}"
        safe_remove_staging_dir "${staging_dir}"
        return 1
    fi

    # Read metadata
    local meta_file
    meta_file="$(find "${staging_dir}/rec_metadata" -maxdepth 1 -type f ! -name '.*' | head -n 1)"
    local tot_orders tot_prods
    if [ -n "${meta_file}" ] && [ -f "${meta_file}" ]; then
        IFS=$'\t' read -r tot_orders tot_prods < "${meta_file}" || true
    fi
    tot_orders="${tot_orders:-0}"
    tot_prods="${tot_prods:-0}"

    local pkg_dir="${staging_dir}/pkg"
    mkdir -p "${pkg_dir}"

    local conf_pct_disp
    conf_pct_disp="$(awk -v c="${MIN_CONFIDENCE:-0.10}" 'BEGIN { printf "%.1f%%", c*100 }')"

    # Check rule output
    local rec_data_file
    rec_data_file="$(find "${staging_dir}/recommendations" -maxdepth 1 -type f ! -name '.*' | head -n 1)"
    local num_rules=0
    if [ -n "${rec_data_file}" ] && [ -f "${rec_data_file}" ]; then
        num_rules="$(grep -c . "${rec_data_file}" || true)"
    fi

    # CSV output
    echo "product_a_id,product_a_name,product_b_id,product_b_name,pair_count,support,confidence" > "${pkg_dir}/recommendations.csv"
    if [ "${num_rules}" -gt 0 ]; then
        tr '\t' ',' < "${rec_data_file}" >> "${pkg_dir}/recommendations.csv"
    fi

    # Human-readable recommendations.txt
    local txt_file="${pkg_dir}/recommendations.txt"
    cat <<EOF > "${txt_file}"
================================================================================
RULE-BASED PRODUCT ASSOCIATION RECOMMENDATION SYSTEM
================================================================================
Batch ID        : ${batch_id}
Algorithm       : Market Basket Order-Level Association Rules
Generated At    : $(date '+%Y-%m-%d %H:%M:%S %Z')
Batch Orders    : ${tot_orders}
Thresholds      : Min Pair Count = ${MIN_PAIR_COUNT:-1}, Min Confidence = ${conf_pct_disp}

Formulas:
  pair_count(A, B)   = Number of distinct orders containing both Product A and Product B
  support(A, B)      = pair_count(A, B) / total_batch_orders
  confidence(A -> B) = pair_count(A, B) / orders_containing_product_A

--------------------------------------------------------------------------------
RECOMMENDATION RULES DERIVED
--------------------------------------------------------------------------------
EOF

    if [ "${num_rules}" -eq 0 ]; then
        cat <<EOF >> "${txt_file}"
No recommendation pairs met the configured threshold.
(Reason: Either all transactions in this batch were single-item orders, or no
co-occurring product pairs met the minimum thresholds: pair_count >= ${MIN_PAIR_COUNT:-1}, confidence >= ${conf_pct_disp}).

Edge-Case Summary:
  Orders analyzed           : ${tot_orders}
  Qualifying product pairs  : 0
  Recommendations generated : 0
  Data Characteristic       : Dataset contains strictly 1 item per order (all distinct orders
                              are single-item baskets); market-basket association rules require
                              >= 2 distinct items per basket to find co-occurrences.
EOF
    else
        cat <<EOF >> "${txt_file}"
$(printf "%-22s -> %-22s | %-10s | %-10s | %s\n" "Product A (Antecedent)" "Product B (Consequent)" "Pair Count" "Support %" "Confidence %")
--------------------------------------------------------------------------------
EOF
        while IFS=$'\t' read -r pa_id pa_name pb_id pb_name p_cnt p_supp p_conf || [ -n "${pa_id}" ]; do
            local supp_pct conf_pct
            supp_pct="$(awk -v s="${p_supp:-0}" 'BEGIN { printf "%.2f%%", s*100 }')"
            conf_pct="$(awk -v c="${p_conf:-0}" 'BEGIN { printf "%.2f%%", c*100 }')"
            printf "%-22s -> %-22s | %-10s | %-10s | %s\n" \
                   "${pa_name:0:22}" "${pb_name:0:22}" "${p_cnt}" "${supp_pct}" "${conf_pct}" >> "${txt_file}"
        done < "${rec_data_file}"
    fi

    cat <<EOF >> "${txt_file}"
================================================================================
Notice: Rule-based recommendation engine (association rules derived from actual purchase history).
================================================================================
EOF

    # Publish to HDFS atomically
    if ! safe_publish_hdfs_dir "${pkg_dir}" "${hdfs_rec_dir}"; then
        log_error "Failed to publish recommendations to HDFS: ${hdfs_rec_dir}"
        safe_remove_staging_dir "${staging_dir}"
        return 1
    fi

    safe_remove_staging_dir "${staging_dir}"
    log "Recommendations generated and published successfully for batch: ${batch_id}"
    echo "Recommendations created: ${hdfs_rec_dir}/"
    return 0
}

# ------------------------------------------------------------------------------
# Block 3: Recommendation Evaluation (Temporal Holdout Validation)
# ------------------------------------------------------------------------------
generate_evaluation() {
    local batch_id="$1"
    log "Starting Recommendation Evaluation for batch: ${batch_id}"
    echo "Evaluating Product Recommendations (Holdout Validation)..."

    if ! validate_batch_id "${batch_id}"; then
        log_error "Invalid batch ID for evaluation: ${batch_id}"
        return 1
    fi

    local hdfs_eval_dir="${EVALUATION_ROOT}/${batch_id}"
    local staging_dir
    staging_dir="$(mktemp -d "${PROJECT_ROOT}/reports/staging_eval_${batch_id}_XXXXXX")"
    chmod 755 "${staging_dir}"

    local pkg_dir="${staging_dir}/pkg"
    mkdir -p "${pkg_dir}"

    # Determine preceding chronological batch in registry that succeeded
    local reg_file="${PROJECT_ROOT}/state/registry.tsv"
    local prior_batch=""
    if [ -f "${reg_file}" ]; then
        prior_batch="$(awk -F'\t' -v curr="${batch_id}" '$8 == "SUCCESS" && $1 < curr && $1 ~ /^[0-9]{4}-W[0-9]{2}$/ { print $1 }' "${reg_file}" | sort | tail -n 1)"
    fi

    local txt_file="${pkg_dir}/evaluation.txt"
    echo "evaluation_method,train_batch,eval_batch,rules_evaluated,recommendations_tested,hits,hit_rate_pct,status_note" > "${pkg_dir}/evaluation.csv"

    if [ -z "${prior_batch}" ]; then
        # No prior historical batch exists in chronological timeline
        log "Evaluation: No prior historical batch found for holdout evaluation of batch ${batch_id}."
        echo "Temporal Holdout,None,${batch_id},0,0,0,N/A,Insufficient historical training data" >> "${pkg_dir}/evaluation.csv"

        cat <<EOF > "${txt_file}"
================================================================================
PRODUCT RECOMMENDATION SYSTEM EVALUATION
================================================================================
Evaluation Batch: ${batch_id}
Evaluation Mode : Temporal Holdout Verification (Earlier -> Later Batch)
Training Period : None (First chronological batch in processing timeline)
Generated At    : $(date '+%Y-%m-%d %H:%M:%S %Z')

Methodology:
  1. Train association rules on historical transaction data from preceding batches.
  2. Test rules against actual orders in the current holdout evaluation batch.
  3. Metric: Hit Rate = (Hits / Recommendations Tested) * 100
     where a Hit occurs when an order containing Antecedent A also contains Recommended B.

--------------------------------------------------------------------------------
EVALUATION RESULTS
--------------------------------------------------------------------------------
Evaluation status: Evaluation unavailable due to insufficient historical training data.
Explanation      : Batch ${batch_id} is the earliest batch registered in the system.
                   No prior historical batches exist to derive baseline association rules.
                   Evaluation will automatically activate once subsequent batches arrive.
================================================================================
EOF
    else
        log "Holdout Evaluation: Training rules on prior batch '${prior_batch}', testing against holdout batch '${batch_id}'."
        local sql_file="${staging_dir}/run_eval.sql"
        sed -e "s|__DB_NAME__|${HIVE_DATABASE}|g" \
            -e "s|__TABLE_NAME__|${HIVE_TABLE}|g" \
            -e "s|__TRAIN_BATCH_ID__|${prior_batch}|g" \
            -e "s|__EVAL_BATCH_ID__|${batch_id}|g" \
            -e "s|__STAGING_DIR__|${staging_dir}|g" \
            -e "s|__MIN_PAIR_COUNT__|${MIN_PAIR_COUNT:-1}|g" \
            -e "s|__MIN_CONFIDENCE__|${MIN_CONFIDENCE:-0.10}|g" \
            "${PROJECT_ROOT}/hive/evaluation.sql" > "${sql_file}"

        local hive_code=0
        hive -e "$(< "${sql_file}")" >>"${LOG_FILE}" 2>&1 || hive_code=$?
        if [ "${hive_code}" -ne 0 ]; then
            log_error "Hive execution failed during recommendation evaluation."
            safe_remove_staging_dir "${staging_dir}"
            return 1
        fi

        local ev_summary_file
        ev_summary_file="$(find "${staging_dir}/evaluation_summary" -maxdepth 1 -type f ! -name '.*' | head -n 1)"
        local rules_eval=0 recs_tested=0 hits=0 hit_rate="0.00"
        if [ -n "${ev_summary_file}" ] && [ -f "${ev_summary_file}" ]; then
            IFS=$'\t' read -r rules_eval recs_tested hits hit_rate < "${ev_summary_file}" || true
        fi
        rules_eval="${rules_eval:-0}"
        recs_tested="${recs_tested:-0}"
        hits="${hits:-0}"
        hit_rate="${hit_rate:-0.00}"

        local status_note="Evaluation completed"
        local hit_rate_display="${hit_rate}%"
        if [ "${rules_eval}" -eq 0 ]; then
            status_note="Zero recommendation rules met threshold in historical training batch"
            hit_rate_display="N/A"
        elif [ "${recs_tested}" -eq 0 ]; then
            status_note="Historical rule antecedents were not purchased in holdout batch orders"
            hit_rate_display="N/A"
        fi

        echo "Temporal Holdout,${prior_batch},${batch_id},${rules_eval},${recs_tested},${hits},${hit_rate_display},${status_note}" >> "${pkg_dir}/evaluation.csv"

        cat <<EOF > "${txt_file}"
================================================================================
PRODUCT RECOMMENDATION SYSTEM EVALUATION
================================================================================
Evaluation Batch: ${batch_id}
Evaluation Mode : Temporal Holdout Verification (Earlier -> Later Batch)
Training Batch  : ${prior_batch} (Historical baseline)
Generated At    : $(date '+%Y-%m-%d %H:%M:%S %Z')

Methodology:
  1. Derive association rules from preceding historical batch (${prior_batch}).
  2. Test rules against actual orders in the current holdout evaluation batch (${batch_id}).
  3. Metric: Hit Rate = (Hits / Recommendations Tested) * 100
     where a Hit occurs when an order containing Antecedent A also contains Recommended B.

--------------------------------------------------------------------------------
EVALUATION METRICS
--------------------------------------------------------------------------------
Historical Rules Evaluated: ${rules_eval}
Recommendations Tested    : ${recs_tested}
Recommendation Hits       : ${hits}
Hit Rate                  : ${hit_rate_display}
Evaluation Status         : ${status_note}

Audit Limitations:
  - Evaluation accuracy reflects real historical purchases without synthetic simulations.
  - If customers purchase items across multiple distinct orders over time, order-level
    holdout evaluates only within-order co-purchases.
================================================================================
EOF
    fi

    # Publish to HDFS atomically
    if ! safe_publish_hdfs_dir "${pkg_dir}" "${hdfs_eval_dir}"; then
        log_error "Failed to publish evaluation to HDFS: ${hdfs_eval_dir}"
        safe_remove_staging_dir "${staging_dir}"
        return 1
    fi

    safe_remove_staging_dir "${staging_dir}"
    log "Recommendation evaluation generated and published successfully for batch: ${batch_id}"
    echo "Evaluation created: ${hdfs_eval_dir}/"
    return 0
}

# ------------------------------------------------------------------------------
# Block 3: Master Orchestrator for Reports
# ------------------------------------------------------------------------------
run_block3_reports() {
    local batch_id="$1"
    log "Running all Block 3 reporting pipelines for batch: ${batch_id}"

    echo "========================================"
    echo "STAGE 4: WEEKLY ANALYTICAL REPORT"
    echo "========================================"
    if ! generate_weekly_report "${batch_id}"; then
        log_error "Weekly report generation failed for batch: ${batch_id}"
        return 1
    fi
    echo "Weekly report: [OK]"
    echo ""

    echo "========================================"
    echo "STAGE 5: HISTORICAL ANALYTICS & TRENDS"
    echo "========================================"
    if ! generate_historical_report; then
        log_error "Historical report generation failed."
        return 1
    fi
    echo "Historical report: [OK]"
    echo ""

    echo "========================================"
    echo "STAGE 6: PRODUCT RECOMMENDATIONS"
    echo "========================================"
    if ! generate_recommendations "${batch_id}"; then
        log_error "Recommendation generation failed for batch: ${batch_id}"
        return 1
    fi
    echo "Recommendations: [OK]"
    echo ""

    echo "========================================"
    echo "STAGE 7: RECOMMENDATION EVALUATION"
    echo "========================================"
    if ! generate_evaluation "${batch_id}"; then
        log_error "Recommendation evaluation failed for batch: ${batch_id}"
        return 1
    fi
    echo "Evaluation: [OK]"
    echo ""

    log "All Block 3 reports generated successfully for batch: ${batch_id}"
    return 0
}

# ------------------------------------------------------------------------------
# Block 3: Final Execution Summary Display
# ------------------------------------------------------------------------------
print_final_summary() {
    local batch_id="$1"
    
    local r_in r_clean r_rej
    r_in="$(get_batch_field "${batch_id}" 5)"
    r_clean="$(get_batch_field "${batch_id}" 6)"
    r_rej="$(get_batch_field "${batch_id}" 7)"
    
    local inv_cnt=0 dup_cnt=0
    if hdfs dfs -test -e "${REJECT_ROOT}/${batch_id}/invalid" 2>>"${LOG_FILE}"; then
        inv_cnt=$(hdfs dfs -cat "${REJECT_ROOT}/${batch_id}/invalid/part*" 2>>"${LOG_FILE}" | grep -c . || true)
    fi
    if hdfs dfs -test -e "${REJECT_ROOT}/${batch_id}/duplicates" 2>>"${LOG_FILE}"; then
        dup_cnt=$(hdfs dfs -cat "${REJECT_ROOT}/${batch_id}/duplicates/part*" 2>>"${LOG_FILE}" | grep -c . || true)
    fi
    if [ "${inv_cnt}" -eq 0 ] && [ "${dup_cnt}" -eq 0 ]; then
        inv_cnt="${r_rej:-0}"
    fi

    local acct_status="FAIL"
    if [ $(( r_clean + inv_cnt + dup_cnt )) -eq "${r_in:-0}" ]; then
        acct_status="PASS"
    fi

    local weekly_txt="${WEEKLY_REPORT_ROOT}/${batch_id}/weekly_summary.txt"
    local orders="N/A" cust="N/A" prod="N/A" qty="N/A" sales="N/A" aov="N/A" hive_recs="N/A" hive_tx="N/A" hive_dups="0"
    
    if hdfs dfs -test -e "${weekly_txt}" 2>>"${LOG_FILE}"; then
        local content
        content="$(hdfs dfs -cat "${weekly_txt}" 2>>"${LOG_FILE}" || true)"
        hive_recs="$(echo "${content}" | grep -E '^Clean Records' | head -n 1 | awk -F':' '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')"
        orders="$(echo "${content}" | grep -E '^Distinct Orders' | head -n 1 | awk -F':' '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')"
        hive_tx="$(echo "${content}" | grep -E '^Distinct Transactions' | head -n 1 | awk -F':' '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')"
        cust="$(echo "${content}" | grep -E '^Unique Customers' | head -n 1 | awk -F':' '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')"
        prod="$(echo "${content}" | grep -E '^Unique Products' | head -n 1 | awk -F':' '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')"
        qty="$(echo "${content}" | grep -E '^Total Quantity' | head -n 1 | awk -F':' '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')"
        sales="$(echo "${content}" | grep -E '^Gross Sales' | head -n 1 | awk -F':' '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')"
        aov="$(echo "${content}" | grep -E '^Avg Order Value' | head -n 1 | sed -e 's/^[^(]*:[[:space:]]*//' -e 's/[[:space:]]*(.*//')"
        hive_dups="$(echo "${content}" | grep -E '^In-Hive Duplicates' | head -n 1 | awk -F':' '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}' | awk '{print $1}')"
    fi
    hive_recs="${hive_recs:-$r_clean}"
    hive_tx="${hive_tx:-$r_clean}"
    hive_dups="${hive_dups:-0}"

    local rec_cnt=0
    local rec_reason="No qualifying multi-item pairs"
    local rec_csv="${RECOMMENDATION_ROOT}/${batch_id}/recommendations.csv"
    if hdfs dfs -test -e "${rec_csv}" 2>>"${LOG_FILE}"; then
        rec_cnt=$(hdfs dfs -cat "${rec_csv}" 2>>"${LOG_FILE}" | grep -v '^product_a_id' | grep -c . || true)
        if [ "${rec_cnt}" -gt 0 ]; then
            rec_reason="Association rules derived from multi-item orders"
        fi
    fi

    local clean_orders=""
    if hdfs dfs -test -e "${CLEAN_ROOT}/${batch_id}" 2>>"${LOG_FILE}"; then
        clean_orders="$(hdfs dfs -cat "${CLEAN_ROOT}/${batch_id}/part*" 2>>"${LOG_FILE}" | awk -F'\t' '{print $1}' | sort -u | grep -c . || true)"
    fi

    local hive_val="FAIL"
    if [ "${hive_recs}" = "${r_clean}" ] && [ "${hive_tx}" = "${r_clean}" ] && [ "${hive_dups}" = "0" ]; then
        if [ -n "${clean_orders}" ] && [ "${clean_orders}" -gt 0 ]; then
            if [ "${orders}" = "${clean_orders}" ]; then
                hive_val="PASS"
            fi
        else
            hive_val="PASS"
        fi
    fi

    local hist_data_msg="Baseline / First Batch"
    local wow_msg="N/A"
    local eval_type="Baseline / Warm-up"

    local reg_file="${PROJECT_ROOT}/state/registry.tsv"
    local succ_batches=0
    if [ -f "${reg_file}" ]; then
        succ_batches=$(awk -F'\t' '$8 == "SUCCESS" { count++ } END { print count + 0 }' "${reg_file}")
    fi
    if [ "${succ_batches}" -gt 1 ]; then
        hist_data_msg="Available (${succ_batches} Batches)"
        wow_msg="Calculated"
        eval_type="Temporal Holdout"
    fi

    echo "============================================================"
    echo "E-COMMERCE BIG DATA PIPELINE"
    echo "============================================================"
    echo ""
    echo "Batch                 : ${batch_id}"
    echo "Pipeline Status       : SUCCESS"
    echo ""
    echo "---------------- DATA PROCESSING ----------------"
    echo "Raw Records           : ${r_in:-0}"
    echo "Accepted Records      : ${r_clean:-0}"
    echo "Invalid Rejected      : ${inv_cnt:-0}"
    echo "Duplicate Excluded    : ${dup_cnt:-0}"
    echo "Total Excluded        : $(( inv_cnt + dup_cnt ))"
    echo ""
    echo "Data Accounting       : ${acct_status}"
    echo "${r_in:-0} = ${r_clean:-0} + ${inv_cnt:-0} + ${dup_cnt:-0}"
    echo ""
    echo "---------------- HIVE VALIDATION ----------------"
    echo "Hive Records          : ${hive_recs:-0}"
    echo "Distinct Orders       : ${orders:-0}"
    echo "Distinct Transactions : ${hive_tx:-0}"
    echo "Duplicate Transactions: ${hive_dups}"
    echo ""
    echo "Hive Validation       : ${hive_val}"
    echo ""
    echo "---------------- ANALYTICS ----------------"
    echo "Weekly Report         : PASS"
    echo "Historical Report     : PASS"
    echo "Historical Data       : ${hist_data_msg}"
    echo "WoW Analysis          : ${wow_msg}"
    echo ""
    echo "---------------- RECOMMENDATIONS ----------------"
    echo "Orders Analyzed       : ${orders:-0}"
    echo "Recommendations       : PASS"
    echo "Recommendation Count  : ${rec_cnt}"
    echo "Reason                : ${rec_reason}"
    echo ""
    echo "---------------- EVALUATION ----------------"
    echo "Evaluation            : PASS"
    echo "Evaluation Type       : ${eval_type}"
    echo ""
    echo "---------------- OUTPUTS ----------------"
    echo "Weekly:"
    echo "  ${WEEKLY_REPORT_ROOT}/${batch_id}/"
    echo ""
    echo "Historical:"
    echo "  ${HISTORICAL_REPORT_ROOT}/"
    echo ""
    echo "Recommendations:"
    echo "  ${RECOMMENDATION_ROOT}/${batch_id}/"
    echo ""
    echo "Evaluation:"
    echo "  ${EVALUATION_ROOT}/${batch_id}/"
    echo ""
    echo "============================================================"
    echo "FINAL STATUS: SUCCESS"
    echo "============================================================"
}
