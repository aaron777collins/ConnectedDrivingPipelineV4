#!/bin/bash
# Robust download script for large BSM dataset from data.transportation.gov
# Handles large CSV files with resume support, integrity checking, progress tracking
# Updated: Uses public API endpoint (no auth required)

set -euo pipefail

# Configuration - Updated URL to use public API v1 endpoint
DATA_URL="https://data.transportation.gov/api/views/9k4m-a3jc/rows.csv?accessType=DOWNLOAD"
OUTPUT_DIR="${OUTPUT_DIR:-/home/ubuntu/ConnectedDrivingPipelineV4/data}"
OUTPUT_FILE="${OUTPUT_DIR}/bsm_data.csv"
TEMP_FILE="${OUTPUT_DIR}/bsm_data.csv.part"
LOG_FILE="${OUTPUT_DIR}/download.log"
PROGRESS_FILE="${OUTPUT_DIR}/download_progress.json"

# Download settings
MAX_RETRIES=10
RETRY_DELAY=30  # seconds
TIMEOUT=600  # 10 min timeout per chunk
MIN_SPEED=1000  # Minimum bytes/sec before abort

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${BLUE}[$timestamp]${NC} $1" | tee -a "$LOG_FILE"
}

log_success() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${GREEN}[$timestamp] ✓${NC} $1" | tee -a "$LOG_FILE"
}

log_warn() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${YELLOW}[$timestamp] ⚠${NC} $1" | tee -a "$LOG_FILE"
}

log_error() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${RED}[$timestamp] ✗${NC} $1" | tee -a "$LOG_FILE"
}

update_progress() {
    local status="$1"
    local bytes_downloaded="${2:-0}"
    local total_bytes="${3:-0}"
    local attempt="${4:-1}"

    cat > "$PROGRESS_FILE" << EOF
{
    "status": "$status",
    "bytes_downloaded": $bytes_downloaded,
    "total_bytes": $total_bytes,
    "attempt": $attempt,
    "last_update": "$(date -Iseconds)",
    "output_file": "$OUTPUT_FILE"
}
EOF
}

get_local_size() {
    if [[ -f "$TEMP_FILE" ]]; then
        stat -c%s "$TEMP_FILE" 2>/dev/null || echo "0"
    elif [[ -f "$OUTPUT_FILE" ]]; then
        stat -c%s "$OUTPUT_FILE" 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

download_streaming() {
    local attempt=1

    log "Note: This endpoint uses chunked transfer (streaming). Cannot determine size upfront."

    while [[ $attempt -le $MAX_RETRIES ]]; do
        local local_size=$(get_local_size)
        log "Attempt $attempt/$MAX_RETRIES - Starting download..."

        update_progress "downloading" "$local_size" "0" "$attempt"

        # Build curl command for streaming download
        local curl_opts=(
            --location                      # Follow redirects
            --fail                          # Fail on HTTP errors
            --retry 3                       # Built-in retry for transient errors
            --retry-delay 5                 # Delay between retries
            --connect-timeout 30            # Connection timeout
            --max-time 0                    # No overall timeout (file might be huge)
            --speed-limit $MIN_SPEED        # Minimum bytes/sec
            --speed-time 120                # Tolerate slow speed for 2 min
            --progress-bar                  # Show progress
            --output "$TEMP_FILE"           # Output file
            -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # Execute download
        set +e
        curl "${curl_opts[@]}" "$DATA_URL" 2>&1 | tee -a "$LOG_FILE"
        local exit_code=$?
        set -e

        # Check result
        if [[ $exit_code -eq 0 ]]; then
            log_success "Download completed successfully!"
            mv "$TEMP_FILE" "$OUTPUT_FILE"
            local final_size=$(stat -c%s "$OUTPUT_FILE")
            update_progress "completed" "$final_size" "$final_size" "$attempt"
            return 0
        elif [[ $exit_code -eq 28 ]]; then
            log_warn "Timeout/slow transfer occurred. Retrying..."
        elif [[ $exit_code -eq 18 ]]; then
            log_warn "Partial transfer. Will retry fresh (streaming doesn't support resume)..."
            rm -f "$TEMP_FILE"
        else
            log_error "Download failed with exit code $exit_code"
        fi

        attempt=$((attempt + 1))

        if [[ $attempt -le $MAX_RETRIES ]]; then
            log "Waiting ${RETRY_DELAY}s before retry..."
            sleep $RETRY_DELAY
        fi
    done

    log_error "Download failed after $MAX_RETRIES attempts"
    update_progress "failed" "$(get_local_size)" "0" "$MAX_RETRIES"
    return 1
}

verify_download() {
    log "Verifying downloaded file..."

    if [[ ! -f "$OUTPUT_FILE" ]]; then
        log_error "Output file not found: $OUTPUT_FILE"
        return 1
    fi

    local file_size=$(stat -c%s "$OUTPUT_FILE")
    log "File size: $(numfmt --to=iec-i --suffix=B $file_size)"

    # Check if file has content
    if [[ $file_size -lt 1000 ]]; then
        log_error "File too small - likely failed download"
        return 1
    fi

    # Check if it's valid CSV (has headers)
    local header=$(head -1 "$OUTPUT_FILE")
    if [[ -z "$header" ]]; then
        log_error "File appears empty or corrupted"
        return 1
    fi

    log "Header columns: $(echo "$header" | tr ',' '\n' | wc -l)"

    # Count total lines (may take a while for large files)
    log "Counting rows (this may take a moment for large files)..."
    local total_lines=$(wc -l < "$OUTPUT_FILE")
    log "Total rows (including header): $(numfmt --grouping $total_lines)"

    # Calculate average line size and storage info
    local avg_line_size=$((file_size / total_lines))
    log "Average row size: $avg_line_size bytes"

    log_success "File verification passed"
    log "Dataset: Wyoming CV Pilot BSM"
    log "Rows: $((total_lines - 1)) (excluding header)"
    return 0
}

main() {
    log "========================================"
    log "BSM Data Download Script"
    log "========================================"
    log "URL: $DATA_URL"
    log "Output: $OUTPUT_FILE"
    log ""

    # Create output directory
    mkdir -p "$OUTPUT_DIR"

    # Check for existing complete file
    if [[ -f "$OUTPUT_FILE" ]]; then
        local existing_size=$(stat -c%s "$OUTPUT_FILE")
        log_warn "Output file already exists: $(numfmt --to=iec-i --suffix=B $existing_size)"
        log "Use FORCE=1 to re-download"
        if [[ "${FORCE:-0}" != "1" ]]; then
            verify_download
            exit 0
        fi
        rm -f "$OUTPUT_FILE"
    fi

    # Remove any partial downloads (streaming doesn't support resume well)
    if [[ -f "$TEMP_FILE" ]]; then
        log_warn "Removing partial download (fresh start required for streaming)"
        rm -f "$TEMP_FILE"
    fi

    # Start download
    update_progress "starting" "0" "0" "1"

    if download_streaming; then
        verify_download
        log_success "Download complete: $OUTPUT_FILE"
        exit 0
    else
        log_error "Download failed"
        exit 1
    fi
}

# Run main function
main "$@"
