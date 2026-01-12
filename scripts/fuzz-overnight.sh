#!/bin/bash
# Overnight fuzz testing script for go-openexr
# Runs each fuzz test with duration based on priority

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

LOG_DIR="${LOG_DIR:-$PROJECT_ROOT/fuzz-logs}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_FILE="$LOG_DIR/fuzz-$TIMESTAMP.log"

mkdir -p "$LOG_DIR"

echo "=== go-openexr Overnight Fuzz Testing ===" | tee "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"
echo "Log file: $LOG_FILE" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Durations by priority
HIGH_TIME="${HIGH_TIME:-1h}"      # 1 hour for critical security tests
MEDIUM_TIME="${MEDIUM_TIME:-30m}" # 30 min for important tests
LOW_TIME="${LOW_TIME:-15m}"       # 15 min for lower risk tests

# High-priority tests (most likely to find security issues) - 10 tests × 1h = 10h
HIGH_PRIORITY=(
    "./exr/:FuzzOpenReader"
    "./exr/:FuzzScanlineReader"
    "./exr/:FuzzTiledReader"
    "./exr/:FuzzAttributeValue"
    "./compression/:FuzzRLEDecompress"
    "./compression/:FuzzZIPDecompress"
    "./compression/:FuzzPIZDecompress"
    "./compression/:FuzzDWADecompress"
    "./compression/:FuzzB44Decompress"
    "./compression/:FuzzPXR24Decompress"
)

# Medium-priority tests - 8 tests × 30m = 4h
MEDIUM_PRIORITY=(
    "./exr/:FuzzReadHeader"
    "./compression/:FuzzRLERoundtrip"
    "./compression/:FuzzZIPRoundtrip"
    "./compression/:FuzzPIZRoundtrip"
    "./compression/:FuzzInterleave"
    "./internal/xdr/:FuzzReaderReadString"
    "./internal/xdr/:FuzzReaderReadBytes"
    "./internal/xdr/:FuzzWriterRoundtrip"
)

# Lower-priority tests (simpler code) - 20 tests × 15m = 5h
LOW_PRIORITY=(
    "./half/:FuzzFromFloat32"
    "./half/:FuzzFromBits"
    "./half/:FuzzBatchConvert"
    "./half/:FuzzMultiplyBatch"
    "./half/:FuzzAddBatch"
    "./half/:FuzzLerpBatch"
    "./half/:FuzzHalfNeg"
    "./half/:FuzzHalfAbs"
    "./half/:FuzzHalfComparison"
    "./half/:FuzzHalfString"
    "./half/:FuzzConvertBytesToFloat32"
    "./half/:FuzzConvertFloat32ToBytes"
    "./exrid/:FuzzMurmur3Hash"
    "./exrid/:FuzzManifest"
    "./exrid/:FuzzChannelGroupManifest"
    "./exrid/:FuzzInsertHashed"
    "./internal/xdr/:FuzzReaderReadInt"
    "./internal/xdr/:FuzzReaderReadFloat"
    "./internal/xdr/:FuzzReaderPositioning"
    "./internal/xdr/:FuzzReaderEdgeCases"
)

FAILED_TESTS=()

# Convert duration string (e.g., "1h", "30m", "10s") to seconds
duration_to_seconds() {
    local dur=$1
    if [[ $dur =~ ^([0-9]+)h$ ]]; then
        echo $((${BASH_REMATCH[1]} * 3600))
    elif [[ $dur =~ ^([0-9]+)m$ ]]; then
        echo $((${BASH_REMATCH[1]} * 60))
    elif [[ $dur =~ ^([0-9]+)s$ ]]; then
        echo ${BASH_REMATCH[1]}
    else
        echo 60  # default 1 minute
    fi
}

run_fuzz() {
    local pkg=$1
    local test=$2
    local duration=$3
    echo "----------------------------------------" | tee -a "$LOG_FILE"
    echo "[$test] Starting at $(date) (duration: $duration)" | tee -a "$LOG_FILE"

    # Calculate timeout (duration + 60s buffer)
    local dur_secs=$(duration_to_seconds "$duration")
    local timeout_secs=$((dur_secs + 60))

    if timeout --signal=KILL ${timeout_secs}s \
       go test -fuzz="$test" -fuzztime="$duration" "$pkg" >> "$LOG_FILE" 2>&1; then
        echo "[$test] PASSED" | tee -a "$LOG_FILE"
    else
        local exit_code=$?
        if [ $exit_code -eq 137 ]; then
            echo "[$test] TIMEOUT (killed)" | tee -a "$LOG_FILE"
        else
            echo "[$test] FAILED (exit code: $exit_code)" | tee -a "$LOG_FILE"
            FAILED_TESTS+=("$test")
        fi
    fi
}

echo "=== High Priority Tests (${#HIGH_PRIORITY[@]} tests × $HIGH_TIME) ===" | tee -a "$LOG_FILE"
for entry in "${HIGH_PRIORITY[@]}"; do
    pkg="${entry%%:*}"
    test="${entry##*:}"
    run_fuzz "$pkg" "$test" "$HIGH_TIME"
done

echo "" | tee -a "$LOG_FILE"
echo "=== Medium Priority Tests (${#MEDIUM_PRIORITY[@]} tests × $MEDIUM_TIME) ===" | tee -a "$LOG_FILE"
for entry in "${MEDIUM_PRIORITY[@]}"; do
    pkg="${entry%%:*}"
    test="${entry##*:}"
    run_fuzz "$pkg" "$test" "$MEDIUM_TIME"
done

echo "" | tee -a "$LOG_FILE"
echo "=== Low Priority Tests (${#LOW_PRIORITY[@]} tests × $LOW_TIME) ===" | tee -a "$LOG_FILE"
for entry in "${LOW_PRIORITY[@]}"; do
    pkg="${entry%%:*}"
    test="${entry##*:}"
    run_fuzz "$pkg" "$test" "$LOW_TIME"
done

echo "" | tee -a "$LOG_FILE"
echo "=== Fuzz Testing Complete ===" | tee -a "$LOG_FILE"
echo "Finished: $(date)" | tee -a "$LOG_FILE"

# Summary
echo "" | tee -a "$LOG_FILE"
echo "=== Summary ===" | tee -a "$LOG_FILE"
passed=$(grep -c "PASSED" "$LOG_FILE" || echo 0)
failed=$(grep -c "FAILED" "$LOG_FILE" || echo 0)
echo "Passed: $passed" | tee -a "$LOG_FILE"
echo "Failed: $failed" | tee -a "$LOG_FILE"

# Check for any new crashes saved
crash_count=$(find "$PROJECT_ROOT" -path "*/testdata/fuzz/*" -type f 2>/dev/null | wc -l | tr -d ' ')
echo "Total crash corpus files: $crash_count" | tee -a "$LOG_FILE"

if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
    echo "" | tee -a "$LOG_FILE"
    echo "Failed tests:" | tee -a "$LOG_FILE"
    for t in "${FAILED_TESTS[@]}"; do
        echo "  - $t" | tee -a "$LOG_FILE"
    done
fi

echo "" | tee -a "$LOG_FILE"
echo "Log saved to: $LOG_FILE"
