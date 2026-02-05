#!/bin/bash
# Run pipeline on Jaekel server (64GB RAM) with auto-copy to static

cd "$(dirname "$0")"
source venv/bin/activate

# Default pipeline if none specified
PIPELINE="${1:-MClassifierLargePipelineUserJNoCacheWithXYOffsetPos2000mDistRandSplit80PercentTrain20PercentTestAllRowsONLYXYELEVCols30attackersConstPosPerCarOffset10To20xN106y41d01to30m04y2021.py}"

# Create timestamped results directory
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="/var/www/static/pipeline-results/${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"

echo "=== Pipeline Run ==="
echo "Time: $(date)"
echo "Host: $(hostname)"
echo "Pipeline: $PIPELINE"
echo "Results will be copied to: $RESULTS_DIR"
echo "==================="

# Set Dask config
export DASK_CONFIG="configs/dask/64gb-production.yml"

# Run pipeline
python "$PIPELINE" 2>&1 | tee "$RESULTS_DIR/pipeline.log"
EXIT_CODE=${PIPESTATUS[0]}

# Copy results (excluding large data files)
echo "Copying results to static folder..."
find data -name "*.csv" -size -100M -exec cp {} "$RESULTS_DIR/" \; 2>/dev/null
find data -name "*.json" -exec cp {} "$RESULTS_DIR/" \; 2>/dev/null
find data -name "*.png" -exec cp {} "$RESULTS_DIR/" \; 2>/dev/null
find . -maxdepth 1 -name "*.csv" -size -100M -exec cp {} "$RESULTS_DIR/" \; 2>/dev/null
find . -maxdepth 1 -name "*.png" -exec cp {} "$RESULTS_DIR/" \; 2>/dev/null

# Create index file
echo "Pipeline: $PIPELINE" > "$RESULTS_DIR/README.txt"
echo "Run at: $(date)" >> "$RESULTS_DIR/README.txt"
echo "Exit code: $EXIT_CODE" >> "$RESULTS_DIR/README.txt"
ls -lh "$RESULTS_DIR" >> "$RESULTS_DIR/README.txt"

echo "==================="
echo "Results available at: http://65.108.237.46/pipeline-results/${TIMESTAMP}/"
echo "Exit code: $EXIT_CODE"
