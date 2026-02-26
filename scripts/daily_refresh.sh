#!/usr/bin/env bash
set -euo pipefail

RAW_INPUT=${1:-data/raw/daily_export.csv}
CLEAN_OUTPUT=${CLEAN_OUTPUT:-data/trim/clean.csv}
CANON_OUTPUT=${CANON_OUTPUT:-data/trim/canonicalized.csv}
LINE_ITEMS_OUTPUT=${LINE_ITEMS_OUTPUT:-data/trim/canonicalized_line_items.csv}

if [ ! -f "$RAW_INPUT" ]; then
  echo "Missing raw input: $RAW_INPUT"
  echo "Pass the export path as the first argument, e.g.:"
  echo "  scripts/daily_refresh.sh data/raw/square_export.csv"
  exit 1
fi

python3 src/clean.py --input "$RAW_INPUT" --output "$CLEAN_OUTPUT"
python3 src/canonicalize.py --input "$CLEAN_OUTPUT" --output "$CANON_OUTPUT"

python3 src/estimate_usage.py \
  --input "$LINE_ITEMS_OUTPUT" \
  --output data/analysis/usage_line_items.csv \
  --component-output data/analysis/usage_components.csv \
  --summary-output data/analysis/usage_summary.csv \
  --weekday-output data/analysis/usage_weekday_summary.csv \
  --monthly-weekday-output data/analysis/usage_monthly_weekday_summary.csv \
  --validation-output data/analysis/usage_validation.csv

python3 src/batch_yield_model.py
python3 src/merge_usage_with_batch_yield.py
python3 src/tgy_usage_audit.py
python3 src/tea_jelly_usage.py \
  --input data/analysis/usage_line_items.csv \
  --output data/analysis/tea_jelly_usage_summary.csv

echo "Daily refresh complete."
