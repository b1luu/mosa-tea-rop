# Mosa Tea Reorder Point Pipeline

[![CI](https://github.com/b1luu/mosa-tea-rop/actions/workflows/ci.yml/badge.svg)](https://github.com/b1luu/mosa-tea-rop/actions/workflows/ci.yml)
[![CD](https://github.com/b1luu/mosa-tea-rop/actions/workflows/cd.yml/badge.svg)](https://github.com/b1luu/mosa-tea-rop/actions/workflows/cd.yml)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Pandas](https://img.shields.io/badge/pandas-required-150458)

Privacy-safe pipeline for Mosa Tea reorder-point analysis: clean Square exports, canonicalize items/modifiers, resolve tea-base/topping usage, and produce analysis + debug CSVs with validation reports and CI tests for reliable inventory planning.

## Pipeline

1. Clean raw Square exports: `src/clean.py` -> `data/trim/clean.csv`
2. Canonicalize items/modifiers: `src/canonicalize.py` -> `data/trim/canonicalized.csv` + `data/trim/canonicalized_line_items.csv`
3. Estimate usage: `src/estimate_usage.py` -> `data/analysis/*.csv`

## Key Outputs

- `data/trim/clean.csv` cleaned source rows
- `data/trim/canonicalized.csv` canonicalized (one row per original line item)
- `data/trim/canonicalized_line_items.csv` exploded (one row per drink)
- `data/analysis/usage_line_items.csv` per-drink usage estimates
- `data/analysis/usage_components.csv` per-drink tea-component usage
- `data/analysis/usage_summary.csv` daily component totals
- `data/analysis/usage_weekday_summary.csv` weekday averages by component
- `data/analysis/usage_monthly_weekday_summary.csv` month+weekday averages by component
- `data/analysis/usage_validation.csv` pipeline validation metrics

## Usage

```bash
python3 src/clean.py
python3 src/canonicalize.py
python3 src/estimate_usage.py
```

Optional date filter:

```bash
python3 src/estimate_usage.py --start-date 2026-01-01 --end-date 2026-01-31
```

## Recipe Config (Data-Driven)

Primary override table: `data/reference/recipe_simple.csv`

Columns:
- `category`, `item_name`
- `tea_base_ml`, `milk_ml`
- `ice` (`ice (per ice level)`, `100% ice`, `no ice`)
- `match_tokens` (optional substring matcher, pipe-separated)
- `tea_base_ml_0`, `tea_base_ml_25`, `tea_base_ml_50`, `tea_base_ml_75`, `tea_base_ml_100` (ice-based defaults)

Example row (JavaScript-labeled for readability):

```javascript
{
  "item_name": "Hot Au Lait",
  "match_tokens": "hot|au lait",
  "tea_base_ml": 200,
  "milk_ml": 150,
  "ice": "no ice"
}
```

## Usage Logic (Summary)

- Ice-based tea volume uses manual sample means.
- 0% ice defaults to `550 ml` unless overridden.
- Toppings reduce tea volume by `10%` per topping, capped at `20%`.
- Milk drinks split total volume by tea/milk ratio from `recipe_simple.csv`.
- Forced ice values (`100% ice`, `no ice`) override the ice bucket.

Example (JavaScript-labeled):

```javascript
// tea_base_ml_est = base_tea_ml * (1 - topping_reduction_pct)
// milk_ml_est = base_total_ml * milk_ratio (for milk drinks)
```

## Design Notes

- Keep two outputs by design: `canonicalized.csv` for analysis and `canonicalized_debug.csv` for audits.
- Parse modifiers into structured fields (`ice_pct`, `sugar_pct`, toppings, tea override) to reduce free-text dependency.
- Preserve privacy in CI/CD with synthetic fixtures only; no raw production data is required.
- Track mapping drift using `unknown_modifier_tokens.csv` so token map updates are explicit and testable.
