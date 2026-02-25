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

Example (for reference):

```
tea_base_ml_est = base_tea_ml * (1 - topping_reduction_pct)
milk_ml_est = base_total_ml * milk_ratio (for milk drinks)
```

## Example Output (Tie Guan Yin)

Sample from `usage_weekday_summary.csv` (tie_guan_yin only). Replace with your own run data:

| weekday   | tea_component | avg_tea_ml_total | avg_drink_count | days_count |
|-----------|---------------|------------------|-----------------|------------|
| Monday    | tie_guan_yin   | 16490.96         | 43.42           | 24         |
| Tuesday   | tie_guan_yin   | 16524.35         | 43.26           | 23         |
| Wednesday | tie_guan_yin   | 17345.30         | 45.22           | 23         |
| Thursday  | tie_guan_yin   | 18055.63         | 47.21           | 24         |
| Friday    | tie_guan_yin   | 25732.58         | 67.58           | 24         |
| Saturday  | tie_guan_yin   | 31882.13         | 83.79           | 24         |
| Sunday    | tie_guan_yin   | 29285.29         | 77.42           | 24         |

## Example Output (TGY Monthly Bag Usage)

Sample from `tgy_monthly_bag_usage.csv` (full months only). Replace with your own run data:

```
month  days_covered  days_in_month  tgy_ml_total  batch_yield_ml  leaf_grams_per_batch  bag_grams  batches_needed  leaf_grams_used  bags_used
2025-09            30             30      716141.0          6504.0                 160.0      600.0          110.11         17617.24      29.36
2025-10            31             31      706924.0          6504.0                 160.0      600.0          108.69         17390.50      28.98
2025-11            30             30      609384.0          6504.0                 160.0      600.0           93.69         14991.00      24.98
2025-12            31             31      645441.0          6504.0                 160.0      600.0           99.24         15878.01      26.46
2026-01            31             31      673929.0          6504.0                 160.0      600.0          103.62         16578.82      27.63
```

## Design Notes

- Keep two outputs by design: `canonicalized.csv` for analysis and `canonicalized_debug.csv` for audits.
- Parse modifiers into structured fields (`ice_pct`, `sugar_pct`, toppings, tea override) to reduce free-text dependency.
- Preserve privacy in CI/CD with synthetic fixtures only; no raw production data is required.
- Track mapping drift using `unknown_modifier_tokens.csv` so token map updates are explicit and testable.
