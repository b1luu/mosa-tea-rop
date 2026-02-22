# Mosa Tea Reorder Point Pipeline

Privacy-safe pipeline for Mosa Tea reorder-point analysis: clean Square exports, canonicalize items/modifiers, resolve tea-base/topping usage and produce analysis and debug CSVs with unknown-token reports and CI tests for reliable inventory planning.

## Design Notes

- Keep two outputs by design: `canonicalized.csv` for analysis and `canonicalized_debug.csv` for audits.
- Parse modifiers into structured fields (`ice_pct`, `sugar_pct`, toppings, tea override) to reduce free-text dependency.
- Preserve privacy in CI/CD with synthetic fixtures only; no raw production data is required.
- Track mapping drift using `unknown_modifier_tokens.csv` so token map updates are explicit and testable.
