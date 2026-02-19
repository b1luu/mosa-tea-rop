# Tests

This folder contains automated validation for canonicalization logic.

## Run tests locally

```bash
python3 -m unittest discover -s tests -p "test_*.py" -v
```

## Current automated edge cases

- Writes both `canonicalized.csv` (slim) and `canonicalized_debug.csv` (full).
- Blend default applies when no tea override is selected.
- Tea override takes precedence over blend default.
- `requires_tea_choice=1` without override resolves to `missing_choice`.
- Multiple tea overrides on a single row resolve to `conflict`.

## Recommended manual sanity checks after running pipeline

- `tea_resolution` distribution looks plausible for current menu mix.
- No unresolved `unknown` values for mapped drinks.
- Blend strings reflect weighted shares and sum to 1.0 per blend item.
- Fresh fruit tea rows are either `override` or `missing_choice`.
