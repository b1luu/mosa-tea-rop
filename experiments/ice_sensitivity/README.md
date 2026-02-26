# Ice Sensitivity Experiments

This folder is for analyzing how ice variation impacts batch yield and bag usage.

## What We’re Testing
- TGY ice input can vary (e.g., 2400–2900 g).
- That range changes batch yield by ~500 ml (~8%).
- We want to understand how that shifts bag usage and inventory gaps.

## Suggested Outputs
- A CSV that sweeps ice grams and recomputes:
  - `yield_ml`
  - `batches_needed`
  - `bags_used`
- A small chart showing sensitivity vs. ice grams.

## Notes
- Keep experiment code isolated here.
- Do not modify core pipeline unless the experiment is accepted.
