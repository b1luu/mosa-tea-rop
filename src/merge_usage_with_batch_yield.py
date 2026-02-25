"""Combine weekday usage summary with batch yield estimates."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


TEA_COMPONENT_TO_BATCH_KEY = {
    "tie_guan_yin": "tie_guan_yin",
    "four_seasons": "four_seasons",
    "green": "green_tea",
    "genmai": "genmai",
    "black": "matured_black",
    # Treat buckwheat_barley as buckwheat batch by default.
    "buckwheat_barley": "buckwheat",
    # Matcha is a separate concentrate process; no batch yield mapping here.
    "matcha": "",
}

DEFAULT_BATCH_YIELD_ML = 800


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge usage_weekday_summary with batch_yield_estimates."
    )
    parser.add_argument(
        "--usage",
        default="data/analysis/usage_weekday_summary.csv",
        help="Usage weekday summary CSV path.",
    )
    parser.add_argument(
        "--batches",
        default="data/analysis/batch_yield_estimates.csv",
        help="Batch yield estimates CSV path.",
    )
    parser.add_argument(
        "--output",
        default="data/analysis/usage_weekday_with_batch_yield.csv",
        help="Output CSV path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    usage_path = Path(args.usage)
    batch_path = Path(args.batches)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    usage = pd.read_csv(usage_path)
    batch = pd.read_csv(batch_path)

    usage["batch_key"] = usage["tea_component"].map(TEA_COMPONENT_TO_BATCH_KEY).fillna("")
    batch = batch.rename(columns={"tea_key": "batch_key", "yield_ml": "batch_yield_ml"})

    merged = usage.merge(
        batch[["batch_key", "batch_yield_ml"]],
        on="batch_key",
        how="left",
    )
    merged["batch_yield_ml"] = merged["batch_yield_ml"].fillna(DEFAULT_BATCH_YIELD_ML)
    merged["avg_batches_needed"] = merged["avg_tea_ml_total"] / merged["batch_yield_ml"]

    merged.to_csv(output_path, index=False)
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
