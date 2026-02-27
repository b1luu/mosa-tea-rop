"""Estimate TGY bag usage with component displacement adjustments."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estimate TGY bag usage with sugar/creamer displacement."
    )
    parser.add_argument(
        "--components",
        default="data/analysis/usage_components.csv",
        help="Usage components CSV path.",
    )
    parser.add_argument(
        "--ingredients-summary",
        default="data/analysis/usage_ingredients_summary.csv",
        help="Usage ingredients summary CSV path.",
    )
    parser.add_argument(
        "--batch-estimates",
        default="data/analysis/batch_yield_estimates.csv",
        help="Batch yield estimates CSV path.",
    )
    parser.add_argument(
        "--output",
        default="data/analysis/tgy_monthly_bag_usage_full_components.csv",
        help="Output CSV path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    components = pd.read_csv(args.components)
    components["Date"] = pd.to_datetime(components["Date"], errors="coerce")
    components = components.dropna(subset=["Date"])
    components["month"] = components["Date"].dt.to_period("M").astype(str)

    tgy_components = components[
        components["tea_component"].astype(str).str.strip().eq("tie_guan_yin")
    ].copy()
    base_monthly = (
        tgy_components.groupby("month", as_index=False)["tea_component_ml_est"]
        .sum()
        .rename(columns={"tea_component_ml_est": "tgy_ml_base"})
    )

    ingredients = pd.read_csv(args.ingredients_summary)
    ingredients["Date"] = pd.to_datetime(ingredients["Date"], errors="coerce")
    ingredients = ingredients.dropna(subset=["Date"])
    ingredients["month"] = ingredients["Date"].dt.to_period("M").astype(str)

    sugar = ingredients[
        ingredients["component_key"].astype(str).str.strip().eq("sugar_syrup")
    ].copy()
    sugar_monthly = sugar.groupby("month", as_index=False)["qty_total"].sum().rename(
        columns={"qty_total": "sugar_grams"}
    )

    creamer = ingredients[
        ingredients["component_key"].astype(str).str.strip().eq("non_dairy_creamer")
    ].copy()
    creamer_monthly = creamer.groupby("month", as_index=False)["qty_total"].sum().rename(
        columns={"qty_total": "creamer_grams"}
    )

    merged = base_monthly.merge(sugar_monthly, on="month", how="left")
    merged = merged.merge(creamer_monthly, on="month", how="left")
    merged = merged.fillna({"sugar_grams": 0, "creamer_grams": 0})

    merged["tgy_ml_adjusted"] = (
        merged["tgy_ml_base"]
        - merged["sugar_grams"]
        - merged["creamer_grams"]
    )
    merged.loc[merged["tgy_ml_adjusted"] < 0, "tgy_ml_adjusted"] = 0

    batch = pd.read_csv(args.batch_estimates)
    row = batch[batch["tea_key"].astype(str).str.strip().eq("tie_guan_yin")].iloc[0]
    yield_ml = float(row["yield_ml"])
    leaf_grams = float(row["leaf_grams"])
    bag_grams = float(row.get("bag_grams", 600))

    merged["batches_needed"] = merged["tgy_ml_adjusted"] / yield_ml
    merged["bags_used"] = merged["batches_needed"] * leaf_grams / bag_grams

    for col in ["tgy_ml_adjusted", "batches_needed", "bags_used"]:
        merged[col] = merged[col].round(2)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged[
        [
            "month",
            "tgy_ml_base",
            "sugar_grams",
            "creamer_grams",
            "tgy_ml_adjusted",
            "batches_needed",
            "bags_used",
        ]
    ].to_csv(out_path, index=False)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
