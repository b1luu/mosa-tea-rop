"""Audit Tie Guan Yin usage and batch needs."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit Tie Guan Yin usage.")
    parser.add_argument(
        "--components",
        default="data/analysis/usage_components.csv",
        help="Usage components CSV path.",
    )
    parser.add_argument(
        "--line-items",
        default="data/analysis/usage_line_items.csv",
        help="Usage line items CSV path.",
    )
    parser.add_argument(
        "--batch-estimates",
        default="data/analysis/batch_yield_estimates.csv",
        help="Batch yield estimates CSV path.",
    )
    parser.add_argument(
        "--output",
        default="data/analysis/tgy_usage_audit.csv",
        help="Output CSV path for daily audit.",
    )
    parser.add_argument(
        "--item-output",
        default="data/analysis/tgy_item_breakdown.csv",
        help="Output CSV path for item breakdown.",
    )
    parser.add_argument(
        "--monthly-output",
        default="data/analysis/tgy_monthly_bag_usage.csv",
        help="Output CSV path for monthly bag usage (full months only).",
    )
    parser.add_argument(
        "--bag-grams",
        type=float,
        default=600,
        help="Leaf grams per vendor bag (default: 600).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    components_path = Path(args.components)
    line_items_path = Path(args.line_items)
    batch_path = Path(args.batch_estimates)

    output_path = Path(args.output)
    item_output_path = Path(args.item_output)
    monthly_output_path = Path(args.monthly_output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    item_output_path.parent.mkdir(parents=True, exist_ok=True)
    monthly_output_path.parent.mkdir(parents=True, exist_ok=True)

    components = pd.read_csv(components_path)
    components["Date"] = pd.to_datetime(components["Date"], errors="coerce").dt.date
    tgy_components = components[
        components["tea_component"].astype(str).str.strip().eq("tie_guan_yin")
    ].copy()

    line_items = pd.read_csv(line_items_path)
    line_items["Date"] = pd.to_datetime(line_items["Date"], errors="coerce").dt.date

    tgy_line_ids = tgy_components["line_item_id"].dropna().unique().tolist()
    tgy_lines = line_items[line_items["line_item_id"].isin(tgy_line_ids)].copy()

    # Daily totals + batch needs.
    daily = (
        tgy_components.groupby("Date", as_index=False)
        .agg(
            tgy_ml_total=("tea_component_ml_est", "sum"),
            tgy_drink_count=("line_item_id", "nunique"),
        )
        .sort_values("Date")
    )

    # Pull batch yield for tie_guan_yin.
    batch = pd.read_csv(batch_path)
    tgy_batch = batch[batch["tea_key"].astype(str).str.strip().eq("tie_guan_yin")]
    if tgy_batch.empty:
        raise ValueError("tie_guan_yin batch yield not found in batch_yield_estimates.csv")
    tgy_yield_ml = float(tgy_batch.iloc[0]["yield_ml"])
    leaf_grams_per_batch = float(tgy_batch.iloc[0].get("leaf_grams", 0))
    bag_grams = float(tgy_batch.iloc[0].get("bag_grams", args.bag_grams))

    daily["batch_yield_ml"] = tgy_yield_ml
    daily["batches_needed"] = daily["tgy_ml_total"] / tgy_yield_ml

    # Resolution mix for TGY-linked drinks.
    resolution_mix = (
        tgy_lines.groupby("tea_resolution", as_index=False)
        .agg(drink_count=("line_item_id", "nunique"))
        .sort_values("drink_count", ascending=False)
    )

    # Top items for TGY usage.
    item_breakdown = (
        tgy_lines.groupby("Item", as_index=False)
        .agg(
            drink_count=("line_item_id", "nunique"),
        )
        .sort_values("drink_count", ascending=False)
    )

    # Join per-item total ml from components.
    item_ml = (
        tgy_components.groupby("Item", as_index=False)["tea_component_ml_est"]
        .sum()
        .rename(columns={"tea_component_ml_est": "tgy_ml_total"})
        .sort_values("tgy_ml_total", ascending=False)
    )
    item_breakdown = item_breakdown.merge(item_ml, on="Item", how="left")

    # Monthly bag usage (full months only).
    all_dates = line_items["Date"].dropna().unique()
    date_df = pd.DataFrame({"Date": pd.to_datetime(all_dates)})
    date_df["month"] = date_df["Date"].dt.to_period("M")
    coverage = (
        date_df.groupby("month", as_index=False)
        .agg(days_covered=("Date", "nunique"))
    )
    coverage["days_in_month"] = coverage["month"].dt.days_in_month
    coverage["full_month"] = coverage["days_covered"] == coverage["days_in_month"]

    tgy_monthly = tgy_components.copy()
    tgy_monthly["month"] = pd.to_datetime(tgy_monthly["Date"], errors="coerce").dt.to_period("M")
    tgy_monthly = (
        tgy_monthly.groupby("month", as_index=False)["tea_component_ml_est"]
        .sum()
        .rename(columns={"tea_component_ml_est": "tgy_ml_total"})
    )

    monthly = coverage.merge(tgy_monthly, on="month", how="left").fillna(
        {"tgy_ml_total": 0}
    )
    monthly = monthly[monthly["full_month"]].copy()
    monthly["batch_yield_ml"] = tgy_yield_ml
    monthly["leaf_grams_per_batch"] = leaf_grams_per_batch
    monthly["bag_grams"] = bag_grams
    monthly["batches_needed"] = monthly["tgy_ml_total"] / tgy_yield_ml
    monthly["leaf_grams_used"] = monthly["batches_needed"] * leaf_grams_per_batch
    monthly["bags_used"] = (
        monthly["leaf_grams_used"] / bag_grams if bag_grams else 0
    )

    monthly = monthly[
        [
            "month",
            "days_covered",
            "days_in_month",
            "tgy_ml_total",
            "batch_yield_ml",
            "leaf_grams_per_batch",
            "bag_grams",
            "batches_needed",
            "leaf_grams_used",
            "bags_used",
        ]
    ].copy()
    monthly["month"] = monthly["month"].astype(str)
    numeric_cols = monthly.select_dtypes(include="number").columns
    monthly[numeric_cols] = monthly[numeric_cols].round(2)

    # Output daily audit and item breakdown.
    daily.to_csv(output_path, index=False)
    item_breakdown.to_csv(item_output_path, index=False)
    monthly.to_csv(monthly_output_path, index=False)

    # Write resolution mix as a footer-like appendix file.
    resolution_path = output_path.with_name("tgy_resolution_mix.csv")
    resolution_mix.to_csv(resolution_path, index=False)

    print(f"Wrote {output_path}")
    print(f"Wrote {item_output_path}")
    print(f"Wrote {monthly_output_path}")
    print(f"Wrote {resolution_path}")


if __name__ == "__main__":
    main()
