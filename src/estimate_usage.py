"""Estimate tea usage per line item based on ice level and toppings.

Inputs:
  - data/trim/canonicalized_line_items.csv (one row per drink)
  - data/experiment/manual_samples_*pct.csv (manual tea base volumes)
  - data/reference/item_default_component.csv (default components)

Outputs:
  - line-item usage file (one row per drink)
  - component usage file (one row per drink per tea component)
  - summary file (aggregate by date + tea component)
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


MANUAL_SAMPLE_FILES = [
    "manual_samples_25pct.csv",
    "manual_samples_50pct.csv",
    "manual_samples_75pct.csv",
    "manual_samples_100pct.csv",
]

ZERO_ICE_BASE_ML = 550.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estimate tea usage from line items.")
    parser.add_argument(
        "--input",
        default="data/trim/canonicalized_line_items.csv",
        help="Line-item input CSV path.",
    )
    parser.add_argument(
        "--manual-samples-dir",
        default="data/experiment",
        help="Directory containing manual_samples_*pct.csv files.",
    )
    parser.add_argument(
        "--default-components",
        default="data/reference/item_default_component.csv",
        help="Item default component CSV path.",
    )
    parser.add_argument(
        "--output",
        default="data/analysis/usage_line_items.csv",
        help="Output CSV path for line-item usage.",
    )
    parser.add_argument(
        "--component-output",
        default="data/analysis/usage_components.csv",
        help="Output CSV path for tea-component usage.",
    )
    parser.add_argument(
        "--summary-output",
        default="data/analysis/usage_summary.csv",
        help="Output CSV path for daily component summary.",
    )
    parser.add_argument(
        "--start-date",
        default="",
        help="Inclusive start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default="",
        help="Inclusive end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--ice-fallback",
        default="nearest",
        choices=["nearest", "lower", "error"],
        help="How to handle ice_pct values without a manual sample.",
    )
    return parser.parse_args()


def round_half_up(values: pd.Series) -> pd.Series:
    """Round half up (0.5 -> 1) instead of bankers rounding."""
    arr = values.to_numpy(dtype=float)
    rounded = np.where(np.isnan(arr), np.nan, np.floor(arr + 0.5))
    return pd.Series(rounded, index=values.index)


def load_manual_means(samples_dir: Path) -> dict:
    means = {}
    for filename in MANUAL_SAMPLE_FILES:
        path = samples_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing manual sample file: {path}")
        df = pd.read_csv(path)
        if "ice_pct" not in df.columns or "tea_base_ml" not in df.columns:
            raise ValueError(f"Invalid manual sample format: {path}")
        ice_values = pd.to_numeric(df["ice_pct"], errors="coerce").dropna().unique()
        if len(ice_values) != 1:
            raise ValueError(f"Expected one ice_pct in {path}, got {ice_values}")
        ice_pct = int(ice_values[0])
        means[ice_pct] = float(pd.to_numeric(df["tea_base_ml"], errors="coerce").mean())
    return means


def build_default_component_lists(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    for col in ["category_key", "item_key", "component_key"]:
        df[col] = df[col].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(1.0)
    df = df[df["component_key"].ne("")].copy()
    df["component_pair"] = (
        df["component_key"] + ":" + df["qty"].map(lambda v: f"{v:g}")
    )
    list_df = (
        df.sort_values(["category_key", "item_key", "component_key"])
        .groupby(["category_key", "item_key"], as_index=False)["component_key"]
        .agg("|".join)
        .rename(columns={"component_key": "default_components_list"})
    )
    qty_df = (
        df.sort_values(["category_key", "item_key", "component_key"])
        .groupby(["category_key", "item_key"], as_index=False)["component_pair"]
        .agg("|".join)
        .rename(columns={"component_pair": "default_components_qty"})
    )
    return list_df.merge(qty_df, on=["category_key", "item_key"], how="outer")


def assign_ice_bucket(ice_pct: float, keys, fallback: str):
    if pd.isna(ice_pct):
        return pd.NA, True
    value = int(round(float(ice_pct)))
    if value in keys:
        return value, False
    if fallback == "error":
        return pd.NA, True
    if fallback == "lower":
        lower = [k for k in keys if k <= value]
        return (max(lower) if lower else min(keys)), True
    # nearest
    return min(keys, key=lambda k: abs(k - value)), True


def parse_components(tea_base_final: str):
    if tea_base_final is None or str(tea_base_final).strip() == "":
        return [("unknown", 1.0)]
    parts = str(tea_base_final).split("|")
    comps = []
    for part in parts:
        token = part.strip()
        if token == "":
            continue
        if ":" in token:
            name, share = token.split(":", 1)
            name = name.strip()
            share_val = float(share) if share.strip() != "" else 0.0
        else:
            name = token.strip()
            share_val = 1.0
        comps.append((name, share_val))
    total = sum(s for _, s in comps)
    if total <= 0:
        return [(name, 1.0) for name, _ in comps] if comps else [("unknown", 1.0)]
    return [(name, share / total) for name, share in comps]


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    samples_dir = Path(args.manual_samples_dir)
    default_components_path = Path(args.default_components)

    output_path = Path(args.output)
    component_output_path = Path(args.component_output)
    summary_output_path = Path(args.summary_output)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    component_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)

    manual_means = load_manual_means(samples_dir)
    ice_keys = sorted(manual_means.keys())
    print("Manual means (ml):", manual_means)

    df = pd.read_csv(input_path, low_memory=False)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    if args.start_date:
        start = pd.to_datetime(args.start_date).date()
        df = df[df["Date"] >= start]
    if args.end_date:
        end = pd.to_datetime(args.end_date).date()
        df = df[df["Date"] <= end]

    defaults = build_default_component_lists(default_components_path)
    df = df.merge(
        defaults,
        on=["category_key", "item_key"],
        how="left",
    )
    df["default_components_list"] = df["default_components_list"].fillna("")
    df["default_components_qty"] = df["default_components_qty"].fillna("")

    df["ice_pct"] = pd.to_numeric(df["ice_pct"], errors="coerce")
    ice_bucket = []
    imputed = []
    for value in df["ice_pct"]:
        if pd.notna(value) and int(round(float(value))) == 0:
            ice_bucket.append(0)
            imputed.append(False)
            continue
        bucket, flag = assign_ice_bucket(value, ice_keys, args.ice_fallback)
        ice_bucket.append(bucket)
        imputed.append(flag)
    df["ice_pct_bucket"] = ice_bucket
    df["ice_pct_imputed"] = imputed
    df["base_tea_ml"] = df["ice_pct_bucket"].map(manual_means)
    zero_mask = df["ice_pct_bucket"].eq(0)
    df.loc[zero_mask, "base_tea_ml"] = ZERO_ICE_BASE_ML

    df["topping_types_count"] = pd.to_numeric(
        df["topping_types_count"], errors="coerce"
    ).fillna(0)
    reduction_steps = df["topping_types_count"].clip(lower=0, upper=2)
    df["topping_reduction_pct"] = reduction_steps * 0.1
    df["tea_base_ml_raw"] = df["base_tea_ml"] * (1 - df["topping_reduction_pct"])
    df["tea_base_ml_est"] = round_half_up(df["tea_base_ml_raw"]).astype("Int64")

    if "line_group_id" not in df.columns:
        df["line_group_id"] = df.index
    if "line_item_index" not in df.columns:
        df["line_item_index"] = df.groupby("line_group_id").cumcount() + 1
    df["line_item_id"] = df["line_group_id"].astype(str) + "-" + df["line_item_index"].astype(str)

    line_cols = [
        "Date",
        "Time",
        "Transaction ID",
        "Category",
        "Item",
        "ice_pct",
        "sugar_pct",
        "tea_base_final",
        "tea_resolution",
        "toppings_list",
        "toppings_qty",
        "topping_types_count",
        "default_components_list",
        "default_components_qty",
        "ice_pct_bucket",
        "ice_pct_imputed",
        "base_tea_ml",
        "topping_reduction_pct",
        "tea_base_ml_est",
        "line_group_id",
        "line_item_index",
        "line_item_id",
    ]
    line_cols = [c for c in line_cols if c in df.columns]
    df[line_cols].to_csv(output_path, index=False)
    print(f"wrote {output_path}")

    component_rows = []
    for _, row in df.iterrows():
        components = parse_components(row.get("tea_base_final", ""))
        base_ml = row.get("tea_base_ml_est")
        for name, share in components:
            ml = None if pd.isna(base_ml) else float(base_ml) * share
            component_rows.append(
                {
                    "Date": row.get("Date"),
                    "Transaction ID": row.get("Transaction ID"),
                    "Item": row.get("Item"),
                    "ice_pct": row.get("ice_pct"),
                    "sugar_pct": row.get("sugar_pct"),
                    "tea_component": name,
                    "tea_component_share": share,
                    "tea_component_ml_est": ml,
                    "line_item_id": row.get("line_item_id"),
                }
            )
    components_df = pd.DataFrame(component_rows)
    components_df.to_csv(component_output_path, index=False)
    print(f"wrote {component_output_path}")

    summary = (
        components_df.groupby(["Date", "tea_component"], as_index=False)
        .agg(
            drink_count=("line_item_id", "nunique"),
            tea_ml_total=("tea_component_ml_est", "sum"),
        )
        .sort_values(["Date", "tea_component"])
    )
    summary.to_csv(summary_output_path, index=False)
    print(f"wrote {summary_output_path}")


if __name__ == "__main__":
    main()
