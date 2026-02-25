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
        "--recipe-simple",
        default="data/reference/recipe_simple.csv",
        help="Simple recipe CSV for per-item overrides.",
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
        "--weekday-output",
        default="data/analysis/usage_weekday_summary.csv",
        help="Output CSV path for weekday averages.",
    )
    parser.add_argument(
        "--monthly-weekday-output",
        default="data/analysis/usage_monthly_weekday_summary.csv",
        help="Output CSV path for month + weekday averages.",
    )
    parser.add_argument(
        "--validation-output",
        default="data/analysis/usage_validation.csv",
        help="Output CSV path for validation summary.",
    )
    parser.add_argument(
        "--tea-component-filter",
        default="",
        help="Optional tea component filter for monthly weekday averages.",
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


def load_recipe_overrides(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(
            columns=["item_name", "tea_base_ml", "milk_ml", "ice", "match_tokens"]
        )
    df = pd.read_csv(path)
    df["item_name"] = df["item_name"].astype(str).str.strip()
    df["tea_base_ml"] = pd.to_numeric(df.get("tea_base_ml"), errors="coerce")
    df["milk_ml"] = pd.to_numeric(df.get("milk_ml"), errors="coerce")
    df["ice"] = df.get("ice", "").astype(str).str.strip().str.lower()
    df["match_tokens"] = df.get("match_tokens", "").fillna("").astype(str).str.strip()
    df = df[df["item_name"].ne("")].copy()
    df["match_priority"] = df["match_tokens"].where(
        df["match_tokens"].ne(""), df["item_name"]
    ).str.len()
    return df.sort_values("match_priority", ascending=False)[
        ["item_name", "tea_base_ml", "milk_ml", "ice", "match_tokens"]
    ]


def build_default_component_lists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "category_key",
                "item_key",
                "default_components_list",
                "default_components_qty",
            ]
        )
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
    recipe_simple_path = Path(args.recipe_simple)

    output_path = Path(args.output)
    component_output_path = Path(args.component_output)
    summary_output_path = Path(args.summary_output)
    weekday_output_path = Path(args.weekday_output)
    monthly_weekday_output_path = Path(args.monthly_weekday_output)
    validation_output_path = Path(args.validation_output)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    component_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    weekday_output_path.parent.mkdir(parents=True, exist_ok=True)
    monthly_weekday_output_path.parent.mkdir(parents=True, exist_ok=True)
    validation_output_path.parent.mkdir(parents=True, exist_ok=True)

    manual_means = load_manual_means(samples_dir)
    ice_keys = sorted(manual_means.keys())
    print("Manual means (ml):", manual_means)

    recipe_overrides = load_recipe_overrides(recipe_simple_path)

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

    # Apply recipe overrides based on item-name substring matches.
    df["recipe_item_match"] = ""
    df["recipe_tea_base_ml"] = np.nan
    df["recipe_milk_ml"] = np.nan
    df["recipe_ice"] = ""
    if not recipe_overrides.empty:
        overrides = list(recipe_overrides.itertuples(index=False))
        for idx, item in df["Item"].astype(str).items():
            item_lower = item.lower()
            matched = None
            for row in overrides:
                tokens = str(row.match_tokens or "").lower().strip()
                if tokens:
                    required = [t.strip() for t in tokens.split("|") if t.strip()]
                    if required and not all(t in item_lower for t in required):
                        continue
                else:
                    name = str(row.item_name).lower()
                    if not name or name not in item_lower:
                        continue
                matched = row
                break
            if matched is None:
                continue
            df.at[idx, "recipe_item_match"] = matched.item_name
            if pd.notna(matched.tea_base_ml):
                df.at[idx, "recipe_tea_base_ml"] = matched.tea_base_ml
            if pd.notna(matched.milk_ml):
                df.at[idx, "recipe_milk_ml"] = matched.milk_ml
            if matched.ice:
                df.at[idx, "recipe_ice"] = matched.ice

    df["recipe_tea_base_ml"] = pd.to_numeric(
        df["recipe_tea_base_ml"], errors="coerce"
    )
    df["recipe_milk_ml"] = pd.to_numeric(
        df["recipe_milk_ml"], errors="coerce"
    )

    # Force ice bucket when recipe explicitly specifies a fixed ice level.
    force_ice_100 = df["recipe_ice"].str.contains("100%", case=False, na=False)
    if force_ice_100.any():
        df.loc[force_ice_100, "ice_pct_bucket"] = 100
        df.loc[force_ice_100, "ice_pct_imputed"] = True
        if 100 in manual_means:
            df.loc[force_ice_100, "base_tea_ml"] = manual_means[100]

    force_no_ice = df["recipe_ice"].str.contains("no ice", case=False, na=False)
    if force_no_ice.any():
        df.loc[force_no_ice, "ice_pct_bucket"] = 0
        df.loc[force_no_ice, "ice_pct_imputed"] = True
        df.loc[force_no_ice, "base_tea_ml"] = ZERO_ICE_BASE_ML

    df["base_total_ml"] = df["base_tea_ml"]
    df["milk_ml_est"] = 0.0

    has_milk = df["recipe_milk_ml"].notna() & df["recipe_milk_ml"].gt(0)
    tea_specified = df["recipe_tea_base_ml"].notna() & df["recipe_tea_base_ml"].gt(0)
    dynamic_ice = df["recipe_ice"].str.contains("no ice", case=False, na=False).eq(False)
    ratio_mask = has_milk & tea_specified & dynamic_ice

    if ratio_mask.any():
        ratio_total = df.loc[ratio_mask, "recipe_tea_base_ml"] + df.loc[
            ratio_mask, "recipe_milk_ml"
        ]
        tea_ratio = df.loc[ratio_mask, "recipe_tea_base_ml"] / ratio_total
        milk_ratio = df.loc[ratio_mask, "recipe_milk_ml"] / ratio_total
        df.loc[ratio_mask, "base_total_ml"] = df.loc[ratio_mask, "base_tea_ml"]
        df.loc[ratio_mask, "base_tea_ml"] = (
            df.loc[ratio_mask, "base_total_ml"] * tea_ratio
        )
        df.loc[ratio_mask, "milk_ml_est"] = (
            df.loc[ratio_mask, "base_total_ml"] * milk_ratio
        )

    fixed_mask = has_milk & tea_specified & ~ratio_mask
    if fixed_mask.any():
        df.loc[fixed_mask, "base_tea_ml"] = df.loc[fixed_mask, "recipe_tea_base_ml"]
        df.loc[fixed_mask, "milk_ml_est"] = df.loc[fixed_mask, "recipe_milk_ml"]
        df.loc[fixed_mask, "base_total_ml"] = (
            df.loc[fixed_mask, "recipe_tea_base_ml"]
            + df.loc[fixed_mask, "recipe_milk_ml"]
        )

    tea_only_mask = df["recipe_tea_base_ml"].notna() & ~has_milk
    if tea_only_mask.any():
        df.loc[tea_only_mask, "base_tea_ml"] = df.loc[tea_only_mask, "recipe_tea_base_ml"]
        df.loc[tea_only_mask, "base_total_ml"] = df.loc[tea_only_mask, "base_tea_ml"]

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
        "recipe_item_match",
        "recipe_ice",
        "milk_ml_est",
        "base_total_ml",
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

    daily_totals = (
        components_df.groupby(["Date", "tea_component"], as_index=False)
        .agg(
            drink_count=("line_item_id", "nunique"),
            tea_ml_total=("tea_component_ml_est", "sum"),
        )
        .sort_values(["Date", "tea_component"])
    )
    daily_totals["weekday"] = pd.to_datetime(daily_totals["Date"]).dt.day_name()
    weekday_summary = (
        daily_totals.groupby(["weekday", "tea_component"], as_index=False)
        .agg(
            avg_tea_ml_total=("tea_ml_total", "mean"),
            avg_drink_count=("drink_count", "mean"),
            days_count=("Date", "nunique"),
        )
    )
    weekday_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    weekday_summary["weekday"] = pd.Categorical(
        weekday_summary["weekday"], categories=weekday_order, ordered=True
    )
    weekday_summary = weekday_summary.sort_values(["weekday", "tea_component"])
    weekday_summary.to_csv(weekday_output_path, index=False)
    print(f"wrote {weekday_output_path}")

    component_filter = args.tea_component_filter.strip()
    filtered = components_df.copy()
    if component_filter:
        filtered = filtered[
            filtered["tea_component"].astype(str).str.strip().eq(component_filter)
        ].copy()
        if filtered.empty:
            print(f"WARNING: no rows found for tea_component '{component_filter}'.")
    filtered["Date"] = pd.to_datetime(filtered["Date"], errors="coerce")
    filtered = filtered.dropna(subset=["Date"])
    filtered["month"] = filtered["Date"].dt.to_period("M").astype(str)
    filtered["weekday"] = filtered["Date"].dt.day_name()
    daily_component = (
        filtered.groupby(["Date", "month", "weekday", "tea_component"], as_index=False)
        .agg(
            tea_ml_total=("tea_component_ml_est", "sum"),
            drink_count=("line_item_id", "nunique"),
        )
    )
    monthly_weekday = (
        daily_component.groupby(["month", "weekday", "tea_component"], as_index=False)
        .agg(
            avg_tea_ml_total=("tea_ml_total", "mean"),
            avg_drink_count=("drink_count", "mean"),
            days_count=("Date", "nunique"),
        )
    )
    weekday_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    monthly_weekday["weekday"] = pd.Categorical(
        monthly_weekday["weekday"], categories=weekday_order, ordered=True
    )
    monthly_weekday = monthly_weekday.sort_values(
        ["month", "weekday", "tea_component"]
    )
    monthly_weekday.to_csv(monthly_weekday_output_path, index=False)
    print(f"wrote {monthly_weekday_output_path}")

    validation = [
        {"metric": "line_items", "value": int(len(df))},
        {
            "metric": "unique_line_item_ids",
            "value": int(df["line_item_id"].nunique()),
        },
        {"metric": "components_rows", "value": int(len(components_df))},
        {"metric": "missing_base_tea_ml", "value": int(df["base_tea_ml"].isna().sum())},
        {
            "metric": "missing_tea_base_ml_est",
            "value": int(df["tea_base_ml_est"].isna().sum()),
        },
        {
            "metric": "recipe_overrides",
            "value": int(df["recipe_item_match"].astype(str).str.strip().ne("").sum()),
        },
        {"metric": "milk_drinks", "value": int((df["milk_ml_est"] > 0).sum())},
        {
            "metric": "forced_ice_100",
            "value": int(df["recipe_ice"].str.contains("100%", case=False, na=False).sum()),
        },
        {
            "metric": "forced_no_ice",
            "value": int(df["recipe_ice"].str.contains("no ice", case=False, na=False).sum()),
        },
        {
            "metric": "topping_reduction_applied",
            "value": int((df["topping_reduction_pct"] > 0).sum()),
        },
    ]
    pd.DataFrame(validation).to_csv(validation_output_path, index=False)
    print(f"wrote {validation_output_path}")


if __name__ == "__main__":
    main()
