"""Estimate ingredient usage from line-item usage and item BOM rules.

Focuses on TGY-related items defined in data/reference/item_bom.csv.
Outputs a long-form ingredient usage table and daily summary.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Tuple

import pandas as pd


def norm_key(value: str) -> str:
    s = "" if pd.isna(value) else str(value).lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estimate ingredient usage from BOM.")
    parser.add_argument(
        "--input",
        default="data/analysis/usage_line_items.csv",
        help="Usage line-items CSV path.",
    )
    parser.add_argument(
        "--item-bom",
        default="data/reference/item_bom.csv",
        help="Item BOM CSV path.",
    )
    parser.add_argument(
        "--component-units",
        default="data/reference/component_units.csv",
        help="Component units CSV path.",
    )
    parser.add_argument(
        "--sugar-map",
        default="data/reference/sugar_pct_map.csv",
        help="Sugar percentage to grams mapping CSV path.",
    )
    parser.add_argument(
        "--output",
        default="data/analysis/usage_ingredients.csv",
        help="Output ingredient usage CSV path.",
    )
    parser.add_argument(
        "--summary-output",
        default="data/analysis/usage_ingredients_summary.csv",
        help="Output ingredient usage summary CSV path.",
    )
    return parser.parse_args()


def compute_qty_unit(row, sugar_map, units_map) -> Tuple[float | None, str | None, str | None]:
    rule = str(row.get("rule") or "").strip()
    qty = row.get("qty")
    qty_unit = str(row.get("qty_unit") or "").strip()
    component = row.get("component_key")

    if rule == "tea_base":
        base_ml = row.get("tea_base_ml_est")
        if pd.isna(base_ml):
            return None, None, "missing_tea_base"
        ratio = float(qty) if pd.notna(qty) else 1.0
        return float(base_ml) * ratio, "ml", None

    if rule == "milk_base":
        milk_ml = row.get("milk_ml_est")
        if pd.isna(milk_ml):
            return None, None, "missing_milk"
        ratio = float(qty) if pd.notna(qty) else 1.0
        return float(milk_ml) * ratio, "ml", None

    if rule == "by_sugar_pct":
        sugar_pct = row.get("sugar_pct")
        if pd.isna(sugar_pct):
            return None, None, "missing_sugar_pct"
        sugar_pct = int(round(float(sugar_pct)))
        grams = sugar_map.get(sugar_pct)
        if grams is None:
            return None, None, f"unknown_sugar_pct:{sugar_pct}"
        return float(grams), "g", None

    if rule == "by_ice_pct":
        return None, None, "missing_ice_mapping"

    if rule in {"fixed", "topping_default"}:
        if pd.isna(qty):
            return None, None, "missing_qty"
        qty_val = float(qty)
        unit_info = units_map.get(component, {})
        grams_per_unit = unit_info.get("grams_per_unit")
        if qty_unit in {"shot", "unit"} and pd.notna(grams_per_unit):
            return qty_val * float(grams_per_unit), "g", None
        if qty_unit in {"g", "ml"}:
            return qty_val, qty_unit, None
        if qty_unit:
            return qty_val, qty_unit, None
        return qty_val, None, None

    return None, None, f"unknown_rule:{rule}"


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    item_bom_path = Path(args.item_bom)
    component_units_path = Path(args.component_units)
    sugar_map_path = Path(args.sugar_map)

    output_path = Path(args.output)
    summary_output_path = Path(args.summary_output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)

    usage = pd.read_csv(input_path)
    if "category_key" not in usage.columns:
        usage["category_key"] = usage.get("Category", "").map(norm_key)
    if "item_key" not in usage.columns:
        usage["item_key"] = usage.get("Item", "").map(norm_key)

    item_bom = pd.read_csv(item_bom_path)
    item_bom["category_key"] = item_bom["category_key"].astype(str).str.strip()
    item_bom["item_key"] = item_bom["item_key"].astype(str).str.strip()

    usage = usage.merge(
        item_bom,
        on=["category_key", "item_key"],
        how="inner",
    )

    component_units = pd.read_csv(component_units_path)
    units_map = {}
    for _, row in component_units.iterrows():
        units_map[str(row["component_key"]).strip()] = {
            "unit": row.get("unit"),
            "grams_per_unit": row.get("grams_per_unit"),
        }

    sugar_map_df = pd.read_csv(sugar_map_path)
    sugar_map = {
        int(row["sugar_pct"]): float(row["grams_sugar"])
        for _, row in sugar_map_df.iterrows()
        if pd.notna(row.get("sugar_pct")) and pd.notna(row.get("grams_sugar"))
    }

    qty_values = usage.apply(
        lambda row: compute_qty_unit(row, sugar_map, units_map), axis=1
    )
    usage["qty"] = [q for q, _, _ in qty_values]
    usage["unit"] = [u for _, u, _ in qty_values]
    usage["status"] = [s for _, _, s in qty_values]

    usage_out = usage[
        [
            "Date",
            "Category",
            "Item",
            "category_key",
            "item_key",
            "component_key",
            "qty",
            "unit",
            "rule",
            "line_item_id",
            "status",
        ]
    ].copy()
    usage_out = usage_out[pd.notna(usage_out["qty"])].copy()

    usage_out.to_csv(output_path, index=False)
    print(f"Wrote {output_path}")

    summary = (
        usage_out.groupby(["Date", "component_key", "unit"], as_index=False)
        .agg(
            qty_total=("qty", "sum"),
            drink_count=("line_item_id", "nunique"),
        )
        .sort_values(["Date", "component_key"])
    )

    summary.to_csv(summary_output_path, index=False)
    print(f"Wrote {summary_output_path}")


if __name__ == "__main__":
    main()
