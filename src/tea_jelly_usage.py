"""Summarize tea jelly usage from line-item CSVs."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd

DEFAULT_TOPPING_KEYS = {"tea_jelly", "tgy_jelly", "osmanthus_tgy_jelly"}


def parse_topping_qty(value: str) -> Dict[str, float]:
    if value is None or str(value).strip() == "":
        return {}
    items: Dict[str, float] = {}
    for part in str(value).split("|"):
        token = part.strip()
        if token == "":
            continue
        if ":" in token:
            name, qty = token.split(":", 1)
        else:
            name, qty = token, "1"
        name = name.strip()
        try:
            qty_val = float(qty)
        except (TypeError, ValueError):
            qty_val = 1.0
        if name == "":
            continue
        items[name] = items.get(name, 0.0) + qty_val
    return items


def extract_topping_units(
    toppings_qty: str, toppings_list: str, target_keys: Iterable[str]
) -> float:
    qtys = parse_topping_qty(toppings_qty)
    if not qtys:
        for name in str(toppings_list).split("|"):
            token = name.strip()
            if token:
                qtys[token] = qtys.get(token, 0.0) + 1.0
    return float(sum(qtys.get(key, 0.0) for key in target_keys))


def summarize_tea_jelly_usage(
    line_items_path: Path,
    *,
    ml_per_scoop: float = 87.0,
    topping_keys: Iterable[str] = DEFAULT_TOPPING_KEYS,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(line_items_path)
    if "toppings_qty" not in df.columns and "toppings_list" not in df.columns:
        raise ValueError("Expected toppings_qty or toppings_list columns in input.")

    df["tea_jelly_units"] = df.apply(
        lambda row: extract_topping_units(
            row.get("toppings_qty", ""),
            row.get("toppings_list", ""),
            topping_keys,
        ),
        axis=1,
    )
    df["tea_jelly_ml"] = df["tea_jelly_units"] * ml_per_scoop

    total_items = len(df)
    jelly_items = int((df["tea_jelly_units"] > 0).sum())
    total_scoops = float(df["tea_jelly_units"].sum())
    total_ml = float(df["tea_jelly_ml"].sum())

    summary = pd.DataFrame(
        [
            {
                "line_items": total_items,
                "drinks_with_tea_jelly": jelly_items,
                "total_tea_jelly_scoops": total_scoops,
                "avg_scoops_per_drink": total_scoops / total_items if total_items else 0.0,
                "avg_scoops_per_jelly_drink": (
                    total_scoops / jelly_items if jelly_items else 0.0
                ),
                "ml_per_scoop": ml_per_scoop,
                "total_tgy_ml_from_jelly": total_ml,
                "avg_tgy_ml_from_jelly_per_drink": (
                    total_ml / total_items if total_items else 0.0
                ),
            }
        ]
    )

    return summary, df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize tea jelly usage from line-item CSVs."
    )
    parser.add_argument(
        "--input",
        default="data/analysis/usage_line_items.csv",
        help="Line-item CSV path (usage_line_items or canonicalized_line_items).",
    )
    parser.add_argument(
        "--output",
        default="data/analysis/tea_jelly_usage_summary.csv",
        help="Output CSV path for summary.",
    )
    parser.add_argument(
        "--line-item-output",
        default="",
        help="Optional output for annotated line items with tea_jelly_units.",
    )
    parser.add_argument(
        "--ml-per-scoop",
        type=float,
        default=87.0,
        help="Tea ml per scoop (default: 87).",
    )
    parser.add_argument(
        "--topping-keys",
        default=",".join(sorted(DEFAULT_TOPPING_KEYS)),
        help="Comma-separated topping keys to count as tea jelly.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    topping_keys = [k.strip() for k in args.topping_keys.split(",") if k.strip()]

    summary, annotated = summarize_tea_jelly_usage(
        input_path,
        ml_per_scoop=args.ml_per_scoop,
        topping_keys=topping_keys,
    )

    summary.to_csv(output_path, index=False)
    print(f"Wrote {output_path}")

    if args.line_item_output:
        line_path = Path(args.line_item_output)
        line_path.parent.mkdir(parents=True, exist_ok=True)
        annotated.to_csv(line_path, index=False)
        print(f"Wrote {line_path}")


if __name__ == "__main__":
    main()
