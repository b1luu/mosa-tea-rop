"""Batch yield model for brewed teas (no-squeeze absorption)."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Tuple

ABSORB_ML_PER_G_NO_SQUEEZE: Dict[str, float] = {
    "four_seasons": 3.2,
    "green_tea": 3.0,
    "tie_guan_yin": 3.1,
    "matured_black": 2.7,
    "buckwheat": 2.4,
    "barley": 2.8,
    "genmai": 2.8,
}

DEFAULT_LEAF_GRAMS: Dict[str, float] = {
    "four_seasons": 160,
    "green_tea": 160,
    "tie_guan_yin": 160,
    "matured_black": 140,
    "buckwheat": 120,
    "barley": 240,
    "genmai": 120,
}

DEFAULT_BATCH_WATER_ICE: Dict[str, Dict[str, float]] = {
    # Standard cold-brew style: 4200 hot water + 2800 ice
    "tie_guan_yin": {"hot_water_ml": 4200, "ice_grams": 2800},
    "four_seasons": {"hot_water_ml": 4200, "ice_grams": 2800},
    "green_tea": {"hot_water_ml": 4200, "ice_grams": 2800},
    # Long brew: 6000 hot water, no ice
    "matured_black": {"hot_water_ml": 6000, "ice_grams": 0},
    "buckwheat": {"hot_water_ml": 6000, "ice_grams": 0},
    "genmai": {"hot_water_ml": 6000, "ice_grams": 0},
}


def resolve_batch_inputs(
    tea_key: str,
    hot_water_ml: float | None,
    ice_grams: float | None,
) -> Tuple[float, float]:
    batch_defaults = DEFAULT_BATCH_WATER_ICE.get(
        tea_key, {"hot_water_ml": 6000, "ice_grams": 0}
    )
    resolved_hot_water_ml = (
        batch_defaults["hot_water_ml"] if hot_water_ml is None else hot_water_ml
    )
    resolved_ice_grams = batch_defaults["ice_grams"] if ice_grams is None else ice_grams
    return resolved_hot_water_ml, resolved_ice_grams


def estimate_batch_yield_ml(
    tea_key: str,
    leaf_grams: float | None = None,
    *,
    hot_water_ml: float | None = None,
    ice_grams: float | None = None,
    process_loss_ml: float = 0,
) -> Tuple[float, float, float]:
    """Estimate batch yield in mL for a tea key.

    Formula:
        yield_ml = hot_water_ml + ice_ml - (leaf_grams * absorb_ml_per_g) - process_loss_ml
    """
    if tea_key not in ABSORB_ML_PER_G_NO_SQUEEZE:
        raise KeyError(f"Unknown tea_key: {tea_key}")

    if leaf_grams is None:
        if tea_key not in DEFAULT_LEAF_GRAMS:
            raise KeyError(f"Missing default leaf grams for tea_key: {tea_key}")
        leaf_grams = DEFAULT_LEAF_GRAMS[tea_key]

    hot_water_ml, ice_grams = resolve_batch_inputs(
        tea_key=tea_key,
        hot_water_ml=hot_water_ml,
        ice_grams=ice_grams,
    )

    for name, value in {
        "leaf_grams": leaf_grams,
        "hot_water_ml": hot_water_ml,
        "ice_grams": ice_grams,
        "process_loss_ml": process_loss_ml,
    }.items():
        if value < 0:
            raise ValueError(f"{name} must be non-negative, got {value}")

    absorb_ml_per_g = ABSORB_ML_PER_G_NO_SQUEEZE[tea_key]
    absorbed_ml = leaf_grams * absorb_ml_per_g
    yield_ml = hot_water_ml + ice_grams - absorbed_ml - process_loss_ml
    return yield_ml, absorbed_ml, absorb_ml_per_g


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estimate tea batch yield.")
    parser.add_argument(
        "--tea-key",
        default="",
        help="Tea key to estimate (default: all keys).",
    )
    parser.add_argument(
        "--leaf-grams",
        type=float,
        default=None,
        help="Override leaf grams (only used with --tea-key).",
    )
    parser.add_argument(
        "--hot-water-ml",
        type=float,
        default=None,
        help="Hot water volume in mL (default: per-tea batch config).",
    )
    parser.add_argument(
        "--ice-grams",
        type=float,
        default=None,
        help="Ice in grams (assume 1 g = 1 mL water; default: per-tea batch config).",
    )
    parser.add_argument(
        "--process-loss-ml",
        type=float,
        default=0,
        help="Process loss in mL (default: 0).",
    )
    parser.add_argument(
        "--output",
        default="data/analysis/batch_yield_estimates.csv",
        help="Output CSV path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.tea_key:
        tea_keys = [args.tea_key]
    else:
        tea_keys = sorted(ABSORB_ML_PER_G_NO_SQUEEZE.keys())

    rows = []
    for tea_key in tea_keys:
        leaf_grams = args.leaf_grams
        if leaf_grams is None:
            leaf_grams = DEFAULT_LEAF_GRAMS.get(tea_key)
        if leaf_grams is None:
            raise KeyError(f"Missing default leaf grams for tea_key: {tea_key}")
        resolved_hot_water_ml, resolved_ice_grams = resolve_batch_inputs(
            tea_key=tea_key,
            hot_water_ml=args.hot_water_ml,
            ice_grams=args.ice_grams,
        )
        yield_ml, absorbed_ml, absorb_ml_per_g = estimate_batch_yield_ml(
            tea_key=tea_key,
            leaf_grams=leaf_grams,
            hot_water_ml=resolved_hot_water_ml,
            ice_grams=resolved_ice_grams,
            process_loss_ml=args.process_loss_ml,
        )
        rows.append(
            {
                "tea_key": tea_key,
                "leaf_grams": leaf_grams,
                "hot_water_ml": resolved_hot_water_ml,
                "ice_grams": resolved_ice_grams,
                "process_loss_ml": args.process_loss_ml,
                "absorb_ml_per_g": absorb_ml_per_g,
                "absorbed_ml": absorbed_ml,
                "yield_ml": yield_ml,
            }
        )

    import csv

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "tea_key",
                "leaf_grams",
                "hot_water_ml",
                "ice_grams",
                "process_loss_ml",
                "absorb_ml_per_g",
                "absorbed_ml",
                "yield_ml",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
