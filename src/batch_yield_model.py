"""Batch yield model for brewed teas (no-squeeze absorption)."""

from __future__ import annotations

from typing import Dict, Tuple

ABSORB_ML_PER_G_NO_SQUEEZE: Dict[str, float] = {
    "four_seasons": 3.2,
    "green_tea": 3.0,
    "tie_guan_yin": 3.8,
    "matured_black": 2.7,
    "buckwheat": 2.4,
    "barley": 2.8,
    "toasted_rice": 2.8,
}

DEFAULT_LEAF_GRAMS: Dict[str, float] = {
    "four_seasons": 160,
    "green_tea": 160,
    "tie_guan_yin": 160,
    "matured_black": 140,
    "buckwheat": 120,
    "barley": 240,
    "toasted_rice": 120,
}


def estimate_batch_yield_ml(
    tea_key: str,
    leaf_grams: float | None = None,
    *,
    hot_water_ml: float = 4200,
    ice_grams: float = 2800,
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

