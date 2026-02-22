import argparse
from pathlib import Path
import re

import pandas as pd

NUMERIC_SETTING_RE = r"(?i)^(?:no\s*ice|no\s*sugar|\d{1,3}\s*%\s*ice|\d{1,3}\s*%\s*sugar)$"


def norm_key(v):
    """Normalize free text into a stable snake_case join key."""
    s = "" if pd.isna(v) else str(v).lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def join_unique(values):
    """Return sorted unique non-empty values joined by | for deterministic outputs."""
    vals = sorted({str(v).strip() for v in values if pd.notna(v) and str(v).strip() != ""})
    return "|".join(vals)


def format_qty(v):
    """Format numeric qty without trailing .0 when value is whole."""
    return f"{v:g}" if pd.notna(v) else ""


def parse_args():
    parser = argparse.ArgumentParser(description="Canonicalize cleaned sales data.")
    parser.add_argument("--input", default="data/trim/clean.csv", help="Input clean CSV path.")
    parser.add_argument(
        "--token-map",
        default="data/reference/modifier_token_map.csv",
        help="Modifier token map CSV path.",
    )
    parser.add_argument(
        "--item-rules",
        default="data/reference/item_rules.csv",
        help="Item rules CSV path.",
    )
    parser.add_argument(
        "--blend-rules",
        default="data/reference/item_blend_rules.csv",
        help="Blend rules CSV path.",
    )
    parser.add_argument(
        "--default-components",
        default="data/reference/item_default_component.csv",
        help="Default component CSV path (reserved for downstream usage).",
    )
    parser.add_argument(
        "--output",
        default="data/trim/canonicalized.csv",
        help="Slim canonicalized output path.",
    )
    parser.add_argument(
        "--debug-output",
        default="data/trim/canonicalized_debug.csv",
        help="Debug canonicalized output path.",
    )
    parser.add_argument(
        "--unknown-output",
        default="",
        help=(
            "Optional unknown modifier token report path. "
            "Default: sibling file named unknown_modifier_tokens.csv next to --output."
        ),
    )
    return parser.parse_args()

def run_canonicalization(
    clean_path,
    token_map_path,
    item_rules_path,
    blend_rules_path,
    default_components_path,
):
    """Canonicalize clean sales rows into analysis-ready fields.

    Pipeline:
    1) Build stable row/category/item keys.
    2) Explode modifiers and map tokens.
    3) Resolve tea base with precedence.
    4) Build topping features from both modifiers and default item components.
    """
    clean = pd.read_csv(clean_path, low_memory=False)
    token_map = pd.read_csv(token_map_path)
    item_rules = pd.read_csv(item_rules_path)
    blend_rules = pd.read_csv(blend_rules_path)
    default_comp = pd.read_csv(default_components_path)

    # Stable row key lets us explode/aggregate and merge back without ambiguity.
    clean["row_id"] = range(len(clean))
    clean["category_key"] = clean["Category"].map(norm_key)
    clean["item_key"] = clean["Item"].map(norm_key)

    # Normalize default component keys so they can join cleanly with item keys.
    default_comp["category_key"] = default_comp["category_key"].map(norm_key)
    default_comp["item_key"] = default_comp["item_key"].map(norm_key)
    default_comp["component_key"] = default_comp["component_key"].map(norm_key)
    default_comp["qty"] = pd.to_numeric(default_comp["qty"], errors="coerce").fillna(1.0)
    default_comp = default_comp[default_comp["component_key"].astype(str).str.strip().ne("")].copy()

    # Treat only topping-like defaults as toppings; osmanthus syrup is flavoring, not topping.
    topping_component_mask = default_comp["component_key"].str.contains(
        r"(?i)boba|jelly|foam|pudding|hun_kue|kue",
        regex=True,
    )
    default_toppings = default_comp[
        topping_component_mask & default_comp["component_key"].ne("osmanthus_syrup_shot")
    ].copy()

    tokens = (
        clean[["row_id", "Modifiers Applied"]]
        .assign(token=lambda d: d["Modifiers Applied"].fillna("").astype(str).str.split(","))
        .explode("token")
    )
    tokens["token"] = tokens["token"].fillna("").str.strip()
    tokens = tokens[tokens["token"] != ""].copy()

    # Parse quantity suffixes like "Boba x2", "Boba x 2", "Boba × 2.0".
    mult_re = r"^(?P<name>.+?)\s*[×x]\s*(?P<qty>\d+(?:\.\d+)?)\s*$"
    mult = tokens["token"].str.extract(mult_re, flags=re.IGNORECASE)
    tokens["token_name"] = mult["name"].fillna(tokens["token"]).str.strip()
    tokens["token_qty"] = pd.to_numeric(mult["qty"], errors="coerce").fillna(1.0)
    tokens["token_norm"] = tokens["token_name"].str.lower()

    # normalize token map keys for matching
    token_map = token_map.dropna(subset=["raw_token"]).copy()
    token_map["raw_token_norm"] = token_map["raw_token"].astype(str).str.strip().str.lower()

    mapped = tokens.merge(
        token_map[["raw_token_norm", "token_type", "canonical_value"]],
        left_on="token_norm",
        right_on="raw_token_norm",
        how="left",
    )

    # Harmonize tea override values to match item rules / blend component keys.
    tea_value_map = {
        "green_tea": "green",
        "four_seasons_tea": "four_seasons",
        "green_tea_genmai": "genmai:0.5|green:0.5",
    }
    mapped["tea_value_norm"] = mapped["canonical_value"].apply(
        lambda v: tea_value_map.get(v, v)
    )

    # Unknown tokens are unmapped modifier names excluding numeric ice/sugar settings.
    unknown_modifier_rows = mapped[mapped["token_type"].isna()].copy()
    unknown_modifier_rows = unknown_modifier_rows[
        ~unknown_modifier_rows["token_name"].str.match(NUMERIC_SETTING_RE, na=False)
    ].copy()
    unknown_modifier_summary = (
        unknown_modifier_rows.groupby("token_name", as_index=False)
        .agg(
            count=("token_name", "size"),
            rows_affected=("row_id", "nunique"),
        )
        .sort_values(["count", "token_name"], ascending=[False, True])
    )
    if unknown_modifier_summary.empty:
        unknown_modifier_summary = pd.DataFrame(
            columns=["token_name", "count", "rows_affected"]
        )

    tea_choices = (
        mapped[mapped["token_type"].eq("tea_base") & mapped["tea_value_norm"].notna()]
        .groupby("row_id")["tea_value_norm"]
        .agg(join_unique)
        .rename("tea_override_choices")
        .reset_index()
    )
    tea_choice_count = (
        mapped[mapped["token_type"].eq("tea_base") & mapped["tea_value_norm"].notna()]
        .groupby("row_id")["tea_value_norm"]
        .nunique()
        .rename("tea_choice_count")
        .reset_index()
    )
    tea_choices = tea_choices.merge(tea_choice_count, on="row_id", how="left")
    tea_choices["tea_base_override"] = tea_choices["tea_override_choices"].where(
        tea_choices["tea_choice_count"].eq(1), pd.NA
    )
    tea_choices["tea_override_conflict"] = tea_choices["tea_override_choices"].where(
        tea_choices["tea_choice_count"].gt(1), pd.NA
    )
    tea_override = tea_choices[["row_id", "tea_base_override", "tea_override_conflict"]]

    # Modifier toppings: explicit customer choices from the order string.
    topping_rows = mapped[
        mapped["token_type"].eq("topping") & mapped["canonical_value"].notna()
    ].copy()
    modifier_topping_qty_long = (
        topping_rows.groupby(["row_id", "canonical_value"], as_index=False)["token_qty"]
        .sum()
    )

    # Default toppings: components that always come with the item (e.g. Mosa signatures).
    default_topping_qty_long = (
        clean[["row_id", "category_key", "item_key"]]
        .merge(
            default_toppings[["category_key", "item_key", "component_key", "qty"]],
            on=["category_key", "item_key"],
            how="left",
        )
        .dropna(subset=["component_key"])
        .rename(columns={"component_key": "canonical_value", "qty": "token_qty"})
    )

    # Combine modifier + default toppings, then collapse to per-row canonical features.
    topping_qty_long = pd.concat(
        [
            modifier_topping_qty_long[["row_id", "canonical_value", "token_qty"]],
            default_topping_qty_long[["row_id", "canonical_value", "token_qty"]],
        ],
        ignore_index=True,
    )
    topping_qty_long = (
        topping_qty_long.groupby(["row_id", "canonical_value"], as_index=False)["token_qty"]
        .sum()
    )

    toppings_list = (
        topping_qty_long.groupby("row_id")["canonical_value"]
        .agg(join_unique)
        .rename("toppings_list")
        .reset_index()
    )

    topping_qty_long["topping_pair"] = (
        topping_qty_long["canonical_value"].astype(str).str.strip()
        + ":"
        + topping_qty_long["token_qty"].map(format_qty)
    )
    toppings_qty = (
        topping_qty_long.sort_values(["row_id", "canonical_value"])
        .groupby("row_id")["topping_pair"]
        .agg("|".join)
        .rename("toppings_qty")
        .reset_index()
    )
    topping_stats = (
        topping_qty_long.groupby("row_id")
        .agg(
            topping_types_count=("canonical_value", "nunique"),
            topping_units_total=("token_qty", "sum"),
            max_single_topping_qty=("token_qty", "max"),
        )
        .reset_index()
    )

    # Build deterministic blend strings from weighted components.
    blend_rules = blend_rules.copy()
    blend_rules["share"] = pd.to_numeric(blend_rules["share"], errors="coerce")
    blend_rules["pair"] = (
        blend_rules["component_tea"].astype(str).str.strip()
        + ":"
        + blend_rules["share"].map(lambda v: f"{v:g}" if pd.notna(v) else "")
    )
    blend_rows = blend_rules[blend_rules["pair"].ne(":")].sort_values(
        ["category_key", "item_key", "component_tea"]
    )
    blend_agg = (
        blend_rows.groupby(["category_key", "item_key"])["pair"]
        .agg("|".join)
        .reset_index(name="tea_blend")
    )

    # QA: warn if any blend shares do not sum to 1.
    blend_share_check = (
        blend_rules.groupby(["category_key", "item_key"], as_index=False)["share"]
        .sum()
        .rename(columns={"share": "share_sum"})
    )
    bad_share = blend_share_check[(blend_share_check["share_sum"] - 1.0).abs() > 1e-6]
    if not bad_share.empty:
        print("WARNING: blend share sums not equal to 1 for:")
        print(bad_share.to_string(index=False))

    # Merge canonicalized features back to one row per original sale row.
    df = clean.merge(
        item_rules[["category_key", "item_key", "default_tea_base", "requires_tea_choice"]],
        on=["category_key", "item_key"],
        how="left"
    )
    df = df.merge(blend_agg, on=["category_key", "item_key"], how="left")
    df = df.merge(tea_override, on="row_id", how="left")
    df = df.merge(toppings_list, on="row_id", how="left")
    df = df.merge(toppings_qty, on="row_id", how="left")
    df = df.merge(topping_stats, on="row_id", how="left")

    df["requires_tea_choice"] = (
        pd.to_numeric(df["requires_tea_choice"], errors="coerce").fillna(0).astype("Int64")
    )
    df["toppings_list"] = df["toppings_list"].fillna("")
    df["toppings_qty"] = df["toppings_qty"].fillna("")
    df["topping_types_count"] = df["topping_types_count"].fillna(0).astype("Int64")
    df["topping_units_total"] = df["topping_units_total"].fillna(0.0)
    df["max_single_topping_qty"] = df["max_single_topping_qty"].fillna(0.0)
    df["has_topping"] = df["topping_types_count"].gt(0)
    df["has_multiple_toppings"] = df["topping_types_count"].gt(1)
    df["topping_multiplier_class"] = "none_or_single"
    df.loc[df["max_single_topping_qty"] >= 2, "topping_multiplier_class"] = "double"
    df.loc[df["max_single_topping_qty"] >= 3, "topping_multiplier_class"] = "triple"
    df.loc[df["max_single_topping_qty"] >= 4, "topping_multiplier_class"] = "quad_or_more"

    # Tea resolution precedence:
    # conflict -> override -> blend -> default -> missing_choice -> unknown
    df["tea_base_final"] = pd.NA
    df["tea_resolution"] = "unknown"

    conflict_mask = (
        df["tea_override_conflict"].notna()
        & df["tea_override_conflict"].astype(str).str.strip().ne("")
    )
    df.loc[conflict_mask, "tea_resolution"] = "conflict"
    df.loc[conflict_mask, "tea_base_final"] = pd.NA

    override_mask = (
        df["tea_base_override"].notna()
        & df["tea_base_override"].astype(str).str.strip().ne("")
        & ~conflict_mask
    )
    df.loc[override_mask, "tea_base_final"] = df.loc[override_mask, "tea_base_override"]
    df.loc[override_mask, "tea_resolution"] = "override"

    blend_mask = (
        df["tea_blend"].notna()
        & df["tea_blend"].astype(str).str.strip().ne("")
        & ~conflict_mask
        & ~override_mask
    )
    df.loc[blend_mask, "tea_base_final"] = df.loc[blend_mask, "tea_blend"]
    df.loc[blend_mask, "tea_resolution"] = "blend"

    default_mask = (
        df["default_tea_base"].notna()
        & df["default_tea_base"].astype(str).str.strip().ne("")
        & ~conflict_mask
        & ~override_mask
        & ~blend_mask
    )
    df.loc[default_mask, "tea_base_final"] = df.loc[default_mask, "default_tea_base"]
    df.loc[default_mask, "tea_resolution"] = "default"

    missing_choice_mask = (
        df["requires_tea_choice"].eq(1)
        & df["tea_resolution"].eq("unknown")
    )
    df.loc[missing_choice_mask, "tea_resolution"] = "missing_choice"

    return df, unknown_modifier_summary


def write_outputs(df, unknown_modifier_summary, output_path, debug_output_path, unknown_output_path):
    """Write full debug output and a slim analysis output."""
    output_path = Path(output_path)
    debug_output_path = Path(debug_output_path)
    unknown_output_path = Path(unknown_output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    debug_output_path.parent.mkdir(parents=True, exist_ok=True)
    unknown_output_path.parent.mkdir(parents=True, exist_ok=True)

    print("tea_resolution counts:")
    print(df["tea_resolution"].value_counts(dropna=False).to_string())

    # Sort deterministically to reduce noisy diffs across runs.
    sort_cols = [c for c in ["Date", "category_key", "item_key", "row_id"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    # Write full debug output (all intermediate columns).
    df.to_csv(debug_output_path, index=False)
    print(f"wrote {debug_output_path}")

    # Write slim analysis output (reduced clutter).
    final_cols = [
        "Date",
        "Category",
        "Item",
        "Qty",
        "ice_pct",
        "sugar_pct",
        "has_topping",
        "has_multiple_toppings",
        "toppings_list",
        "toppings_qty",
        "topping_types_count",
        "topping_units_total",
        "max_single_topping_qty",
        "topping_multiplier_class",
        "category_key",
        "item_key",
        "tea_base_final",
        "tea_resolution",
    ]
    df_final = df[final_cols].copy()
    df_final.to_csv(output_path, index=False)
    print(f"wrote {output_path}")

    unknown_modifier_summary.to_csv(unknown_output_path, index=False)
    unknown_count = (
        int(unknown_modifier_summary["count"].sum())
        if not unknown_modifier_summary.empty
        else 0
    )
    print(f"Unknown modifier token count: {unknown_count}")
    print(f"wrote {unknown_output_path}")

def main():
    args = parse_args()
    df, unknown_modifier_summary = run_canonicalization(
        clean_path=args.input,
        token_map_path=args.token_map,
        item_rules_path=args.item_rules,
        blend_rules_path=args.blend_rules,
        default_components_path=args.default_components,
    )
    unknown_output_path = args.unknown_output or str(
        Path(args.output).with_name("unknown_modifier_tokens.csv")
    )
    write_outputs(
        df=df,
        unknown_modifier_summary=unknown_modifier_summary,
        output_path=args.output,
        debug_output_path=args.debug_output,
        unknown_output_path=unknown_output_path,
    )


if __name__ == "__main__":
    main()
