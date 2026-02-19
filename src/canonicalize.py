import pandas as pd
import re

clean = pd.read_csv("data/trim/clean.csv", low_memory=False)
token_map = pd.read_csv("data/reference/modifier_token_map.csv")
item_rules = pd.read_csv("data/reference/item_rules.csv")
blend_rules = pd.read_csv("data/reference/item_blend_rules.csv")
default_comp = pd.read_csv("data/reference/item_default_component.csv")


def norm_key(v):
    s = "" if pd.isna(v) else str(v).lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def join_unique(values):
    vals = sorted({str(v).strip() for v in values if pd.notna(v) and str(v).strip() != ""})
    return "|".join(vals)

clean["row_id"] = range(len(clean))
clean["category_key"] = clean["Category"].map(norm_key)
clean["item_key"] = clean["Item"].map(norm_key)

tokens = (
    clean[["row_id", "Modifiers Applied"]]
    .assign(token=lambda d: d["Modifiers Applied"].fillna("").astype(str).str.split(","))
    .explode("token")
)
tokens["token"] = tokens["token"].fillna("").str.strip()
tokens = tokens[tokens["token"] != ""].copy()
tokens["token_norm"] = tokens["token"].str.lower()

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
mapped["tea_value_norm"] = mapped["canonical_value"].replace(tea_value_map)

tea_choices = (
    mapped[mapped["token_type"].eq("tea_base") & mapped["tea_value_norm"].notna()]
    .groupby("row_id")["tea_value_norm"]
    .agg(join_unique)
    .rename("tea_override_choices")
    .reset_index()
)
tea_choices["tea_choice_count"] = tea_choices["tea_override_choices"].str.split("|").str.len()
tea_choices["tea_base_override"] = tea_choices["tea_override_choices"].where(
    tea_choices["tea_choice_count"].eq(1), pd.NA
)
tea_choices["tea_override_conflict"] = tea_choices["tea_override_choices"].where(
    tea_choices["tea_choice_count"].gt(1), pd.NA
)
tea_override = tea_choices[["row_id", "tea_base_override", "tea_override_conflict"]]

# Build deterministic blend strings from weighted components.
blend_rules = blend_rules.copy()
blend_rules["share"] = pd.to_numeric(blend_rules["share"], errors="coerce")
blend_rules["pair"] = (
    blend_rules["component_tea"].astype(str).str.strip()
    + ":"
    + blend_rules["share"].map(lambda v: f"{v:g}" if pd.notna(v) else "")
)
blend_agg = (
    blend_rules[blend_rules["pair"].ne(":")]
    .sort_values(["category_key", "item_key", "component_tea"])
    .groupby(["category_key", "item_key"], as_index=False)["pair"]
    .agg("|".join)
    .rename(columns={"pair": "tea_blend"})
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

df = clean.merge(
    item_rules[["category_key", "item_key", "default_tea_base", "requires_tea_choice"]],
    on=["category_key", "item_key"],
    how="left"
)
df = df.merge(blend_agg, on=["category_key", "item_key"], how="left")
df = df.merge(tea_override, on="row_id", how="left")

df["requires_tea_choice"] = pd.to_numeric(df["requires_tea_choice"], errors="coerce").fillna(0).astype("Int64")

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

print("tea_resolution counts:")
print(df["tea_resolution"].value_counts(dropna=False).to_string())

df.to_csv("data/trim/canonicalized.csv", index=False)
print("wrote data/trim/canonicalized.csv")
