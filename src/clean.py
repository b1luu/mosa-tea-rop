"""Clean raw Square export data for demand analysis."""

import pandas as pd

INPUT_PATH = "data/raw/raw.csv"
OUTPUT_PATH = "data/trim/clean.csv"

USE_COLS = [
    "Date",
    "Category",
    "Item",
    "Qty",
    "Modifiers Applied",
    "Event Type",
]

CJK_PATTERN = r"[\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]"
FREE_DRINK_ITEM = "Free Drink (100☼ Reward)"
FIXED_ICE_ITEMS = {
    "Strawberry Matcha Latte",
    "Mango Matcha Latte",
    "Chestnut Forest",
}


def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["Category", "Item"]:
        df[col] = (
            df[col]
            .fillna("")
            .str.replace(CJK_PATTERN, "", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
    return df


def main() -> None:
    clean = pd.read_csv(
        INPUT_PATH,
        usecols=USE_COLS,
        low_memory=False,
    )

    # Normalize key types early.
    clean["Date"] = pd.to_datetime(clean["Date"], errors="coerce")
    clean["Qty"] = pd.to_numeric(clean["Qty"], errors="coerce")
    clean = clean.dropna(subset=["Date", "Qty"]).copy()

    # Normalize text fields.
    clean = clean_text_columns(clean)

    # Refund handling summary and filter.
    event = clean["Event Type"].fillna("").str.strip().str.lower()
    is_payment = event.eq("payment")
    is_refund = event.eq("refund") | (clean["Qty"] < 0)

    print("Payment rows:", int(is_payment.sum()))
    print("Refund rows:", int(is_refund.sum()))
    print("Payment Qty sum:", float(clean.loc[is_payment, "Qty"].sum()))
    print("Refund Qty sum:", float(clean.loc[is_refund, "Qty"].sum()))

    clean = clean.loc[is_payment & (clean["Qty"] > 0)].copy()
    clean = clean.drop(columns=["Event Type"])

    # Remove free-drink rewards.
    reward_mask = clean["Item"].fillna("").eq(FREE_DRINK_ITEM)
    redeemed_rows = int(reward_mask.sum())
    redeemed_qty = float(clean.loc[reward_mask, "Qty"].sum())

    print("Free drink redemption rows:", redeemed_rows)
    print("Free drinks redeemed (Qty):", redeemed_qty)

    clean = clean.loc[~reward_mask].copy()

    # Remove merchandise rows.
    merch_mask = clean["Category"].fillna("").eq("Merchandise")
    print("Removing merchandise rows:", int(merch_mask.sum()))
    clean = clean.loc[~merch_mask].copy()

    # Parse modifiers into numeric percentages.
    mods = clean["Modifiers Applied"].fillna("").astype(str).str.strip()
    clean["ice_pct"] = pd.to_numeric(
        mods.str.extract(r"(?i)\b(\d{1,3})\s*%\s*ice\b")[0],
        errors="coerce",
    )
    clean["sugar_pct"] = pd.to_numeric(
        mods.str.extract(r"(?i)\b(\d{1,3})\s*%\s*sugar\b")[0],
        errors="coerce",
    )
    clean.loc[mods.str.contains(r"(?i)\bno\s*ice\b", regex=True), "ice_pct"] = 0
    clean.loc[mods.str.contains(r"(?i)\bno\s*sugar\b", regex=True), "sugar_pct"] = 0

    # Hot drinks missing an explicit ice token should default to No Ice.
    hot_mask = (
        clean["Category"].fillna("").str.contains(r"(?i)\bhot\b", regex=True)
        | clean["Item"].fillna("").str.contains(r"(?i)^hot\b", regex=True)
    )
    has_ice_token = mods.str.contains(r"(?i)\b(?:no\s*ice|\d{1,3}\s*%\s*ice)\b", regex=True)
    add_no_ice_mask = hot_mask & ~has_ice_token

    clean.loc[add_no_ice_mask, "Modifiers Applied"] = mods[add_no_ice_mask].apply(
        lambda x: "No Ice" if x == "" else f"{x}, No Ice"
    )
    clean.loc[add_no_ice_mask, "ice_pct"] = 0

    # Fixed-ice drinks default to 100% Ice if missing.
    fixed_ice_mask = clean["Item"].isin(FIXED_ICE_ITEMS) & clean["ice_pct"].isna()
    clean.loc[fixed_ice_mask, "ice_pct"] = 100

    no_ice_token_mask = ~clean["Modifiers Applied"].fillna("").str.contains(
        r"(?i)\b(?:no\s*ice|\d{1,3}\s*%\s*ice)\b", regex=True
    )
    mods_fix_mask = fixed_ice_mask & no_ice_token_mask
    clean.loc[mods_fix_mask, "Modifiers Applied"] = (
        clean.loc[mods_fix_mask, "Modifiers Applied"]
        .fillna("")
        .str.strip()
        .apply(lambda x: "100% Ice" if x == "" else f"{x}, 100% Ice")
    )

    clean["ice_pct"] = clean["ice_pct"].astype("Int64")
    clean["sugar_pct"] = clean["sugar_pct"].astype("Int64")

    print("Fixed 100% ice rows:", int(fixed_ice_mask.sum()))
    clean.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    main()

