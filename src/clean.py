""" Clean raw Square export data """

import pandas as pd

# Load only columns we actually use
use_cols = [
    "Date",
    "Category",
    "Item",
    "Qty",
    "Modifiers Applied",
    "Event Type",   # keep for refund handling
]

clean = pd.read_csv(
    "data/raw/raw.csv",
    usecols=use_cols,
    low_memory=False,
)

# Normalize key types early
clean["Date"] = pd.to_datetime(clean["Date"], errors="coerce")
clean["Qty"] = pd.to_numeric(clean["Qty"], errors="coerce")
clean = clean.dropna(subset=["Date", "Qty"]).copy()


#Remove Chinese characters from Category and Item 
cjk_pattern = r"[\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]"
for col in ["Category", "Item"]:
     clean[col] = (
          clean[col]
          .fillna("")
          .str.replace(cjk_pattern, "", regex=True)
          .str.replace(r"\s+", " ", regex=True)
          .str.strip()
     )

# Refund handling
event = clean["Event Type"].fillna("").str.strip().str.lower()
is_payment = event.eq("payment")
is_refund = event.eq("refund") | (clean["Qty"] < 0)

print("Payment rows:", int(is_payment.sum()))
print("Refund rows:", int(is_refund.sum()))
print("Payment Qty sum:", float(clean.loc[is_payment, "Qty"].sum()))
print("Refund Qty sum:", float(clean.loc[is_refund, "Qty"].sum()))  # usually negative

# Keep only sell-through demand rows
clean = clean.loc[is_payment & (clean["Qty"] > 0)].copy()

# No longer needed after filtering
clean = clean.drop(columns=["Event Type"])



#Remove free drink reward for less clutter
reward_item = "Free Drink (100☼ Reward)"
reward_mask = clean["Item"].fillna("").eq(reward_item)

redeemed_rows = int(reward_mask.sum())
redeemed_qty = float(clean.loc[reward_mask, "Qty"].sum())

print("Free drink redemption rows:", redeemed_rows)
print("Free drinks redeemed (Qty):", redeemed_qty)

clean = clean.loc[~reward_mask].copy()

#Parse modifiers into numeric percentages
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

#Hot drinks missing any ice token should default to No Ice
hot_mask = clean["Category"].fillna("").str.contains(r"(?i)\bhot\b", regex=True)
has_ice_token = mods.str.contains(r"(?i)\b(?:no\s*ice|\d{1,3}\s*%\s*ice)\b", regex=True)

fix_mask = hot_mask & ~has_ice_token
# Add "No Ice" text for hot drinks missing any ice setting
clean.loc[fix_mask, "Modifiers Applied"] = mods[fix_mask].apply(
    lambda x: "No Ice" if x == "" else f"{x}, No Ice"
)

# Force those to 0 in parsed output
clean.loc[fix_mask, "ice_pct"] = 0
clean["ice_pct"] = clean["ice_pct"].astype("Int64")
clean["sugar_pct"] = clean["sugar_pct"].astype("Int64")

#Fixed-ice drinks: force blank ice_pct to 100
fixed_ice_items = {
     "Strawberry Matcha Latte",
     "Mango Matcha Latte", 
     "Chestnut Forest",
}

fixed_ice_mask = clean["Item"].isin(fixed_ice_items) & clean["ice_pct"].isna()
clean.loc[fixed_ice_mask, "ice_pct"] = 100

no_ice_token_mask = ~clean["Modifiers Applied"].fillna("").str.contains(
    r"(?i)\b(?:no\s*ice|\d{1,3}\s*%\s*ice)\b", regex=True
)
mods_fix_mask = fixed_ice_mask & no_ice_token_mask
clean.loc[mods_fix_mask, "Modifiers Applied"] = clean.loc[mods_fix_mask, "Modifiers Applied"].fillna("").str.strip().apply(
    lambda x: "100% Ice" if x == "" else f"{x}, 100% Ice"
)

print("Fixed 100% ice rows:", int(fixed_ice_mask.sum()))


clean.to_csv("data/trim/clean.csv", index=False)


