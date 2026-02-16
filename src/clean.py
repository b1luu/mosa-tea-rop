""" Clean raw Square export data """

import pandas as pd

#Load raw file
raw = pd.read_csv(
    "data/raw/raw.csv",
    dtype={"Token": "string", "PAN Suffix": "string"},
    low_memory=False
)

#Manually drop columns
cols_to_drop = [
    "Time",
    "Time Zone",
    "Price Point Name",
    "SKU",
    "Transaction ID",
    "Payment ID",
    "Device Name",
    "Notes",
    "Gross Sales",
    "Discounts",
    "Net Sales",
    "Employee",
    "Fulfillment Note",
    "Channel",
    "Token",
    "Card Brand",
    "Customer Name",
    "Customer Reference ID",
    "Customer ID",
    "PAN Suffix",
    "Dining Option",
    "Location",
    "Event Type",
    "GTIN",
    "Tax",
    "Details",
    "Count",
    "Unit",
    "Itemization Type",
    "Commission",
]
clean = raw.drop(columns=cols_to_drop, errors="ignore").copy()

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


clean.to_csv("data/trim/clean.csv", index=False)


