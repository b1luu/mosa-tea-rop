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
clean = raw.drop(columns=cols_to_drop, errors="ignore")

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

reward_item = "Free Drink (100☼ Reward)"
reward_mask = clean["Item"].fillna("").eq(reward_item)

redeemed_rows = int(reward_mask.sum())
redeemed_qty = float(clean.loc[reward_mask, "Qty"].sum())

print("Free drink redemption rows:", redeemed_rows)
print("Free drinks redeemed (Qty):", redeemed_qty)

clean = clean.loc[~reward_mask].copy()


clean.to_csv("data/trim/clean.csv", index=False)
