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

clean["row_id"] = range(len(clean))
clean["category_key"] = clean["Category"].map(norm_key)
clean["item_key"] = clean["Item"].map(norm_key)

