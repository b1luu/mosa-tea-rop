import pandas as pd 
import re 

clean = pd.read_csv("data/trim/clean.csv", low_memory=False)
token_map = pd.read_csv("data/reference/modifier_token_map.csv")
item_rules = pd.read_csv("data/reference/item_rules.csv")