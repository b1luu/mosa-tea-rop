""" Clean raw Square export data """

import pandas as pd

#Load raw file
raw = pd.read_csv("data/raw/raw.csv")

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



clean.to_csv("data/trim/clean.csv")

