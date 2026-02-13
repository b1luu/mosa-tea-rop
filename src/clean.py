""" Clean raw Square export data """

import pandas as pd

#Load raw file
raw = pd.read_csv("data/raw/raw.csv")

#Manually drop columns

cols_to_drop = {
    "Time",
    
}