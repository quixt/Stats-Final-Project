# **********************************************************
# Converts the Global Terrorism Database from Excel to CSV
# CSV makes chunk reading much simpler
# **********************************************************
import pandas as pd


print("[0] Reading GTD...")

df = pd.read_excel("./data/gtd.xlsx", engine="openpyxl")
print("[1] GTD read")
print("[2] Converting GTD...")

df.to_csv("./data/GTD.csv", index=False)
print("[4] GTD successfully converted")