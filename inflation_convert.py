# **********************************************************
# Converts the IMF Inflation Database from Excel to CSV
# CSV makes chunk reading much simpler
# **********************************************************
import pandas as pd
import xlrd


print("[0] Reading IMF...")

workbook = xlrd.open_workbook('./data/inflation.xls', ignore_workbook_corruption=True)
df = pd.read_excel(workbook, engine="xlrd")
print("[1] IMF read")
print("[2] Converting IMF...")

df.to_csv("./data/inflation.csv", index=False)
print("[4] IMF successfully converted")