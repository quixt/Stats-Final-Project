import pandas as pd
data = pd.read_stata('./data/religionAndState.DTA')
for index, row in data.iterrows():
    print(row["country"])
print(data)