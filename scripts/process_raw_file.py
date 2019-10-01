import pandas as pd

df = pd.read_csv("dois_20190618.csv", parse_dates=['dcdate'])
df.set_index(df.columns[0], inplace=True)

# drop duplicates
df = df.drop_duplicates(subset="doi")

# Reset index
df.index = range(len(df))
df.index.name = "id"

df.to_csv("dois_20190618_cleaned.csv")