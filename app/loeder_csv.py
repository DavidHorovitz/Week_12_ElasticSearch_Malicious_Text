import pandas as pd
df=pd.read_csv("../data/tweets_injected 3.csv")
print(df.columns)
print(df["CreateDate"].head(3))