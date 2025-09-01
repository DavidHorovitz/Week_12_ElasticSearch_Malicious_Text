import pandas as pd

df=pd.read_csv("../data/tweets_injected 3.csv")
# print(df.columns)
dict_of_data=df.to_dict(orient="records")