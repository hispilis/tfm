import pandas as pd

df = pd.read_csv('s3://hgonzalezb1/market_data.txt', sep=";")
print(df)

df.to_csv('s3://hgonzalezb1/market_data_mod.txt')
