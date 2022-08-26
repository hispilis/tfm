from datetime import datetime, timedelta

import pandas as pd
from tqdm import tqdm

from extractors import (
    ExtractorAbc,
    ExtractorCincoDias,
    ExtractorElPais,
    ExtractorEuropaPress,
)

# Fecha de extracción de los datos
start_date = datetime(2021, 5, 1)
end_date = start_date + pd.offsets.MonthEnd()
end_date = end_date.date()


if __name__ == "__main__":

    extr3 = ExtractorEuropaPress(start_date)
    df = pd.DataFrame(extr3.extract_news())

    extr4 = ExtractorAbc(start_date)
    df = pd.concat([df, pd.DataFrame(extr4.extract_news())], ignore_index=True)

    extr5 = ExtractorElPais(start_date)
    df = pd.concat([df, pd.DataFrame(extr5.extract_news())], ignore_index=True)

    extr6 = ExtractorCincoDias(start_date)
    df = pd.concat([df, pd.DataFrame(extr6.extract_news())], ignore_index=True)


for date in tqdm(pd.date_range(start=start_date, end=end_date)[1:]):
    extr3.date_news = date
    df = pd.concat([df, pd.DataFrame(extr3.extract_news())], ignore_index=True)

    extr4.date_news = date
    df = pd.concat([df, pd.DataFrame(extr4.extract_news())], ignore_index=True)

    extr5.date_news = date
    df = pd.concat([df, pd.DataFrame(extr5.extract_news())], ignore_index=True)

    extr6.date_news = date
    df = pd.concat([df, pd.DataFrame(extr6.extract_news())], ignore_index=True)

    df = df.sort_values(by="date")
    df.dropna(subset=["date", "title", "body"], inplace=True)
    df.to_pickle(f"data/outputs/output{date.strftime('%Y%m%d')}.pkl")
    df.to_csv(f"data/outputs/output{date.strftime('%Y%m%d')}.csv", sep=";")

# df = df.sort_values(by="date")
# df.drop(df[pd.isnull(df.body)].index, inplace=True)
# df.to_pickle(f"data/outputs/output{start_date.strftime('%Y%m')}.pkl")
# df.to_csv(f"data/outputs/output{start_date.strftime('%Y%m')}.csv", sep=";")

# Para leer los csv en cualquier otro sitio:
# df = pd.read_csv(f"data/outputs/output202101.csv", sep=";", index_col=0, parse_dates=["date"])
