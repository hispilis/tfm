from datetime import datetime, timedelta

import pandas as pd
from fastapi import FastAPI

from extractors import (
    ExtractorAbc,
    ExtractorCincoDias,
    ExtractorElPais,
    ExtractorEuropaPress,
)

# Fecha de extracción de los datos
date_news = datetime(2021, 12, 1)

app = FastAPI()


@app.get("/")
def home():

    extr3 = ExtractorEuropaPress(date_news)
    df = pd.DataFrame(extr3.extract_news())

    extr4 = ExtractorAbc(date_news)
    df = pd.concat([df, pd.DataFrame(extr4.extract_news())], ignore_index=True)

    extr5 = ExtractorElPais(date_news)
    df = pd.concat([df, pd.DataFrame(extr5.extract_news())], ignore_index=True)

    extr6 = ExtractorCincoDias(date_news)
    df = pd.concat([df, pd.DataFrame(extr6.extract_news())], ignore_index=True)
    df = df.sort_values(by="date")
    df.to_pickle("./output.pkl")
    print(df)
    return {"Hello": "FastAPI"}


# limitamos a un dia, de momento, por rendimiento
# for i in range(1, 2):
#     extr3.date_news = date_news + timedelta(i)
#     df = pd.concat([df, pd.DataFrame(extr3.extract_news())], ignore_index=True)

#     extr4.date_news = date_news + timedelta(i)
#     df = pd.concat([df, pd.DataFrame(extr4.extract_news())], ignore_index=True)

#     extr5.date_news = date_news + timedelta(i)
#     df = pd.concat([df, pd.DataFrame(extr5.extract_news())], ignore_index=True)

#     extr6.date_news = date_news + timedelta(i)
#     df = pd.concat([df, pd.DataFrame(extr6.extract_news())], ignore_index=True)
