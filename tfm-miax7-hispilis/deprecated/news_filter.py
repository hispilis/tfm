import glob
import os
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from tqdm import tqdm

from s3_dao import S3_Dao


class Filter:
    def __init__(self, df, df_tickers):
        self.df = df
        self.df["date"] = pd.to_datetime(self.df["date"], format="%Y/%m/%d %H:%M")
        self.df_tickers = df_tickers

        tqdm.pandas()

    def func(self, pattern, x):
        if type(x) == str:
            return bool(re.search(pattern, x))
        else:
            return False

    def filter_news_k(self, start_date, end_date, keywords):
        keywords_list = keywords.split(",")
        keywords_upper = [each.upper() for each in keywords_list]
        # keywords_lower = [each.lower() for each in keywords_list]
        total_list = set([*keywords_list, *keywords_upper])
        total_matches = []
        if type(start_date) == str:
            start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
        if end_date is pd.NaT:
            df = self.df[self.df.date >= start_date]
        else:
            if type(end_date) == str:
                end_date = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")
            end_date = end_date + pd.Timedelta(1, "d")
            df = self.df[(self.df.date >= start_date) & (self.df.date < end_date)]
        if df.shape[0] == 0:
            return None
        for kw in total_list:
            matches = []
            if "Melia" in keywords_list[0] or "Meliá" in keywords_list[0]:
                pattern = fr"\b{kw}"
                pattern = re.compile(pattern)
            else:
                pattern = fr"\b{kw}\b"
                pattern = re.compile(pattern)
            matches = list(map(lambda x: self.func(pattern, x), df.body.to_list()))
            total_matches.append(matches)
            # print(kw, sum(matches))

        results = self.df[np.logical_or.reduce(total_matches)]
        print(f"{results.shape[0]} results were found in your dataset for {keywords}")

        return results

    def filter_news(self):
        resultados_iteracion = {
            key: self.filter_news_k(
                start_date=values[0], end_date=values[1], keywords=values[4]
            )
            for key, values in tqdm(
                self.df_tickers.iterrows(), total=self.df_tickers.shape[0], leave=False
            )
        }
        return resultados_iteracion

    def output(self, resultados, cols, name="output_filter_2021", index=False):
        df_news_activos = pd.DataFrame(columns=cols)
        for key, value in resultados.items():
            for k, v in value.items():
                if v is None:
                    continue
                df_news_activo = pd.DataFrame(v)
                df_news_activo["ticker"] = k
                df_news_activo["ticker_name"] = self.df_tickers.loc[k].legal_name
                df_news_activo["sector"] = self.df_tickers.loc[k].sector
                df_news_activo["subsector"] = self.df_tickers.loc[k].subsector
                # TODO : se podria ajustar esto similar a como hace el NER
                df_news_activo["ticker_freq"] = 1
                df_news_activo["ticker_first"] = 0
                df_news_activo["ticker_title"] = 0
                df_news_activo = df_news_activo[cols]
                df_news_activos = pd.concat([df_news_activos, df_news_activo])
        df_news_activos = df_news_activos.reindex(columns=cols)
        df_news_activos.sort_values("date", inplace=True)
        df_news_activos.to_csv(f"data/outputs/{name}_full.csv", sep=";", index=index)
        df_clean = df_news_activos.dropna()
        df_clean.to_csv(f"data/outputs/{name}_dropna.csv", sep=";", index=index)


if __name__ == "__main__":

    empresas = pd.read_csv(
        "data/inputs/listadoempresas.csv",
        sep=";",
        index_col=0,
        parse_dates=["start_date", "end_date"],
    )

    # bucket_list = glob.glob("output_*")
    # os.chdir("data/outputs")
    # bucket_list = [file for file in os.listdir() if file.startswith("output2021")]
    bucket_list = ["data/outputs/output202101.csv"]
    num_articles = pd.DataFrame(
        0, columns=empresas.keywords, index=range(len(bucket_list))
    )
    resultados = {}
    # s3_dao = S3_Dao("hgonzalezb1")
    # bucket_list = s3_dao.get_files("output_*", ".pkl")

    for i, file in tqdm(enumerate(bucket_list), total=len(bucket_list)):
        # df = pd.read_pickle(s3_dao.get_data(file))
        # df = pd.read_pickle(file)

        # df = pd.read_pickle(file)

        df = pd.read_csv(file, sep=";", index_col=0, parse_dates=["date"])

        filter = Filter(df, empresas)
        resultados_iteracion = filter.filter_news()
        resultados[file] = resultados_iteracion

        num_iteration = pd.Series(
            {
                tck: (data.shape[0] if data is not None else data)
                for tck, data in resultados_iteracion.items()
            }
        )
        num_articles.iloc[i] = num_iteration

    cols = [
        "date",
        "title",
        "url",
        "body",
        "resource",
        "ticker",
        "ticker_name",
        "sector",
        "subsector",
        "ticker_freq",
        "ticker_first",
        "ticker_title",
    ]
    filter.output(resultados=resultados, cols=cols)

    num_articles.to_csv("./num_articles_companies_v2.csv", sep=";")
