import re
from collections import Counter

import numpy as np
import pandas as pd
from tqdm.auto import tqdm


class TickersNER:
    def __init__(self, df, df_tickers, paragraphs=False):
        self.df = df
        self.df_tickers = df_tickers
        self.paragraphs = paragraphs

        tqdm.pandas()

    def get_tickers_apply(self, row):
        ticker_list = []
        entities = []
        mask = (row.date > self.df_tickers.start_date) & (
            np.isnan(self.df_tickers.end_date)
            | (row.date < (self.df_tickers.end_date + pd.Timedelta(1, "d")))
        )
        df_tck = self.df_tickers[mask]

        if str(row["body"]) != "nan" and str(row["body"]) != "":
            body = row["body"].replace("<EOL>", " ")
            for tck, val in df_tck.iterrows():
                keywords_list = val["keywords"].split(",")
                keywords_upper = [each.upper() for each in keywords_list]
                total_list = set([*keywords_list, *keywords_upper])
                for kw in total_list:
                    if "Melia" in keywords_list[0] or "Meliá" in keywords_list[0]:
                        pattern = fr"\b{kw}"
                        pattern = re.compile(pattern)
                    else:
                        pattern = fr"\b{kw}\b"
                        pattern = re.compile(pattern)
                    result_tck = re.findall(pattern, body)
                    if len(result_tck) > 0:
                        ticker_list.extend(len(result_tck) * [tck])
                        entities.append(kw)

                ceo_list = val["ceos"].split(",")
                for ceo_kw in ceo_list:
                    pattern = fr"\b{ceo_kw}\b"
                    pattern = re.compile(pattern)
                    result_ceo = re.findall(pattern, body)
                    if len(result_ceo) > 0:
                        ticker_list.extend(len(result_ceo) * [tck])
                        entities.append(ceo_kw)

        if len(entities) > 0:
            row["entities"] = entities
        ticker_freq = Counter(ticker_list)
        row["tickers"] = ticker_freq
        return row

    def get_ticker_info_apply(self, row):
        if len(row.tickers) > 0:
            row["ticker_freq"] = row.tickers[row.ticker]
            if len(row.tickers) > 1 and list(row.tickers.keys())[0] == row.ticker:
                row["ticker_first"] = 1
            else:
                row["ticker_first"] = 0
            for k, v in self.df_tickers["keywords"].to_dict().items():
                if k == row.ticker:
                    l_v = v.split(",")
                    for el_v in l_v:
                        if el_v in row.title:
                            row["ticker_title"] = 1
                            break
                        else:
                            row["ticker_title"] = 0
                    break
                else:
                    row["ticker_title"] = 0
            row["ticker_name"] = self.df_tickers.loc[row.ticker].legal_name
            row["sector"] = self.df_tickers.loc[row.ticker].sector
            row["subsector"] = self.df_tickers.loc[row.ticker].subsector
            if not self.paragraphs:
                row["pk"] = f'{row["pk"]}_{row["ticker"]}'
            return row
        else:
            row["ticker_freq"] = 0
            row["ticker_name"] = ""
            row["sector"] = ""
            row["subsector"] = ""
            return row

    def get_tickers(self):
        print("extrayendo tickers...\n")
        self.df = self.df.progress_apply(
            lambda row: self.get_tickers_apply(row), axis=1
        )
        self.df["ticker"] = self.df["tickers"].progress_apply(lambda k: k.keys())
        self.df = self.df.explode("ticker")
        self.df = self.df.progress_apply(
            lambda row: self.get_ticker_info_apply(row), axis=1
        )

    def output(self, cols, ruta="data/outputs", name="output_ner_2021_01", index=False):
        self.df = self.df.reindex(columns=cols)
        self.df.sort_values(["date", "pk"], inplace=True)
        self.df.dropna(subset=["date", "title", "body", "ticker"], inplace=True)
        self.df.to_csv(f"{ruta}/{name}.csv", sep=";", index=index)
