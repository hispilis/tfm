import logging

import pandas as pd
import requests

logger = logging.getLogger(__name__)


def allocs_to_frame(json_allocations):
    alloc_list = []
    for json_alloc in json_allocations:
        allocs = pd.DataFrame(json_alloc["allocations"])
        allocs.set_index("ticker", inplace=True)
        alloc_serie = allocs["alloc"]
        alloc_serie.name = json_alloc["date"]
        alloc_list.append(alloc_serie)
    all_alloc_df = pd.concat(alloc_list, axis=1).T
    return all_alloc_df


class APIBMEHandler:
    def __init__(self, market):
        self.url_base = "https://miax-gateway-jog4ew3z3q-ew.a.run.app"
        self.competi = "mia_7"
        self.user_key = "AIzaSyA_Fqdnlw3_heV6I9KQFIT2ND2pZ7dTCZk"
        self.market = market

    def get_ticker_master(self):
        url = f"{self.url_base}/data/ticker_master"
        params = {"competi": self.competi, "market": self.market, "key": self.user_key}
        response = requests.get(url, params)
        tk_master = response.json()
        maestro_df = pd.DataFrame(tk_master["master"])
        return maestro_df

    def get_close_data(self):
        maestro_df = self.get_ticker_master()
        data_close = {}
        for i, data in maestro_df.iterrows():
            ticker = data.ticker
            logger.info(ticker)
            data_close[ticker] = self.get_close_data_ticker(ticker)
        data_close = pd.DataFrame(data_close)
        return data_close

    def get_opens_closes_data(self):
        maestro_df = self.get_ticker_master()
        data_ohlcv = {}
        d_opens = {}
        d_closes = {}
        for i, data in maestro_df.iterrows():
            ticker = data.ticker
            logger.info(ticker)
            data_ohlcv[ticker] = self.get_ohlcv_ticker(ticker)
            d_opens[ticker] = data_ohlcv[ticker].open
            d_closes[ticker] = data_ohlcv[ticker].close

        data_opens = pd.DataFrame(d_opens)
        data_closes = pd.DataFrame(d_closes)
        return data_opens, data_closes

    def get_benchmark(self):
        url2 = f"{self.url_base}/data/time_series"
        params = {
            "market": self.market,
            "key": self.user_key,
            "ticker": "benchmark",
            "close": False,
        }
        response = requests.get(url2, params)
        tk_data = response.json()
        df_benchmark = pd.read_json(tk_data, typ="frame")
        return df_benchmark

    def get_ohlcv_ticker(self, ticker):
        url2 = f"{self.url_base}/data/time_series"
        params = {
            "market": self.market,
            "key": self.user_key,
            "ticker": ticker,
            "close": False,
        }
        response = requests.get(url2, params)
        tk_data = response.json()
        if response.status_code == 200:
            df_data = pd.read_json(tk_data, typ="frame")
            return df_data
        else:
            print(response.text)

    def get_close_data_ticker(self, ticker):
        url = f"{self.url_base}/data/time_series"
        params = {"market": self.market, "key": self.user_key, "ticker": ticker}
        response = requests.get(url, params)
        tk_data = response.json()
        series_data = pd.read_json(tk_data, typ="series")
        return series_data
