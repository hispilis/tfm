import pickle
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

import api_bme_handler as abh


class Featurer:
    def __init__(
        self,
        df_bench,
        data_opens,
        data_closes,
        df_news_activos,
        df_tickers,
        window_size,
        paragraphs=False,
    ):
        self.paragraphs = paragraphs
        self.df_news_activos = self.get_news_activos(df_news_activos)

        # self.df_news_activos = self.df_news_activos[0:1000]

        self.df_rd_bench = np.log(df_bench.close).diff()
        self.df_rd_activos = self.get_returns(data_closes)
        self.df_bench = df_bench
        self.data_opens = data_opens
        self.data_closes = data_closes
        self.df_activos_opens = self.concat_periods(data_opens)
        self.df_activos_close = self.concat_periods(data_closes)
        self.df_tickers = df_tickers
        self.window_size = window_size
        # self.beta = self.get_beta(window=30)

        tqdm.pandas()

    def get_news_activos(self, df_news_activos):
        print(f"cleaning news...")
        self.tickers = list(df_news_activos.ticker.sort_values().dropna().unique())
        if "IBEX" in self.tickers:
            self.tickers.remove("IBEX")
        df_news_activos = self.clean_duplicate_news(df_news_activos)
        return df_news_activos

    def clean_duplicate_news(self, df_news_activos):
        # Elimina las noticias duplicadas por ticker
        if not self.paragraphs:
            df_news_activos.drop_duplicates(subset=["ticker", "title"], inplace=True)

        # Elimina noticias del IBEX que ya habían sido asignadas a tickers
        mask = df_news_activos.duplicated(subset=["title"]) & (
            df_news_activos.ticker == "IBEX"
        )
        df_news_activos.drop(df_news_activos[mask].index, inplace=True)

        # Temporal para que no dé error al no encontrar cotizaciones del IBEX
        df_news_activos.drop(
            df_news_activos[df_news_activos.ticker == "IBEX"].index, inplace=True
        )
        return df_news_activos

    def concat_periods(self, df_activos):
        print(f"concatenate periods...")
        df_final_activo = pd.DataFrame(columns=self.tickers)
        for ticker in self.tickers:
            filter_col = [col for col in df_activos if col.startswith(ticker)]
            df_activo = df_activos[filter_col]
            # Unifico los distintos periodos del activo en el ibex
            if len(filter_col) > 1:
                df_activo[ticker] = df_activo.sum(axis=1, min_count=1)
            df_activo = df_activo.loc[:, [ticker]]
            df_final_activo[ticker] = df_activo
        return df_final_activo

    def get_returns(self, df_activos):
        print(f"computing returns...")
        df_rd_activo = pd.DataFrame(columns=self.tickers)
        for ticker in self.tickers:
            filter_col = [col for col in df_activos if col.startswith(ticker)]
            df_activo = df_activos[filter_col]
            # Unifico los distintos periodos del activo en el ibex
            if len(filter_col) > 1:
                df_activo[ticker] = df_activo.sum(axis=1, min_count=1)
            df_activo = df_activo.loc[:, [ticker]]
            df_rd_activo[ticker] = np.log(df_activo).diff()
        return df_rd_activo

    def get_beta(self, window=30):
        print(f"computing beta...")
        varianza_ind = self.df_rd_bench.rolling(window).var()
        cov_act_ind = self.df_rd_activos.rolling(window).cov(self.df_rd_bench)
        beta = cov_act_ind.div(varianza_ind, axis=0)
        return beta

    def get_current_day(self, df_rd_activo, dt):
        date = dt.date()
        delta = 0
        if dt.hour >= 9 and dt.minute >= 0:
            delta = 1
        date = date + timedelta(delta)
        date = datetime(date.year, date.month, date.day)
        date = df_rd_activo.index[df_rd_activo.index.get_loc(date, method="bfill")]
        return date

    def get_next_day(self, df_rd_activo, dt):
        date = dt + timedelta(1)
        date = datetime(date.year, date.month, date.day)
        date = df_rd_activo.index[df_rd_activo.index.get_loc(date, method="bfill")]
        return date

    def get_dates(self, df_rd_activo, dt, delta=0):
        # Este método se podría eliminar, sin embargo lo dejo por si en un futuro nos sirve
        # El delta va en número de días
        d = self.get_current_day(df_rd_activo, dt)
        delta = int(delta)
        date = d
        for i in range(delta):
            date = self.get_next_day(df_rd_activo, date)
        return date

    def get_start_date(self, df_activo_close, dt):
        # ojo pasar df_activo_close, no df_rd_activo para evitar errores
        # el primer día de un activo que en realidad no tiene retorno pero sí precio.
        if type(dt) == str:
            dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        date = dt.date()
        df_activo_close.dropna(inplace=True)
        try:
            start_date = df_activo_close.index[
                df_activo_close.index.get_loc(dt, method="ffill")
            ]
            if dt.hour < 9:
                start_date = df_activo_close.index[
                    df_activo_close.index.get_loc(dt - timedelta(1), method="ffill")
                ]
                open_close_start = "close"
            elif dt > datetime(date.year, date.month, date.day, 17, 35, 0):
                open_close_start = "close"
            else:  # dt.hour >= 9
                if start_date.date() == date:
                    open_close_start = "open"
                else:
                    open_close_start = "close"
            return start_date, open_close_start
        except KeyError:
            return None, None

    def get_end_date(self, df_activo_close, dt):
        if type(dt) == str:
            dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        start_date, open_close_start = self.get_start_date(df_activo_close, dt)
        df_activo_close.dropna(inplace=True)
        if open_close_start == "open":
            end_date = start_date
            open_close_end = "close"
        else:
            try:
                end_date = df_activo_close.index[
                    df_activo_close.index.get_loc(
                        start_date + timedelta(1), method="bfill"
                    )
                ]
                open_close_end = "open"
            except KeyError:
                return None, None
        return end_date, open_close_end

    def get_window_data(
        self,
        df_activo_open,
        df_activo_close,
        date,
        open_close,
        window_size,
        exante=True,
    ):

        if open_close == "open":
            if exante:
                end_row = df_activo_close.index.get_loc(date)
                start_row = end_row - window_size
                if start_row < 0:
                    start_row = 0
                concat_close_open = pd.concat(
                    [
                        df_activo_close.iloc[start_row:end_row],
                        df_activo_open.loc[date:date],
                    ]
                )
            else:
                start_row = df_activo_close.index.get_loc(date)
                end_row = start_row + window_size
                if df_activo_close.shape[0] < end_row:
                    end_row = df_activo_close.shape[0]
                concat_close_open = pd.concat(
                    [
                        df_activo_open.loc[date:date],
                        df_activo_close.iloc[start_row:end_row],
                    ]
                )
            return concat_close_open

        else:
            if exante:
                end_row = df_activo_close.index.get_loc(date) + 1
                start_row = end_row - window_size - 1
                if start_row < 0:
                    start_row = 0
            else:
                start_row = df_activo_close.index.get_loc(date) + 1
                end_row = start_row + window_size - 1
                if df_activo_close.shape[0] < end_row:
                    end_row = df_activo_close.shape[0]
            return df_activo_close.iloc[start_row:end_row]

    def get_vola(self, data, annualized=True):
        vola = np.log(data).diff().dropna().std()

        if annualized:
            vola *= np.sqrt(252)

        return vola

    def get_alpha(self, data_activo, data_bench):
        df_rd_activo = np.log(data_activo).diff().dropna()
        df_rd_bench = np.log(data_bench).diff().dropna()
        varianza_ind = df_rd_bench.rolling(self.window_size).var()
        if df_rd_activo.shape[0] < self.window_size:
            return None
        cov_act_ind = df_rd_activo.rolling(self.window_size).cov(df_rd_bench)
        beta = cov_act_ind.div(varianza_ind, axis=0)
        alpha = df_rd_activo.subtract(
            beta.multiply(df_rd_bench, axis=0), axis=0
        ).dropna()
        return alpha

    def get_umbral(self, df_activo_close, date, window_size, annualized=True):

        # opt1 umbral fijo
        # return 0.05

        # opt2 umbral % volatilidad historica del activo
        # # pct = 0.5
        # pct = 0.25
        # vola = np.log(df_activo_close).diff().dropna().std()
        # vola *= np.sqrt(252)
        # return vola * pct

        # opt3 umbral % media volatilidad enventanada historica del activo
        pct = 0.25
        vola = np.log(df_activo_close).diff().dropna().rolling(window_size).std()
        if annualized:
            vola *= np.sqrt(252)
        return np.mean(vola) * pct

        # opt4 umbral % vola enventanada del 2021
        # d2021_1 = datetime(2021, 1, 1, 0, 0, 0)
        # d2021_2 = datetime(2021, 12, 31, 23, 59, 59)
        # # pct = 0.25
        # pct = 0.10
        # vola = np.log(df_activo_close[d2021_1:d2021_2]).diff().dropna().std()
        # vola *= np.sqrt(252)
        # return vola * pct

        # opt5 umbral % en relacion con el indice

    def extract(self, row):
        df_activo_close = self.df_activos_close[row.ticker]
        df_activo_open = self.df_activos_opens[row.ticker]
        start_date, open_close_start = self.get_start_date(df_activo_close, row.date)
        if start_date == None:
            return row
        end_date, open_close_end = self.get_end_date(df_activo_close, row.date)
        if end_date == None:
            return row

        d1 = start_date.strftime("%Y-%m-%d")
        d2 = end_date.strftime("%Y-%m-%d")

        if open_close_start == "open":
            open_value = df_activo_open.get(d1, np.NaN)
            open_bench_value = self.df_bench.open.get(d1, np.NaN)
        else:
            open_value = df_activo_close.get(d1, np.NaN)
            open_bench_value = self.df_bench.close.get(d1, np.NaN)

        if open_close_end == "open":
            close_value = df_activo_open.get(d2, np.NaN)
            close_bench_value = self.df_bench.open.get(d2, np.NaN)
        else:
            close_value = df_activo_close.get(d2, np.NaN)
            close_bench_value = self.df_bench.close.get(d2, np.NaN)

        # podria ser algo tan sencillo como restar close a open
        row["rd_activo_news"] = np.diff(np.log([open_value, close_value]))[0]
        row["rd_bench_news"] = np.diff(np.log([open_bench_value, close_bench_value]))[0]

        data_activo_exante = self.get_window_data(
            df_activo_open, df_activo_close, d1, open_close_start, self.window_size
        )
        data_activo_expost = self.get_window_data(
            df_activo_open, df_activo_close, d2, open_close_end, self.window_size
        )

        data_activo_exante_x4 = self.get_window_data(
            df_activo_open, df_activo_close, d1, open_close_start, self.window_size * 4
        )
        data_activo_expost_x4 = self.get_window_data(
            df_activo_open, df_activo_close, d2, open_close_end, self.window_size * 4
        )

        data_bench_exante = self.get_window_data(
            self.df_bench.open,
            self.df_bench.close,
            d1,
            open_close_start,
            self.window_size * 4,
        )
        data_bench_expost = self.get_window_data(
            self.df_bench.open,
            self.df_bench.close,
            d2,
            open_close_end,
            self.window_size * 4,
        )

        data_activo_expost_future = self.get_window_data(
            df_activo_open,
            df_activo_close,
            d2,
            open_close_end,
            self.window_size,
            exante=False,
        )

        alpha_exante = self.get_alpha(data_activo_exante_x4, data_bench_exante)
        if alpha_exante is None:
            return row
        row["alpha_exante"] = alpha_exante.iloc[-1]
        row["alpha_exante_Q2"] = alpha_exante.quantile(0.05)
        row["alpha_exante_Q3"] = alpha_exante.quantile(0.95)

        alpha_expost = self.get_alpha(data_activo_expost_x4, data_bench_expost)
        row["alpha_expost"] = alpha_expost.iloc[-1]
        row["alpha_expost_Q2"] = alpha_expost.quantile(0.05)
        row["alpha_expost_Q3"] = alpha_expost.quantile(0.95)

        if row["alpha_expost"] <= row["alpha_expost_Q2"]:
            row["alpha_expost_label"] = -1
        elif row["alpha_expost"] >= row["alpha_expost_Q3"]:
            row["alpha_expost_label"] = 1
        else:
            row["alpha_expost_label"] = 0

        # Podemos reutilizar la función y meter más periodos
        row["vola20_exante"] = self.get_vola(data_activo_exante)
        if np.isnan(row["vola20_exante"]):
            return row
        row["vola20_expost"] = self.get_vola(data_activo_expost_future)

        row["vola20_diff"] = row["vola20_expost"] - row["vola20_exante"]

        row["vola20_umbral"] = self.get_umbral(df_activo_close, d2, self.window_size)

        if row["vola20_diff"] > row["vola20_umbral"]:
            # La vola ha subido por encima del umbral, la noticia es "negativa"
            row["vola_label"] = -1
        elif row["vola20_diff"] < -row["vola20_umbral"]:
            # La vola ha bajado por debajo del umbral, la noticia es "positiva"
            row["vola_label"] = 1
        else:
            row["vola_label"] = 0

        return row

    def featuring(self):
        print(f"featuring...")
        self.df_news_activos = self.df_news_activos.progress_apply(
            lambda row: self.extract(row) if not pd.isna(row.ticker) else row, axis=1
        )
        # self.df_news_activos = self.df_news_activos.fillna(method="ffill")
        # self.df_news_activos = self.df_news_activos.fillna(method="bfill")
        return self.df_news_activos

    def output(
        self,
        df,
        columns,
        ruta="data/outputs",
        name="output_featuring_2021_01",
        index=False,
    ):
        df = df.reindex(columns=columns)
        df.sort_values(["date", "pk"], inplace=True)
        # limpiamos noticias no asignadas a ticker. mas velocidad, perdemos noticias generalistas
        # df.to_csv(f'{ruta}/{name}_full.csv', sep=";")
        df_clean = df.dropna()
        df_clean.to_csv(f"{ruta}/{name}.csv", sep=";", index=index)


if __name__ == "__main__":
    print("loading dataset")
    df_tickers = pd.read_csv(
        "data/inputs/listadoempresas.csv",
        sep=";",
        parse_dates=["start_date", "end_date"],
        index_col=0,
    )

    df_news_activos = pd.read_csv(
        "data/outputs/20210101/output_classifier_20210101.csv",
        sep=";",
        index_col=0,
        parse_dates=["date"],
    )

    columns = df_news_activos.columns.tolist()[:]
    print(columns)

    api_handler = abh.APIBMEHandler(market="IBEX")
    try:
        with open("data/outputs/ibex.pkl", "rb") as f:
            df_ibex = pickle.load(f)
    except:
        df_ibex = api_handler.get_benchmark()
        df_ibex.to_pickle("data/outputs/ibex.pkl")

    try:
        with open("data/outputs/activos_opens.pkl", "rb") as f:
            data_opens = pickle.load(f)
        with open("data/outputs/activos_closes.pkl", "rb") as f:
            data_closes = pickle.load(f)
    except:
        data_opens, data_closes = api_handler.get_opens_closes_data()
        data_opens.to_pickle("data/outputs/activos_opens.pkl")
        data_closes.to_pickle("data/outputs/activos_closes.pkl")

    window_size = 20

    featurer = Featurer(
        df_ibex, data_opens, data_closes, df_news_activos, df_tickers, window_size
    )

    tqdm.pandas()
    df = featurer.featuring()
    # df.dropna(inplace=True)

    new_cols = [
        "rd_activo_news",
        "rd_bench_news",
        "alpha_exante",
        "alpha_exante_Q2",
        "alpha_exante_Q3",
        "alpha_expost",
        "alpha_expost_Q2",
        "alpha_expost_Q3",
        "alpha_expost_label",
        "vola20_exante",
        "vola20_expost",
        "vola20_diff",
        "vola20_umbral",
        "vola_label",
    ]

    columns = [*columns, *new_cols]
    print(columns)
    featurer.output(
        df, columns, "data/outputs/20210101", "output_featuring_20210101", True
    )

    # df.sort_values("date", inplace=True)
    # df.to_csv("data/outputs/entry_dataset.csv", sep=";")

    # df_input = pd.read_csv("data/outputs/entry_dataset.csv", sep=";", index_col=0, parse_dates=["date"])

    # df_input["topic"] = np.NaN
    # df_input["topic_duda"] = np.NaN
    # df_input["finanzas_s_n"] = np.NaN
    # df_input["finanzas_s_n_duda"] = np.NaN
    # df_input["impacto_s_n"] = np.NaN
    # df_input["impacto_s_n_duda"] = np.NaN

    # dataset.to_csv("data/outputs/dataset.csv", sep=";")

    # Separacion en parrafos
    # No tiene sentido hacer aqui la separacion por parrafos
    # df_news_activos = pd.read_csv("data/outputs/dataset.csv", sep=";", index_col=0, parse_dates=["date"])
    # p = Preproccesor(df_news_activos, bigram=False)
    # df_news_paragraphs = p.paragraphe()
    # df_news_paragraphs.to_csv("data/outputs/dataset_paragraph.csv", sep=";")

    # df_input = pd.read_csv("data/outputs/old_entry_dataset.csv", sep=";", index_col=0, parse_dates=["date"])
    # model.modelling(df_input)
