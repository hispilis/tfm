from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from tqdm.auto import tqdm


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
        print(f"cleaning news...\n")
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

        # Mientras no incluyamos las noticias que se asignen al IBEX
        df_news_activos.drop(
            df_news_activos[df_news_activos.ticker == "IBEX"].index, inplace=True
        )
        return df_news_activos

    def concat_periods(self, df_activos):
        print(f"concatenate periods...\n")
        df_final_activo = pd.DataFrame(columns=self.tickers)
        for ticker in self.tickers:
            filter_col = [col for col in df_activos if col.startswith(ticker)]
            df_activo = df_activos[filter_col]
            # Unificando en una sola serie por activo los distintos periodos que
            # el activo ha formado parte del IBEX
            if len(filter_col) > 1:
                df_activo[ticker] = df_activo.sum(axis=1, min_count=1)
            df_activo = df_activo.loc[:, [ticker]]
            df_final_activo[ticker] = df_activo
        return df_final_activo

    def get_returns(self, df_activos):
        print(f"computing returns...\n")
        df_activos_concat = self.concat_periods(df_activos)
        df_rd_activos = np.log(df_activos_concat).diff()
        return df_rd_activos

    def get_start_date(self, df_activo_close, dt):
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
                start_row = df_activo_close.index.get_loc(date) + 1
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
                return df_activo_close.iloc[start_row:end_row]

            else:
                start_row = df_activo_close.index.get_loc(date) + 1
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

    def get_umbrales(
        self,
        data_activo_exante_x4,
        data_activo_expost_future_x4,
        d1,
        open_close_end,
        window_size,
        abs=False,
    ):
        if open_close_end == "open":
            data_activo = pd.concat(
                [data_activo_exante_x4, data_activo_expost_future_x4]
            )
        else:
            data_activo = pd.concat(
                [data_activo_exante_x4, data_activo_expost_future_x4.iloc[1:]]
            )
        rent_activo = np.log(data_activo).diff().dropna()
        old_window_vola = rent_activo.rolling(window_size).std()
        new_window_vola = rent_activo.rolling(window_size).std().shift(-window_size)
        if abs:
            pct_change = np.abs(new_window_vola / old_window_vola - 1)
        else:
            pct_change = new_window_vola / old_window_vola - 1
        umbral_q25 = pct_change.rolling(window_size).quantile(0.25)
        umbral_q75 = pct_change.rolling(window_size).quantile(0.75)
        return umbral_q25.loc[d1], umbral_q75.loc[d1]

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

        row["rd_activo_news"] = np.diff(np.log([open_value, close_value]))[0]
        row["rd_bench_news"] = np.diff(np.log([open_bench_value, close_bench_value]))[0]

        data_activo_exante = self.get_window_data(
            df_activo_open, df_activo_close, d1, open_close_start, self.window_size
        )

        # x4 para que haya una ventana de 3 meses completa
        data_activo_exante_x4 = self.get_window_data(
            df_activo_open, df_activo_close, d1, open_close_start, self.window_size * 4
        )

        data_bench_exante_x4 = self.get_window_data(
            self.df_bench.open,
            self.df_bench.close,
            d1,
            open_close_start,
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

        data_activo_expost_future_x4 = self.get_window_data(
            df_activo_open,
            df_activo_close,
            d2,
            open_close_end,
            self.window_size * 4,
            exante=False,
        )

        alpha_exante = self.get_alpha(data_activo_exante_x4, data_bench_exante_x4)
        if alpha_exante is None:
            return row
        row["alpha_exante"] = alpha_exante.iloc[-1]
        row["alpha_exante_Q1"] = alpha_exante.quantile(0.25)
        row["alpha_exante_below_Q1"] = alpha_exante.iloc[-1] < alpha_exante.quantile(
            0.25
        )
        row["alpha_exante_Q3"] = alpha_exante.quantile(0.75)
        row["alpha_exante_above_Q3"] = (
            alpha_exante.quantile(0.75) < alpha_exante.iloc[-1]
        )

        # Podemos reutilizar la función y meter más periodos
        row["vola20_exante"] = self.get_vola(data_activo_exante)
        if np.isnan(row["vola20_exante"]):
            return row
        row["vola20_expost"] = self.get_vola(data_activo_expost_future)
        row["vola20_antepost_pct_change"] = (
            row["vola20_expost"] / row["vola20_exante"] - 1
        )
        row["vola20_umbral_Q1"], row["vola20_umbral_Q3"] = self.get_umbrales(
            data_activo_exante_x4,
            data_activo_expost_future_x4,
            d1,
            open_close_end,
            self.window_size,
        )
        row["vola20_umbral_abs_Q1"], row["vola20_umbral_abs_Q3"] = self.get_umbrales(
            data_activo_exante_x4,
            data_activo_expost_future_x4,
            d1,
            open_close_end,
            self.window_size,
            True,
        )

        if row["vola20_antepost_pct_change"] > row["vola20_umbral_Q3"]:
            # La vola se ha incrementado por encima del umbral, la noticia ha causado
            # un incremento significativo en la ventana de 20 días de la vola  (= noticia generalmente negativa)
            row["vola_label"] = 1
        elif row["vola20_antepost_pct_change"] < row["vola20_umbral_Q1"]:
            # La vola se ha decrementado por debajo del umbral, la noticia ha causado
            # un descenso significativo en la ventana de 20 días de la vola (= noticia generalmente positiva)
            row["vola_label"] = -1
        else:
            row["vola_label"] = 0

        if row["vola20_antepost_pct_change"] > row["vola20_umbral_abs_Q3"]:
            # La vola se ha incrementado por encima del umbral, la noticia ha causado
            # un incremento absoluto significativo en la ventana de 20 días de la vola  (= noticia generalmente negativa)
            row["vola_label_abs"] = 1
        elif row["vola20_antepost_pct_change"] < -row["vola20_umbral_abs_Q3"]:
            # La vola se ha decrementado por debajo del umbral, la noticia ha causado
            # un descenso absoluto significativo en la ventana de 20 días de la vola (= noticia generalmente positiva)
            row["vola_label_abs"] = -1
        else:
            row["vola_label_abs"] = 0

        return row

    def featuring(self):
        print(f"featuring...\n")
        self.df_news_activos = self.df_news_activos.progress_apply(
            lambda row: self.extract(row) if not pd.isna(row.ticker) else row, axis=1
        )
        return self.df_news_activos

    def output(
        self,
        df,
        columns,
        ruta="data/outputs",
        name="output_featuring_2021_01",
        index=False,
        new_columns=None,
    ):
        df = df.reindex(columns=columns)
        df.sort_values(["date", "pk"], inplace=True)
        if new_columns:
            df.dropna(subset=new_columns, inplace=True)
        df.to_csv(f"{ruta}/{name}.csv", sep=";", index=index)
