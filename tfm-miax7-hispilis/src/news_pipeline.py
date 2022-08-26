import pickle
from ast import literal_eval
from datetime import datetime

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from extractors import (
    ExtractorAbc,
    ExtractorCincoDias,
    ExtractorElPais,
    ExtractorEuropaPress,
)
from pipeline_components import (
    APIBMEHandler,
    Classifier,
    Featurer,
    Preproccesor,
    TickersNER,
)

if __name__ == "__main__":

    tqdm.pandas()

    # Fecha de extracción de los datos
    start_date = datetime(2017, 11, 22)
    end_date = start_date + pd.offsets.MonthEnd()
    end_date = end_date.date()

    end_date = datetime(2017, 11, 22)

    RUTA_BASE = "gs://tfm_aideas_datasets"
    # RUTA_BASE = 'data/outputs'

    print("####### CARGANDO DATASETS AUXILIARES")
    # df_tickers = pd.read_csv('gs://tfm_aideas_datasets/listadoempresas.csv', sep=";", index_col=0, parse_dates=["start_date", "end_date"])
    df_tickers = pd.read_csv(
        "data/inputs/listadoempresas_new.csv",
        sep=";",
        index_col=0,
        parse_dates=["start_date", "end_date"],
    )

    api_handler = APIBMEHandler(market="IBEX")
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

    result_news = None
    result_paragraph = None

    for fecha in tqdm(pd.date_range(start=start_date, end=end_date)):
        print(fecha)
        print("\n####### NOTICIAS PHASE")
        print("#STEP 1 INVOCAMOS AL SCRAPPER\n")
        # extr3 = ExtractorEuropaPress(fecha)
        # df = pd.DataFrame(extr3.extract_news())

        # extr4 = ExtractorAbc(fecha)
        # df = pd.concat([df, pd.DataFrame(extr4.extract_news())], ignore_index=True)

        # extr5 = ExtractorElPais(fecha)
        # df = pd.concat([df, pd.DataFrame(extr5.extract_news())], ignore_index=True)

        # extr6 = ExtractorCincoDias(fecha)
        # df = pd.concat([df, pd.DataFrame(extr6.extract_news())], ignore_index=True)
        # df.dropna(subset=["date", "title", "body"], inplace=True)

        fecha = fecha.strftime("%Y%m%d")
        # outdir = f"{RUTA_BASE}/{fecha}"
        # if not os.path.exists(outdir):
        #     os.mkdir(outdir)

        # df.sort_values(by="date", inplace=True)
        # df.to_csv(
        #     f"{RUTA_BASE}/{fecha}/output{fecha}.csv",
        #     sep=";",
        #     index=False,
        # )

        print("#STEP 2 LIMPIANDO EL BODY\n")
        df = pd.read_csv(
            f"{RUTA_BASE}/{fecha}/output{fecha}.csv",
            sep=";",
            parse_dates=["date"],
        )
        p = Preproccesor(df, bigram=False)
        df = p.clean_special_character()
        df.body.replace("", np.nan, inplace=True)
        df.body = df.body.str.replace("\ufeff", "")
        df.dropna(subset=["date", "title", "body"], inplace=True)
        df.to_csv(
            f"{RUTA_BASE}/{fecha}/output_clean_{fecha}.csv",
            sep=";",
            index=False,
        )

        print("#STEP 3 INVOCAMOS AL NER\n")
        # df = pd.read_csv('gs://tfm_aideas_datasets/output202101.csv', sep=";")
        df = pd.read_csv(
            f"{RUTA_BASE}/{fecha}/output_clean_{fecha}.csv",
            sep=";",
            parse_dates=["date"],
        )

        # USANDO EL NER
        ner = TickersNER(df, df_tickers)
        ner.get_tickers()
        columns = [
            "pk",
            "date",
            "title",
            "url",
            "body",
            "tags",
            "resource",
            "ticker",
            "ticker_name",
            "sector",
            "subsector",
            "ticker_freq",
            "ticker_first",
            "ticker_title",
            "tickers",
            "entities",
        ]

        assert ner.df.columns.shape[0] == len(columns)
        # Generamos el csv sin idx ya que se han duplicado
        ner.output(
            columns,
            f"{RUTA_BASE}/{fecha}",
            f"output_ner_{fecha}",
            False,
        )

        print("#STEP 4 INVOCAMOS AL CLASSIFIER\n")
        # df = pd.read_csv('gs://tfm_aideas_datasets/output202101.csv', sep=";")
        df = pd.read_csv(
            f"{RUTA_BASE}/{fecha}/output_ner_{fecha}.csv",
            sep=";",
            index_col=0,
            parse_dates=["date"],
        )

        classifier = Classifier(df)

        classifier.get_topics()

        classifier.get_sector_in_topics()

        classifier.get_intensidad()

        columns = df.columns.tolist()
        columns.extend(
            [
                "topic_classifier",
                "topics",
                "finanzas_prob",
                "sector_in_topics",
                "intensidad",
            ]
        )

        assert classifier.df.columns.shape[0] == len(columns)

        classifier.df.sort_values(["date", "pk"], inplace=True)

        classifier.output(
            columns,
            f"{RUTA_BASE}/{fecha}",
            f"output_classifier_{fecha}",
            True,
        )

        print("#STEP 5 INVOCAMOS AL FEATURER\n")
        df_news_activos = pd.read_csv(
            f"{RUTA_BASE}/{fecha}/output_classifier_{fecha}.csv",
            sep=";",
            index_col=0,
            parse_dates=["date"],
        )

        columns = df_news_activos.columns.tolist()

        featurer = Featurer(
            df_ibex, data_opens, data_closes, df_news_activos, df_tickers, window_size
        )

        df = featurer.featuring()

        new_cols = [
            "rd_activo_news",
            "rd_bench_news",
            "alpha_exante",
            "alpha_exante_Q1",
            "alpha_exante_below_Q1",
            "alpha_exante_Q3",
            "alpha_exante_above_Q3",
            "vola20_exante",
            "vola20_expost",
            "vola20_antepost_pct_change",
            "vola20_umbral_Q1",
            "vola20_umbral_abs_Q1",
            "vola20_umbral_Q3",
            "vola20_umbral_abs_Q3",
            "vola_label",
            "vola_label_abs",
        ]

        columns = [*columns, *new_cols]

        assert len(columns) == df.columns.shape[0]

        featurer.output(
            df,
            columns,
            f"{RUTA_BASE}/{fecha}",
            f"output_featuring_{fecha}",
            True,
            new_cols,
        )

        print("####### PARRAFOS PHASE")
        print("#STEP 6 PARRAFOS SPLIT AND HIDING ENTITIES\n")
        df_paragraph = pd.read_csv(
            f"{RUTA_BASE}/{fecha}/output_featuring_{fecha}.csv",
            sep=";",
            index_col=0,
            parse_dates=["date"],
            converters={"entities": literal_eval},
        )
        p = Preproccesor(df_paragraph, bigram=False)
        df_paragraph = p.paragraphe()
        df_paragraph = p.hide_entities(df_paragraph)

        # eliminamos las generales
        # df_paragraph.to_csv(f"{RUTA_BASE}/{fecha}/output_{fecha}_paragraph.csv", sep=";", index=False)
        df_paragraph.body.replace("", np.nan, inplace=True)
        df_paragraph["body_no_entities"].replace("", np.nan, inplace=True)
        df_paragraph.dropna(subset=["body", "body_no_entities"], inplace=True)
        df_paragraph.to_csv(
            f"{RUTA_BASE}/{fecha}/output_{fecha}_paragraph.csv",
            sep=";",
            index=True,
        )

        print("#STEP 7 INVOCAMOS AL CLASSIFIER PARAGRAPH\n")
        df = pd.read_csv(
            f"{RUTA_BASE}/{fecha}/output_{fecha}_paragraph.csv",
            sep=";",
            index_col=0,
            parse_dates=["date"],
        )

        classifier = Classifier(df)

        classifier.get_topics()

        classifier.get_sector_in_topics()

        classifier.get_intensidad()

        columns = df.columns.tolist()
        # columns = columns.append(['topic', 'topics', 'finanzas_prob', 'sector_in_topics', 'intensidad'])
        classifier.df.sort_values(["date", "pk_paragraph"], inplace=True)

        assert classifier.df.columns.shape[0] == len(columns)

        classifier.output(
            columns,
            f"{RUTA_BASE}/{fecha}",
            f"output_classifier_{fecha}_paragraph",
            True,
        )

        print("#STEP 8 RECUPERAMOS FEATURES DEL CLASSIFIER A NIVEL DE NOTICIA\n")
        df = pd.read_csv(
            f"{RUTA_BASE}/{fecha}/output_classifier_{fecha}_paragraph.csv",
            sep=";",
            index_col=0,
            parse_dates=["date"],
        )

        columns = df.columns.tolist()

        df_classifier = pd.read_csv(
            f"{RUTA_BASE}/{fecha}/output_classifier_{fecha}.csv",
            sep=";",
            index_col=0,
            parse_dates=["date"],
        )

        df = df.progress_apply(
            lambda row: classifier.get_news_properties(df_classifier, row), axis=1
        )

        new_cols = [
            "topic_classifier_news",
            "topics_news",
            "finanzas_prob_news",
            "sector_in_topics_news",
            "intensidad_news",
        ]

        columns = [*columns, *new_cols]

        featurer.output(
            df,
            columns,
            f"{RUTA_BASE}/{fecha}",
            f"output_featuring_{fecha}_paragraph",
            True,
            new_cols,
        )

    for fecha in tqdm(pd.date_range(start=start_date, end=end_date)):
        fecha = fecha.strftime("%Y%m%d")
        df = pd.read_csv(
            f"{RUTA_BASE}/{fecha}/output_featuring_{fecha}.csv",
            sep=";",
            parse_dates=["date"],
        )
        if not result_news is None:
            result_news = pd.concat([result_news, df])
        else:
            result_news = df

        df = pd.read_csv(
            f"{RUTA_BASE}/{fecha}/output_featuring_{fecha}_paragraph.csv",
            sep=";",
            parse_dates=["date"],
        )

        if not result_paragraph is None:
            result_paragraph = pd.concat([result_paragraph, df])
        else:
            result_paragraph = df

    result_news.sort_values(by=["date", "pk"], inplace=True)

    result_news.to_csv(
        f"{RUTA_BASE}/dataset_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_news.csv",
        sep=";",
        index=False,
    )

    result_paragraph["finanzas_tag_s_n"] = ""
    result_paragraph["impacto_tag_s_n"] = ""
    result_paragraph["topic_tag_news"] = ""
    result_paragraph["finanzas_tag_news_s_n"] = ""
    result_paragraph["impacto_tag_news_s_n"] = ""

    result_paragraph.sort_values(by=["date", "pk_paragraph"], inplace=True)

    result_paragraph.to_csv(
        f"{RUTA_BASE}/dataset_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_paragraph.csv",
        sep=";",
        index=False,
    )
