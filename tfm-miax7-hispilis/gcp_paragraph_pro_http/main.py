from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from tqdm import tqdm

import pickle

import api_bme_handler as abh

from news_featuring import Featurer
from news_classifier import Classifier
from news_preprocessing import Preproccesor

import base64
import functions_framework

def func_get_news_properties(df_news, row):
    row["intensidad_news"] = df_news.loc[row.name].intensidad
    row["topic_news"] = df_news.loc[row.name].topic_classifier
    return row

@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'fecha' in request_json:
        fecha = datetime.strptime(request_json['fecha'], '%d/%m/%Y')
        start_date = fecha
        end_date = fecha
    elif request_args and 'fecha' in request_args:
        fecha = datetime.strptime(request_args['fecha'], '%d/%m/%Y')
        start_date = fecha
        end_date = fecha
    elif request_json and 'start_date' in request_json and 'end_date' in request_json:
        start_date = datetime.strptime(request_json['start_date'], '%d/%m/%Y')
        end_date = datetime.strptime(request_json['end_date'], '%d/%m/%Y')        
    elif request_args and 'start_date' in request_args and 'end_date' in request_args:
        start_date = datetime.strptime(request_args['start_date'], '%d/%m/%Y')
        end_date = datetime.strptime(request_args['end_date'], '%d/%m/%Y')
    else:
        fecha = datetime.today() - timedelta(days=1)
        start_date = fecha
        end_date = fecha
    
    RUTA_BASE = 'gs://tfm_aideas_datasets'
    window_size = 20

    df_tickers = pd.read_csv(
        f"{RUTA_BASE}/listadoempresas_new.csv",
        sep=";",
        index_col=0,
        parse_dates=["start_date", "end_date"],
    )

    api_handler = abh.APIBMEHandler(market="IBEX")
    try:
        with open(f"{RUTA_BASE}/ibex.pkl", "rb") as f:
            df_ibex = pickle.load(f)
    except:
        df_ibex = api_handler.get_benchmark()
        df_ibex.to_pickle(f"{RUTA_BASE}/ibex.pkl")

    try:
        with open(f"{RUTA_BASE}/activos_opens.pkl", "rb") as f:
            data_opens = pickle.load(f)
        with open(f"{RUTA_BASE}/activos_closes.pkl", "rb") as f:
            data_closes = pickle.load(f)
    except:
        data_opens, data_closes = api_handler.get_opens_closes_data()
        data_opens.to_pickle(f"{RUTA_BASE}/activos_opens.pkl")
        data_closes.to_pickle(f"{RUTA_BASE}/activos_closes.pkl")

    for fecha in tqdm(pd.date_range(start=start_date, end=end_date)[0:]):
        print(fecha)
        df_paragraph = pd.read_csv(
            f"{RUTA_BASE}/{fecha.strftime('%Y%m%d')}/output_featuring_{fecha.strftime('%Y%m%d')}.csv",
            sep=";",
            index_col=0,
            parse_dates=["date"],
        )
        p = Preproccesor(df_paragraph, bigram=False)
        df_paragraph = p.paragraphe()
        df_paragraph = p.hide_entities(df_paragraph)
                
        df_paragraph = df_paragraph.dropna()
        df_paragraph.to_csv(
            f"{RUTA_BASE}/{fecha.strftime('%Y%m%d')}/output_{fecha.strftime('%Y%m%d')}_paragraph.csv",
            sep=";",
            index=True,
        )

        df = pd.read_csv(
            f"{RUTA_BASE}/{fecha.strftime('%Y%m%d')}/output_{fecha.strftime('%Y%m%d')}_paragraph.csv",
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
        classifier.output(
            columns,
            f"{RUTA_BASE}/{fecha.strftime('%Y%m%d')}",
            f"output_classifier_{fecha.strftime('%Y%m%d')}_paragraph",
            True,
        )
        
        df_news_activos = pd.read_csv(
            f"{RUTA_BASE}/{fecha.strftime('%Y%m%d')}/output_classifier_{fecha.strftime('%Y%m%d')}_paragraph.csv",
            sep=";",
            index_col=0,
            parse_dates=["date"],
        )

        columns = df_news_activos.columns.tolist()[:]

        featurer = Featurer(
            df_ibex,
            data_opens,
            data_closes,
            df_news_activos,
            df_tickers,
            window_size,
            paragraphs=True,
        )

        df = df_news_activos
        df_classifier = pd.read_csv(
            f"{RUTA_BASE}/{fecha.strftime('%Y%m%d')}/output_classifier_{fecha.strftime('%Y%m%d')}.csv",
            sep=";",
            index_col=0,
            parse_dates=["date"],
        )
        df = df.progress_apply(
            lambda row: func_get_news_properties(df_classifier, row), axis=1
        )

        new_cols = ["intensidad_news", "topic_news"]

        columns = [*columns, *new_cols]

        featurer.output(
            df,
            columns,
            f"{RUTA_BASE}/{fecha.strftime('%Y%m%d')}",
            f"output_featuring_{fecha.strftime('%Y%m%d')}_paragraph",
            True,
        )

    return 'OK'
