from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from tqdm import tqdm

from news_preprocessing import Preproccesor
from news_tickers_ner import TickersNER

import base64
import functions_framework

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

    df_tickers = pd.read_csv(
        f"{RUTA_BASE}/listadoempresas_new.csv",
        sep=";",
        index_col=0,
        parse_dates=["start_date", "end_date"],
    )

    for fecha in tqdm(pd.date_range(start=start_date, end=end_date)[0:]):
        print(fecha)
        df = pd.read_csv(
                f"{RUTA_BASE}/{fecha.strftime('%Y%m%d')}/output{fecha.strftime('%Y%m%d')}.csv",
                sep=";",
                parse_dates=["date"],
            )
        p = Preproccesor(df, bigram=False)
        df = p.clean_special_character()
        df["body"].replace("", np.nan, inplace=True)
        df.body = df.body.str.replace("\ufeff", "")
        df = df.dropna()
        df.to_csv(
            f"{RUTA_BASE}/{fecha.strftime('%Y%m%d')}/output_clean_{fecha.strftime('%Y%m%d')}.csv",
            sep=";",
            index=False,
        )

        df = pd.read_csv(
            f"{RUTA_BASE}/{fecha.strftime('%Y%m%d')}/output_clean_{fecha.strftime('%Y%m%d')}.csv",
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
        # Generamos el csv sin idx ya que se han duplicado
        ner.output(
            columns,
            f"{RUTA_BASE}/{fecha.strftime('%Y%m%d')}",
            f"output_ner_{fecha.strftime('%Y%m%d')}",
            False,
        )

    return 'OK'
