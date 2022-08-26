from datetime import datetime, timedelta

import pandas as pd
from tqdm import tqdm

from extractors import (
    ExtractorAbc,
    ExtractorCincoDias,
    ExtractorElPais,
    ExtractorEuropaPress,
)

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
    
    tqdm.pandas()
    
    RUTA_BASE = 'gs://tfm_aideas_datasets'

    for fecha in tqdm(pd.date_range(start=start_date, end=end_date)[0:]):
        print(fecha)
        extr3 = ExtractorEuropaPress(fecha)
        df = pd.DataFrame(extr3.extract_news())

        extr4 = ExtractorAbc(fecha)
        df = pd.concat([df, pd.DataFrame(extr4.extract_news())], ignore_index=True)

        extr5 = ExtractorElPais(fecha)
        df = pd.concat([df, pd.DataFrame(extr5.extract_news())], ignore_index=True)

        extr6 = ExtractorCincoDias(fecha)
        df = pd.concat([df, pd.DataFrame(extr6.extract_news())], ignore_index=True)
        df.dropna(subset=["date", "title", "body"], inplace=True)

        fecha = fecha.strftime("%Y%m%d")
        # outdir = f"{RUTA_BASE}/{fecha}"
        # if not os.path.exists(outdir):
        #     os.mkdir(outdir)

        df.sort_values(by="date", inplace=True)
        df.to_csv(
            f"{RUTA_BASE}/{fecha}/output{fecha}.csv",
            sep=";",
            index=False,
        )
           

    return 'OK'
