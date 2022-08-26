import os
from datetime import datetime, timedelta

import pandas as pd
from extractors import (ExtractorAbc, ExtractorCincoDias, ExtractorElPais,
                        ExtractorEuropaPress)

from scrapy_poc.tfm_scrapy.run_scraper import Scraper

# Fecha de extracción de los datos
date_news = datetime(2021, 12, 1)

if __name__ == "__main__":               

    extr4 = ExtractorAbc(date_news)
    extr4.get_urls()
    s = Scraper()    
    s.run_spiders(extr4.URL,extr4.resource)    
