import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from .extractor import Extractor
from .news import New


class ExtractorCincoDias(Extractor):
    def __init__(self, date_news):
        self.url_base = "https://cincodias.elpais.com"
        super().__init__("", "CINCODIAS", date_news)

    @Extractor.date_news.setter
    def date_news(self, date_news):
        self._date_news = date_news
        self.URL = [f"{self.url_base}/tag/fecha/{date_news.strftime('%Y%m%d')}"]

    def get_urls(self):
        try:
            page = requests.get(self.URL[-1], headers=self.headers)
            if page.status_code == 200:
                soup = BeautifulSoup(page.content, "html.parser", from_encoding="utf-8")
                if len(soup.findAll("li", class_="paginacion-siguiente activo")) > 0:
                    self.URL.append(
                        soup.findAll("li", class_="paginacion-siguiente activo")[
                            0
                        ].find("a")["href"]
                    )
                    self.get_urls()
        except Exception as e:  # Capturamos la excepción. Esto es lo que se ejecuta cuando salta la excepción
            print("Caught exception:", e)

    def extract_news_impl(self, soup):
        article_elements = soup.find_all("article")
        result = []
        for article in article_elements:
            title_text = (
                article.find("h2")
                .find("a")
                .text.strip()
                .replace("\n", " ")
                .replace("\r", " ")
            )
            title_text = re.sub(" +", " ", title_text)
            url = article.find("h2").find("a")["href"]
            url = self.url_base + url
            body_text, tags_list = self.extraer_body(url)
            date_new = article.find("time")["datetime"]
            date_new = datetime.strptime(date_new.strip(), "%Y-%m-%dT%H:%M:%S")
            n = New(date_new, title_text, body_text, tags_list, self.resource, url)
            result.append(n)
        return result

    def extraer_body(self, url):
        try:
            page = requests.get(url, headers=self.headers)
            if page.status_code == 200:
                soup = BeautifulSoup(page.content, "html.parser", from_encoding="utf-8")
                body_ = soup.find("div", id="cuerpo_noticia")
                tags_ = soup.find("div", id="articulo-tags")
                if tags_:
                    tags_list = [tag.text.strip() for tag in tags_.find_all("li")]
                else:
                    tags_list = None
                if body_:
                    result = "".join(
                        [
                            p.text.strip() + "<EOL>"
                            for p in body_.find_all("p")
                            if p.text != ""
                        ]
                    )
                    result = result.replace("\n", " ")
                    result = result.replace("\r", " ")
                    result = result.removesuffix("<EOL>")
                    result = result.replace("\xa0", " ")
                    result = re.sub(" +", " ", result)
                else:
                    result = None
                return result, tags_list
            else:
                return None, None
        except Exception as e:  # Capturamos la excepción. Esto es lo que se ejecuta cuando salta la excepción
            print("Caught exception:", e)
            return None, None
