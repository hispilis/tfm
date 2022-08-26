import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from .extractor import Extractor
from .news import New


class ExtractorEuropaPress(Extractor):
    def __init__(self, date_news):
        self.url_base = "https://www.europapress.es"
        super().__init__("", "EUROPAPRESS", date_news)

    @Extractor.date_news.setter
    def date_news(self, date_news):
        self._date_news = date_news
        self.URL = [
            f"{self.url_base}/sitemap/{self.date_news.year}/{self.date_news.strftime('%Y-%m-%d')}/"
        ]

    def get_urls(self):
        try:
            page = requests.get(self.URL[-1], headers=self.headers)
            if page.status_code == 200:
                soup = BeautifulSoup(page.content, "html.parser", from_encoding="utf-8")
                if len(soup.findAll("a", class_="siguiente")) > 0:
                    self.URL.append(soup.find("a", class_="siguiente").attrs["href"])
                    self.get_urls()
        except Exception as e:  # Capturamos la excepción. Esto es lo que se ejecuta cuando salta la excepción
            print("Caught exception:", e)

    def extract_news_impl(self, soup):
        results = soup.find("ul", class_="listadohistorico")
        article_elements = results.find_all("li")
        result = []
        for article in article_elements:
            title_text = article.find("a")
            title_text = title_text.text.strip()
            title_text = title_text.replace("\n", " ").replace("\r", " ")
            title_text = re.sub(" +", " ", title_text)
            url = article.find("a")["href"]
            body, date_new, tags_list = self.extraer_body(url)
            if body:
                body_text = body.strip()
            else:
                body_text = ""
            if date_new:
                if date_new.find("Publicado") > -1:
                    if date_new.find("+01") > -1:
                        date_new = datetime.strptime(
                            date_new.strip(), "Publicado %d/%m/%Y %H:%M:%S +01:00CET"
                        )
                    elif date_new.find("+02") > -1:
                        date_new = datetime.strptime(
                            date_new.strip(), "Publicado %d/%m/%Y %H:%M:%S +02:00CET"
                        )
                    else:
                        date_new = datetime.strptime(
                            date_new.strip(), "Publicado %d/%m/%Y %H:%M"
                        )
                else:
                    date_new = datetime.strptime(
                        date_new.strip(), "Actualizado %d/%m/%Y %H:%M"
                    )
            else:
                print(date_new, url)
                date_new = datetime(1900, 1, 1)
            n = New(date_new, title_text, body_text, tags_list, self.resource, url)
            result.append(n)
        return result

    def extraer_body(self, url):
        try:
            page = requests.get(url, headers=self.headers)
            if page.status_code == 200:
                soup = BeautifulSoup(page.content, "html.parser", from_encoding="utf-8")
                body_ = soup.find("div", id="CuerpoNoticiav2")
                tags_ = soup.find("div", id="divEntidadesNoticiav2")
                if tags_:
                    tags_list = [tag.text.strip() for tag in tags_.find_all("li")]
                else:
                    tags_ = soup.find("div", id="divEntidadesNoticia")
                    if tags_:
                        tags_list = [tag.text.strip() for tag in tags_.find_all("li")]
                    else:
                        tags_list = None
                if body_:
                    result = "".join(
                        [
                            p.text.strip() + "<EOL>"
                            for p in body_.find_all("p")
                            if p and p.text != ""
                        ]
                    )
                else:
                    body_ = soup.find("div", id="CuerpoNoticia")
                    if body_:
                        result = "".join(
                            [
                                p.text.strip() + "<EOL>"
                                for p in body_.find_all("p")
                                if p and p.text != ""
                            ]
                        )
                    else:
                        body_ = soup.find("div", id="NoticiaPrincipal")
                        if body_:
                            result = "".join(
                                [
                                    p.text.strip() + "<EOL>"
                                    for p in body_.find_all("p")
                                    if p and p.text != ""
                                ]
                            )
                        else:
                            result = None
                if result is not None:
                    result = result.removesuffix("<EOL>")
                    result = result.replace("\n", " ")
                    result = result.replace("\r", " ")
                    result = re.sub(" +", " ", result)
                    if len(body_.find_all("p")) == 0:
                        result = body_.text.strip()
                date_new = soup.find("div", class_="FechaPublicacionNoticia")
                if date_new:
                    date_new = date_new.text.strip()
                return result, date_new, tags_list
            else:
                return (None, None, None)
        except Exception as e:  # Capturamos la excepción. Esto es lo que se ejecuta cuando salta la excepción
            print("Caught exception:", e)
            return (None, None, None)
