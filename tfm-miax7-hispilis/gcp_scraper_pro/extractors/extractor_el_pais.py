import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from .extractor import Extractor
from .news import New


class ExtractorElPais(Extractor):
    def __init__(self, date_news):
        self.url_base = "https://elpais.com"
        super().__init__("", "ELPAIS", date_news)

    @Extractor.date_news.setter
    def date_news(self, date_news):
        self._date_news = date_news
        self.URL = [
            f"{self.url_base}/hemeroteca/{self.date_news.strftime('%Y-%m-%d')}/"
        ]

    def get_urls(self):
        try:
            page = requests.get(self.URL[-1], headers=self.headers)
            if page.status_code == 200:
                soup = BeautifulSoup(page.content, "html.parser", from_encoding="utf-8")
                if soup.findAll("a", class_="btn btn-lg btn-5")[-1].next == "Siguiente":
                    url_end = soup.findAll("a", class_="btn btn-lg btn-5")[-1]["href"]
                    self.URL.append(f"{self.url_base}{url_end}")
                    self.get_urls()
        except Exception as e:  # Capturamos la excepción. Esto es lo que se ejecuta cuando salta la excepción
            print("Caught exception:", e)

    def monthToNum(self, shortMonth):
        return {
            "ene": 1,
            "feb": 2,
            "mar": 3,
            "abr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "ago": 8,
            "sep": 9,
            "sept": 9,
            "oct": 10,
            "nov": 11,
            "dic": 12,
        }[shortMonth]

    def extract_news_impl(self, soup):
        article_elements = soup.find_all("article")
        result = []
        for article in article_elements:
            title_text = article.find("h2").find("a").text.strip()
            title_text = title_text.replace("\n", " ").replace("\r", " ")
            title_text = re.sub(" +", " ", title_text)
            url = article.find("h2").find("a")["href"]
            url = self.url_base + url
            body_text, tags_list = self.extraer_body(url)
            date_new = article.find("time").find("a")["data-date"]
            if article.find("time").find("a").text.find("UTC") > 0:
                arr_datetime = article.find("time").find("a").text.strip().split("-")
                arr_date = arr_datetime[0].split(" ")
                m = self.monthToNum(arr_date[1])
                arr_date[1] = f"{m}"
                s_date = " ".join(arr_date)
                arr_datetime[0] = s_date
                s_datetime = " ".join(arr_datetime)
                date_new = datetime.strptime(s_datetime.strip(), "%d %m %Y %H:%MUTC")
            elif date_new and date_new.find(".") > 0:
                date_new = datetime.strptime(date_new.strip(), "%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                date_new = datetime.strptime(date_new.strip(), "%Y-%m-%dT%H:%M:%SZ")
            n = New(date_new, title_text, body_text, tags_list, self.resource, url)
            result.append(n)
        return result

    def extraer_body(self, url):
        try:
            page = requests.get(url, headers=self.headers)
            if page.status_code == 200:
                soup = BeautifulSoup(page.content, "html.parser", from_encoding="utf-8")
                body_ = soup.find("div", {"data-dtm-region": "articulo_cuerpo"})
                tags_ = soup.find(
                    "section", {"data-dtm-region": "articulo_archivado-en"}
                )
                if tags_:
                    tags_list = [tag.text.strip() for tag in tags_.find_all("li")]
                else:
                    tags_ = soup.find("div", id="mod_archivado")
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
                    body_ = soup.find("div", id="ctn_article_body")
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
