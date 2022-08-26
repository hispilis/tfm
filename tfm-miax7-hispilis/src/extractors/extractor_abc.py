import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from .extractor import Extractor
from .news import New


class ExtractorAbc(Extractor):
    def __init__(self, date_news):
        self.url_base = "https://www.abc.es"
        super().__init__("", "ABC", date_news)

    @Extractor.date_news.setter
    def date_news(self, date_news):
        self._date_news = date_news
        self.URL = [
            f"{self.url_base}/hemeroteca/dia-{self.date_news.strftime('%d-%m-%Y')}/pagina-1?or=1&nres=20"
        ]

    def get_urls(self):
        try:
            page = requests.get(self.URL[-1], headers=self.headers)
            if page.status_code == 200:
                soup = BeautifulSoup(page.content, "html.parser", from_encoding="utf-8")
                page_num = int(soup.find(id="summery").findAll("strong")[-1].text)
                for i in range(2, page_num + 1):
                    self.URL.append(
                        f"{self.url_base}/hemeroteca/dia-{self.date_news.strftime('%d-%m-%Y')}/pagina-{i}?or=1&nres=20"
                    )
        except Exception as e:  # Capturamos la excepción. Esto es lo que se ejecuta cuando salta la excepción
            print("Caught exception:", e)

    def extract_news_impl(self, soup):
        results = soup.find(id="results-content")
        # 20 artículos como máximo
        article_elements = results.find_all("li")[:20]
        result = []
        for article in article_elements:
            title_text = article.find("a").text
            title_text = title_text.strip()
            title_text = title_text.replace("\n", " ").replace("\r", " ")
            title_text = re.sub(" +", " ", title_text)
            url = article.find("a")["href"]
            body_new, tags_list = self.extraer_body(url)
            if body_new:
                body_text = body_new.strip()
                body_text = body_text.encode("utf-8", "ignore").decode(
                    "utf-8", "replace"
                )
            elif body_new == "" or body_new == None:
                body_text = None
            else:
                body_new = list(article.find("p").descendants)
                body_text = body_new[-1].strip().replace("\n", " ").replace("\r", " ")
                body_text = re.sub(" +", " ", body_text)
                body_text = body_text.encode("utf-8", "ignore").decode(
                    "utf-8", "replace"
                )
            date_new = article.find("span", class_="date")
            if date_new != None:
                date_new = datetime.strptime(date_new.text.strip(), "%d/%m/%Y %H:%M:%S")
            n = New(date_new, title_text, body_text, tags_list, self.resource, url)
            result.append(n)
        return result

    def extraer_body(self, url):
        try:
            page = requests.get(url, headers=self.headers)
            if page.status_code == 200:
                soup = BeautifulSoup(page.content, "html.parser", from_encoding="utf-8")
                body_ = soup.find("span", class_="cuerpo-texto")
                tags_ = soup.find("aside", class_="modulo temas")
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
                    result = re.sub(" +", " ", result)
                else:
                    result = None
                return result, tags_list
            else:
                return None, None
        except Exception as e:  # Capturamos la excepción. Esto es lo que se ejecuta cuando salta la excepción
            print(url)
            print("Caught exception:", e)
            return None, None
