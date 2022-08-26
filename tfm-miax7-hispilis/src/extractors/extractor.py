import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


class Extractor:
    def __init__(self, URL, resource, date_news):
        self.URL = URL
        self.resource = resource
        self.date_news = date_news
        self.headers = {
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36",
        }

    @property
    def date_news(self):
        return self._date_news

    @date_news.setter
    def date_news(self, date_news):
        pass

    def get_urls(self):
        """
        Metodo que obtiene las url cuando hay multipágina
        Se sobreescribe por cada hijo que extienda esta clase
        """
        pass

    def extract_news(self):
        # print(f"\nEXTRACTOR {self.resource} {self.date_news.date()}\n")
        self.get_urls()
        response = []
        for url in tqdm(self.URL, leave=False):
            try:
                page = requests.get(url, headers=self.headers)
                if page.status_code == 200:
                    soup = BeautifulSoup(
                        page.content, "html.parser", from_encoding="utf-8"
                    )
                    result = self.extract_news_impl(soup)
                    response.extend([ob.__dict__ for ob in result])
                else:
                    response.append(None)
            except Exception as e:  # Capturamos la excepción. Esto es lo que se ejecuta cuando salta la excepción
                print(url)
                print("Caught exception:", e)
                response.append(None)
        return response

    def extract_news_impl(self, soup):
        """
        Metodo que devuelve una lista de noticias.
        Se sobreescribe por cada hijo que extienda esta clase con el webscrapping de la página concreta
        """
        pass
