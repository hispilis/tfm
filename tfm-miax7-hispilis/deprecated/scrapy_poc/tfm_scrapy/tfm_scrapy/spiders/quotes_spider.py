import scrapy
import os

class QuotesSpider(scrapy.Spider):
    name = "quotesspider"

    def __init__(self, urls, resource, *a, **kw):
        self.urls = urls
        self.resource = resource
        super(QuotesSpider, self).__init__(*a, **kw)

    def start_requests(self): 
        print(self.urls)               
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        page = response.url.split("/")[-2]
        #filename = f'europapress-{page}.html'
        filename = f'{response.url}'
        filename = filename.split('/')
        directorio = f'/repo/{self.resource}/{filename[4]}/'
        fichero = filename[5].split('?')[0]        
        filename = f'{directorio}{fichero}.html'
        print(filename)
        os.makedirs(os.path.dirname(directorio), exist_ok=True)
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log(f'Saved file {filename}')