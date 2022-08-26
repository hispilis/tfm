from scraper.tfm_scrapy.tfm_scrapy.spiders.quotes_spider import QuotesSpider
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

import os

class Scraper:
    def __init__(self):
        # settings_file_path = 'scraper.tfm_scrapy.tfm_scrapy.settings' # The path seen from root, ie. from main.py
        # os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)        
        self.process = CrawlerProcess({
            'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36',
            'FEED_FORMAT': 'csv',
            'FEED_URI': 'output.csv',
            'DEPTH_LIMIT': 2,
            'CLOSESPIDER_PAGECOUNT': 3})
        self.spider = QuotesSpider # The spider you want to crawl

    def run_spiders(self,urls,resource):
        print(urls)
        self.process.crawl(self.spider,urls,resource)
        self.process.start()  # the script will block here until the crawling is finished