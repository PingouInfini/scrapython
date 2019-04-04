import scrapy
from scrapy.crawler import CrawlerProcess
from scrapython.spiders import spiderthon
from scrapy.settings import Settings
from scrapython import settings as my_settings


crawler_settings = Settings()
crawler_settings.setmodule(my_settings)

process = CrawlerProcess(settings = crawler_settings)

process.crawl(spiderthon.spiderthon)
process.start()





