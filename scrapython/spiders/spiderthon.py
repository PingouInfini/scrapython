import scrapy
from scrapython.items import ScrapythonItem
import json


class spiderthon(scrapy.Spider):
    name = "spiderthon"
    start_urls = [
        'http://quotes.toscrape.com/page/1/',
        'http://quotes.toscrape.com/page/2/',
    ]
    # Méthode qui parse chaque url à crawler, fournie ci-dessus
    def parse(self, response):

        for quote in response.css('div.quote'):
            print("###############   BEFORE YIELD    ####################")
            yield {
                'text' : quote.css('span.text::text').get(),
                'author' : quote.css('small.author::text').get(),
                'tags' :quote.css('div.tags a.tag::text').getall(),
            }

            # page = response.url.split("/")[-2]
            # filename = 'quotes-%s.html' % page
            # with open(filename, 'wb') as f:
            #     f.write(response.body)
            # self.log('Saved file %s' % filename)

            # return item
