import scrapy
from scrapython.items import ScrapythonItem
from bs4 import BeautifulSoup

class spiderthon(scrapy.Spider):
    name = "spiderthon"
    start_urls = [
        'http://rosecitron.fr/2019/03/20/cookies-veganes-chocolat-praline/'
    ]

    # Méthode qui parse chaque url à crawler, fournie ci-dessus
    def parse(self, response):

        for (h_n) in response.xpath('//h1 | //h2 | //h3 | //h4 | //h5 | //h6 ').getall():
            soup = BeautifulSoup(h_n, 'html.parser')
            just_text = soup.get_text()
            yield ScrapythonItem(title=just_text)

        for (txt) in response.xpath('//p').getall():
            soup = BeautifulSoup(txt, 'html.parser')
            just_text = soup.get_text()
            yield ScrapythonItem(text=just_text)

        for href in response.xpath('//a/@href').getall():
            yield ScrapythonItem(urls=href)

        page = response.url.split("/")[-2]
        filename = 'quotes-%s.html' % page
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)


