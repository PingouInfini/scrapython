import scrapy
from bs4 import BeautifulSoup
from common.items import ScrapythonItem


class spiderthon(scrapy.Spider):
    name = "spiderthon"

    def __init__(self, filename=None):
        print(filename)
        self.start_urls = filename

    # Méthode qui parse chaque url à crawler, fournie ci-dessus
    def parse(self, response):

        for (h_n) in response.xpath('//body//text()').getall():
            soup = BeautifulSoup(h_n, 'html.parser')
            just_text = soup.get_text().strip()
            if just_text == '':
                continue
            else:
                yield ScrapythonItem(text=just_text)

        for href in response.xpath('//a/@href').getall():
            yield ScrapythonItem(url=href)

        for img in response.xpath('//img/@src').getall():
            # urlimg = response.urljoin(img)
            # urlparsed = urlparse(img).geturl()
            yield ScrapythonItem(img_url=img)

            # Décommenter si l'on veut télécharger les pages HTML associées aux URLs
            # page = response.url.split("/")[-2] .re("^(http|https):\/\/")
            # filename = 'quotes-%s.html' % page
            # with open(filename, 'wb') as f:
            #     f.write(response.body)
            # self.log('Saved file %s' % filename)
