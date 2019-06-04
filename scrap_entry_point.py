import json
import logging
import os
import time
from argparse import ArgumentParser

from crochet import setup
from kafka import KafkaConsumer
from kafka import KafkaProducer
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from common import settings as my_settings
from common.spiders import spiderthon

# gére le reactor de scrapy dans un thread différent
setup()

parser = ArgumentParser(description='Retrieving Google URL python script')

parser.add_argument("-c", "--complexity", dest="complexity", action="store", default="1",
                    help="Complexity degree for the research, higher is deeper")
parser.add_argument("-e", "--endpoint", dest="endpoint", action="store", default="localhost:8092",
                    help="Endpoint url for Kafka")
parser.add_argument("-v", "--verbosity", action="store_true", help="show debug logs")

options = parser.parse_args()

logging.basicConfig(level=logging.DEBUG)


scrapython_acq_ended = False

@inlineCallbacks
# Crawl la liste des urls de page internet donnée selon les settings dans spiderthon.py
def loop_urls(urls):
    global scrapython_acq_ended
    scrapython_acq_ended = False
    yield process.crawl(spiderthon.spiderthon, urls)
    scrapython_acq_ended = True


# Suppression de l'ancien fichier de récupération des résultats du crawl des urls fournies
# Ce fichier est en effet créé à chaque run du script,
# ou complété (et on ne veut pas que les données deux deux scripts se suivent)


results_file_exists = os.path.isfile("results.json")
if results_file_exists:
    os.remove("results.json")

# Set un consumer
consumer = KafkaConsumer(
    'urlToScrapy',
    bootstrap_servers=options.endpoint,
    group_id='scrapython',
    auto_offset_reset='earliest',  # TODO à changer ?
    value_deserializer=lambda v: json.loads(v.decode('utf-8')))

# Set un producer
producer = KafkaProducer(
    bootstrap_servers=options.endpoint,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

crawler_settings = Settings()
crawler_settings.setmodule(my_settings)

process = CrawlerProcess(settings=crawler_settings)

for message in consumer:
    message = message.value
    logging.info("### Scrapython : reception d'un message ! " + str(message))
    scrapyResults = []
    complexityLevel = 1
    if 'complexity' not in message or (int(message['complexity']) <= int(options.complexity)):
            for i in range(len(message['url'])):
                urlList = []
                urlList.append(message['url'][i])

                loop_urls(urlList)

                while (scrapython_acq_ended == False):
                    time.sleep(1)
                scrapyResult = {}
                if "results.json":  # TODO: changer le mode de sortie en stream
                    textString = ''

                    with open("results.json", 'r') as f:
                        data = json.load(f)
                        for text in data:

                            if next(iter(text)) == 'text':
                                textString = textString + " " + text['text']
                            elif next(iter(text)) == 'urls':
                                if int(options.complexity) > 1:
                                    if 'complexity' not in message:
                                        complexityLevel = 2
                                    else:
                                        complexityLevel = int(message['complexity']) + 1
                                producer.send(
                                    'urlToScrapy',
                                    value={'idBio': message['idBio'], 'url': message['url'], 'complexity': complexityLevel})

                    scrapyResult.update([('url', urlList[0]), ('content', textString)])
                    scrapyResults.append(scrapyResult)

                os.remove("results.json")

            producer.send(
                'textToNER',
                value={'idBio': message['idBio'], 'scrapyResults': scrapyResults})
