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



#recuperation variable d'env
kafka_endpoint = str(os.environ['KAFKA_IP']) + ":" + str(os.environ['KAFKA_PORT'])
topic_in=str(os.environ['TOPIC_IN'])
topic_out=str(os.environ['TOPIC_OUT'])
complexity= int(os.environ['COMPLEXITY'])
debug_level=os.environ["DEBUG"]


# gére le reactor de scrapy dans un thread différent
setup()


logging.basicConfig(level=logging.INFO)
if debug_level == "DEBUG":
       logging.basicConfig(level=logging.DEBUG)
elif debug_level == "INFO":
        logging.basicConfig(level=logging.INFO)
elif debug_level == "WARNING":
    logging.basicConfig(level=logging.WARNING)
elif debug_level == "ERROR":
    logging.basicConfig(level=logging.ERROR)
elif debug_level == "CRITICAL":
    logging.basicConfig(level=logging.CRITICAL)


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
    topic_in,
    bootstrap_servers=[kafka_endpoint],
    group_id='scrapython',
    auto_offset_reset='latest',  # TODO à changer ?
    value_deserializer=lambda v: json.loads(v.decode('utf-8')))

# Set un producer
producer = KafkaProducer(
    bootstrap_servers=[kafka_endpoint],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

crawler_settings = Settings()
crawler_settings.setmodule(my_settings)

process = CrawlerProcess(settings=crawler_settings)

for message in consumer:
    message = message.value
    logging.info("### Scrapython : reception d'un message ! " + str(message))
    scrapyResults = []
    complexityLevel = 1
    if 'complexity' not in message or (int(message['complexity']) <= int(complexity)):
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
                                if int(complexity) > 1:
                                    if 'complexity' not in message:
                                        complexityLevel = 2
                                    else:
                                        complexityLevel = int(message['complexity']) + 1
                                #renvoi dans topic in pour retraitement par scrapython
                                producer.send(
                                    topic_in,
                                    value={'idBio': message['idBio'], 'url': message['url'], 'complexity': complexityLevel})

                    scrapyResult.update([('url', urlList[0]), ('content', textString)])
                    scrapyResults.append(scrapyResult)

                os.remove("results.json")

            producer.send(
                topic_out,
                value={'idBio': message['idBio'], 'scrapyResults': scrapyResults})
