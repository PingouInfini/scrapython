# -*- coding:utf-8 -*-

import json
import logging
import os
import time
import re
import requests

from crochet import setup
from kafka import KafkaConsumer
from kafka import KafkaProducer
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from twisted.internet.defer import inlineCallbacks

from scrapy import signals
from scrapy.signalmanager import dispatcher

from common import settings as my_settings
from common.spiders import spiderthon

# recuperation variable d'env
# kafka_endpoint = str(os.environ['KAFKA_IP']) + ":" + str(os.environ['KAFKA_PORT'])
# topic_in = str(os.environ['TOPIC_IN'])
# topic_out = str(os.environ['TOPIC_OUT'])
# complexity = int(os.environ['COMPLEXITY'])
# debug_level = os.environ["DEBUG_LEVEL"]

kafka_endpoint = "localhost" + ":" + "8092"
topic_in = "urlToScrapy"
topic_out_compara = "scrapyToCompara"
topic_out_coli = "scrapyToColissi"
complexity = 1
debug_level = "INFO"

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


# Permet de remplir une liste des résultats du scraping
def crawler_results(signal, sender, item, response, spider):
    results_crawl.append(item)


# Renvoie dans une file kakfa les urls à crawler en fonction de la complexité
def complexitySend(text, r, complexity, rslash, message, urlCut, topic_in):
    targetUrl = text.get("url", "")
    if re.match(r, text.get("url", "")):
        if int(complexity) > 1:
            if 'complexity' not in message:
                complexityLevel = 2
            else:
                complexityLevel = int(message['complexity']) + 1
            # si le resultat commence par un slash,
            # on prend ce qu'il y a apres le slash et on le concatene à l'adresse urlCut
            if re.match(rslash, text.get("url", "")):
                targetUrl = urlCut + "/" + text.get("url", "").split("/", 1)[1]

            # renvoie dans topic in pour retraitement par scrapython
            producer.send(
                topic_in,
                value={'idBio': message['idBio'], 'url': [targetUrl], 'nom': message['nom'],
                       'prenom': message['prenom'], 'complexity': complexityLevel})


class Dictionnaire:
    # dico = [
    #     {'sport': [{'rugby': '3'}, {'football': '8'}, {'tennis': '6'}]},
    #     {'musique': [{'jazz': '2'}, {'rap': '8'}, {'rock': '4'}]}
    # ]
    dico = ""

    # Recupère le dico aupres de Colissithon
    def retrieve_dico_from_coli(self):
        logging.debug("Asking for Colissithon's dico")

        with requests.Session() as session:
            url_get = "http://localhost:9876/dictionnaire"
            basic_get_response = session.get(url=url_get)
            if basic_get_response.ok:
                j_data = json.loads(basic_get_response.content.decode('utf-8'))
                self.dico = j_data
                return self.dico
            else:
                # If response code is not ok (200), print the resulting http error code with description
                return basic_get_response.raise_for_status()

    def get_dico(self):
        return self.dico


@inlineCallbacks
# Crawl la liste des urls de page internet donnée selon les settings dans spiderthon.py
def loop_urls(urls):
    global scrapython_acq_ended
    scrapython_acq_ended = False
    yield process.crawl(spiderthon.spiderthon, urls)
    scrapython_acq_ended = True


dico = Dictionnaire()
dico.retrieve_dico_from_coli()

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

results_crawl = []
# Call crawler_results avec la lib Signal pour recupèrer les résultats du scraping
dispatcher.connect(crawler_results, signal=signals.item_passed)

crawler_settings = Settings()
crawler_settings.setmodule(my_settings)

process = CrawlerProcess(settings=crawler_settings)

for message in consumer:
    message = message.value
    logging.info("### Scrapython : reception d'un message ! " + str(message))
    scrapyResults = []
    complexityLevel = 1
    prenom_nom = message['biographics'].get('nom') + " " + message['biographics'].get('prenom')

    # regex : détecte si la str commence par http.s:// ou un /
    r = re.compile("^(((http|https):\/\/)|(\/.))")
    # regex : détecte si la str commence par un /
    rslash = re.compile("^(\/.)")

    if 'complexity' not in message or (int(message['complexity']) <= int(complexity)):
        try:
            for url in message['url']:
                urlList = []
                img_url_list = []
                urlList.append(url)
                # regex : détecte si la str est une adresse internet ou non
                urlCut = re.match("^((http|https):\/\/|(www\.|ftp\.))([\w_-]+((\.[\w_-]+)+))", urlList[0]).group(0)
                results_crawl = []
                loop_urls(urlList)  # TODO: selenium pour JS ?

                while not scrapython_acq_ended:
                    time.sleep(1)

                scrapyResult = {}
                textString = ''
                listMotClefHit = []
                try:
                    for text in results_crawl:
                        if next(iter(text)) == 'text':
                            textString = textString + " " + text['text']  # TODO y remplacer les ' par "

                        elif next(iter(text)) == 'url': # TODO doublon url
                            complexitySend(text, r, complexity, rslash, message, urlCut, topic_in)

                        elif next(iter(text)) == 'img_url':
                            img_url_list.append(text['img_url'])

                except Exception as e:
                    logging.warning("got %s on json.load()" % e)

                if prenom_nom in textString: # TODO Gestion de vérif de nom/prenom dans le texte
                    for theme in dico.get_dico():
                        for themeName, listMotClefsPond in theme.items():
                            for motClefsPond in listMotClefsPond:
                                motclef = next(iter(motClefsPond))

                                ### position mot dans le texte concaténé
                                position = textString.find(motclef) ## TODO Ajout frequence
                                if position != -1:
                                    listMotClefHit.append(motclef)
                                    ## TODO: recuperer bout de text avec motclef
                                    ## TODO: renvoie à googlethon nom + prenom + motclef
                if listMotClefHit:
                    value_to_send = {
                        'biographics': message['biographics'],
                        'urlsResults': {
                            'url': url,
                            'listUrlImage': img_url_list,
                            'frequence': 1,
                            'motclefHit': listMotClefHit,
                            'imageHit': 0
                        }
                    }

                    ## verif si une photo correspond, envoi à comparathon
                    producer.send(topic_out_compara, value=value_to_send)

                # scrapyResult.update([('url', urlList[0]), ('content', textString)])
                # scrapyResults.append(scrapyResult)
                #
                # value_to_send = json.dumps({'idBio': message['idBio'], 'scrapyResults': scrapyResult})

                # producer.send(topic_out_compara, value=value_to_send)

        except Exception as e:
            logging.error("scrapython failure %s" % e)

