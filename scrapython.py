# -*- coding:utf-8 -*-

import json
import logging
import os
import re
import time

import requests
from crochet import setup
from kafka import KafkaConsumer
from kafka import KafkaProducer
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapy.signalmanager import dispatcher
from twisted.internet.defer import inlineCallbacks
from urllib.parse import urlparse


import elastic
from common import settings as my_settings
from common.spiders import spiderthon

# recuperation variable d'env
# kafka_endpoint = str(os.environ['KAFKA_IP']) + ":" + str(os.environ['KAFKA_PORT'])
# topic_in = str(os.environ['TOPIC_IN'])
# topic_out_compara = str(os.environ['TOPIC_OUT_COMPARA'])
# topic_out_googlethon = str(os.environ['TOPIC_OUT_GOOGLETHON'])
# depth = int(os.environ['DEPTH'])
# url_get_dico = "http://" + str(os.environ['DICO_IP']).replace("\"", "") + ":" + \
#                str(os.environ['DICO_PORT']).replace("\"", "") + "/" + str(os.environ['DICO_PATH']).replace("\"", "")
# debug_level = os.environ["DEBUG_LEVEL"]

kafka_endpoint = "localhost" + ":" + "8092"
topic_in = "urlToScrapy"
topic_out_compara = "scrapyToCompara"
topic_out_googlethon = "housToGoogle"
depth = 1
url_get_dico = "http://192.168.0.9:9876/dictionary"
debug_level = "INFO"

# gère le reactor de scrapy dans un thread différent
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

def depthSend(text, r, depthLevel, rslash, message, urlCut, topic_in):
    targetUrl = text.get("url", "")
    if re.match(r, text.get("url", "")):
        if int(depthLevel) > 1:
            if 'depthUrl' not in message:
                newDepthUrl = 2
            else:
                newDepthUrl = int(message['depthUrl']) + 1
            # si le resultat commence par un slash,
            # on prend ce qu'il y a apres le slash et on le concatene à l'adresse urlCut
            if re.match(rslash, text.get("url", "")):
                # targetUrl = urlCut + "/" + text.get("url", "").split("/", 1)[1]
                targetUrl = str(urlCut) + str(urlparse(text.get("url", "")).path)

            # renvoie dans topic in pour retraitement par scrapython
            if int(newDepthUrl) <= int(depthLevel):
                producer.send(
                    topic_in,
                    value={'biographics': {
                        "nom": message['biographics'].get('nom'),
                        "prenom": message['biographics'].get('prenom'),
                        "idBio": message['biographics'].get('idBio')
                    },
                        "url": [targetUrl],
                        "idDictionary": message['idDictionary'],
                        "depthUrl": newDepthUrl,
                        "depthLevel": depthLevel
                    })



class Dictionnaire:
    dico = ""

    # Recupère le dico aupres de Colissithon
    def retrieve_dico_from_coli(self, idDictionary):
        logging.debug("Asking for Colissithon's dico")

        with requests.Session() as session:
            basic_get_response = session.get(url=url_get_dico + "/" + idDictionary)
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
    depthUrl = 1
    depthLevel = message['depthLevel']  # niveau de profondeur set par l'utilisateur
    nom_prenom = message['biographics'].get('nom') + " " + message['biographics'].get('prenom')
    dico.retrieve_dico_from_coli(message['idDictionary'])
    # regex : détecte si la str commence par http.s:// ou un /
    r = re.compile("^(((http|https):\/\/)|(\/.))")
    # regex : détecte si la str commence par un /
    rslash = re.compile("^(\/.)")

    if 'depthUrl' not in message or (int(message['depthUrl']) <= int(depthLevel)):

        for url in message['url']:
            try:
                img_url_list = []
                results_crawl = []
                scrapyResult = {}
                textString = ''
                motclefFound = ''
                listThemeMotClefHit = []
                dicoMotClefPhraseHit = {}
                points = 0
                idBio = message['biographics'].get('idBio')
                nom = message['biographics'].get('nom')
                prenom = message['biographics'].get('prenom')
                frequence = 0
                url_visited = False

                # Nous avons besoin d'envoyer une liste d'URL
                urlList = []
                urlList.append(url)
                # regex : coupe l'url pour avoir que le nom de domaine
                urlCut = re.match("^((http|https):\/\/|(www\.|ftp\.))([\w_-]+((\.[\w_-]+)+))", urlList[0]).group(0)

                search_object = \
                    {
                        "query": {
                            "multi_match": {
                                "query": idBio + " " + urlList[0],
                                "type": "cross_fields",
                                "fields": ["idBio", "item"],
                                "operator": "and"
                            }
                        }
                    }

                _es = elastic.connect_elasticsearch()
                if elastic.create_index(_es, 'urls'):
                    results_search_elastic = elastic.search(_es, 'urls', json.dumps(search_object))
                    if not results_search_elastic['hits'].get('total'):
                        out = elastic.store_record(_es, 'urls', json.dumps({"idBio": idBio, "item": urlList[0]}))
                    else:
                        url_visited = True
                        logging.info('URL already crawled with this idBio!')
                if not url_visited:
                    # TODO se poser la question des "dates" liée au traitment d'une URL, et forcer le traitement
                    # si date > 24h (paramètrable: -1=infini // 0=osef des doublons // x= à respecter)

                    loop_urls(urlList)  # TODO: selenium pour JS ?

                    # Attendre la fin du scraping
                    while not scrapython_acq_ended:
                        time.sleep(1)

                    try:
                        for text in results_crawl:
                            if next(iter(text)) == 'text':
                                textString = textString + " " + text['text']  # TODO y remplacer les ' par "

                            elif next(iter(text)) == 'url':
                                depthSend(text, r, depthLevel, rslash, message, urlCut, topic_in)

                            elif next(iter(text)) == 'img_url':
                                img_url_list.append(text['img_url'])

                    except Exception as e:
                        logging.warning("got %s on json.load()" % e)

                    # TODO Gestion de vérif de nom/prenom dans le texte
                    # TODO prise en compte de l'URL si le nom, ou le prénom, ou une combinaison (Mr Chirac?) apparait
                    # if nom_prenom in textString:
                    for theme in dico.get_dico()["theme"]:
                        themeName = theme["name"]
                        listMotClefsPond = theme["motclef"]
                        for motClefsPond in listMotClefsPond:
                            motclef = motClefsPond["clef"]

                            if re.match("^((http|https):\/\/|(www\.|ftp\.))([\w_-]+((\.[\w_-]+)+))", motclef):
                                motclefFound = (urlparse(url).hostname == urlparse(motclef).hostname)
                            else:
                                ### position mot dans le texte concaténé
                                position = textString.find(motclef)  # # # TODO trouver mot seul ou collé ?
                                motclefFound = (position != -1)

                            ### 10% de la pond pour la liste de points.

                            frequence = textString.count(motclef)

                            if motclefFound:
                                phrase = re.findall(r"([^.]*?" + motclef + "[^.]*\.)", textString)

                                ponderation = int(motClefsPond["pond"])
                                points = points + ponderation
                                themeTrigramme = themeName[:3].upper()

                                listThemeMotClefHit.append(themeTrigramme + "." + motclef)

                                dicoMotClefPhraseHit[motclef] = phrase
                                value_to_send_to_googlethon = {
                                    'idBio': message['biographics'].get('idBio'),
                                    'nom': message['biographics'].get('nom'),
                                    'prenom': message['biographics'].get('prenom'),
                                    'motclef': motclef,
                                    'idDictionary': message['idDictionary'],
                                    'depthLevel': depth

                                }
                                search_object = \
                                    {
                                        "query": {
                                            "multi_match": {
                                                "query": idBio + " " + motclef,
                                                "type": "cross_fields",
                                                "fields": ["idBio", "item"],
                                                "operator": "and"
                                            }
                                        }
                                    }
                                _es = elastic.connect_elasticsearch()
                                if elastic.create_index(_es, 'motclefs'):
                                    results_search_elastic = elastic.search(_es, 'motclefs', json.dumps(search_object))
                                    if not results_search_elastic['hits'].get('total'):
                                        out = elastic.store_record(_es, 'motclefs',
                                                                   json.dumps({"idBio": idBio, "item": motclef}))
                                        producer.send(topic_out_googlethon, value=value_to_send_to_googlethon)
                    if listThemeMotClefHit:
                        value_to_send = {
                            'biographics': message['biographics'],
                            'urlsResults': {
                                'url': url,
                                'listUrlImage': img_url_list,
                                'frequence': frequence,
                                'listThemeMotclefHit': listThemeMotClefHit,
                                'imageHit': 0,
                                'points': points,
                                'depthLevel': message['depthLevel'],
                                'idDictionary': message['idDictionary']
                            }
                        }

                        ## verif si une photo correspond, envoi à comparathon
                        producer.send(topic_out_compara, value=value_to_send)

            except Exception as e:
                logging.error("scrapython failure %s" % e)
