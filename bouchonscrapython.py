from kafka import KafkaProducer
import json
import logging
from time import sleep

from argparse import ArgumentParser

parser = ArgumentParser(description='Bouchon googlethon')

parser.add_argument("-v", "--verbosity", action="store_true", help="show debug logs")

options = parser.parse_args()


def main():
    try:
        logging.basicConfig(level=logging.INFO)
        if options.verbosity:
            logging.getLogger().setLevel(logging.DEBUG)

        logging.info(" Démarrage du bouchon ")

        producer = KafkaProducer(bootstrap_servers='localhost:8092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        tab=[
            {'url': ['https://twitter.com/bobjouy', 'https://b0b.fr/']
                ,'nom': 'bob', 'prenom': 'JOUY', 'idBio': '4344'}
                #, 'https://b0b.fr/author/bob-jouy/', 'https://lokan.jp/2016/12/10/lettre-ouverte-bob-jouy-harcelement/', 'https://www.youtube.com/channel/UC0SvAbkS6HDP6_Jv8TjPGmw', 'http://i.trackr.fr/article-b0b-un-oeil-sur-bob-jouy', 'https://fr-fr.facebook.com/public/Bob-Jouy', 'https://www.denisqs.com/author/bob-jouy/', 'https://www.macg.co/ailleurs/2018/12/tesla-la-model-3-partir-de-53-500-eu-en-europe-104546', 'https://forums.automobile-propre.com/topic/suivi-des-commandes-et-livraisons-avec-des-morceaux-de-fake-news-dedans-13393/?page=131']

        ]
        for i in range(len(tab)):
            producer.send('urlToScrapy', value=tab[i])
            sleep(0.5)

    except Exception as e:
        logging.error("ERROR : ", e)
    finally:
        logging.info(" Fin du bouchon ")
        exit(0)

if __name__ == '__main__':
    main()