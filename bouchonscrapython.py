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

        # Ce qu'on recoit de Googlethon
        producer = KafkaProducer(bootstrap_servers='localhost:8092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        tab = [{'biographics': {
                    'nom': 'Chirac',
                    'prenom': 'Jacques',
                    'idBio': '65568'
                },
                 'url': ['https://rugby-senart.fr/loisir/seniors/', 'http://clamartrugby92.fr/seniors-se-remettent'
                                                                    '-route/',
                         'http://clamartrugby92.fr/jeunes/cadets-u16/',
                         'http://clamartrugby92.fr/victoire-des-cadets-teuliere-c/']
            }
        ]

        # {'nom': 'JOUY', 'prenom': 'bob', 'motclef': 'rugby', 'ponderation': '3', 'idBio': '65568',
        # 'url': ['http://clamartrugby92.fr/seniors-se-remettent-route/',
        # 'http://clamartrugby92.fr/jeunes/cadets-u16/', 'http://clamartrugby92.fr/victoire-des-cadets-teuliere-c/',
        # 'https://rugby.scuf.org/2019/04/02/jean-hospital/',
        # 'http://clamartrugby92.fr/seniors-federale-2-le-maintien-compromis/',
        # 'https://www.aslagnyrugby.net/La-fete-du-rugby-en-1979.html',
        # 'http://clamartrugby92.fr/le-club-clamart-rugby-92/',
        # 'http://clamartrugby92.fr/juniors-victoire-bout-suspens-a-houilles/',
        # 'https://rugby-senart.fr/loisir/seniors/', 'http://clamartrugby92.fr/ecole-de-rugby/minimes-u14/']}

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
