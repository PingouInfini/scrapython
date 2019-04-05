import os
import sys
import shutil
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapython import settings as my_settings
from scrapython.spiders import spiderthon

#Suppression de l'ancien fichier de récupération des résultats du crawl des urls fournies
#Ce fichier est en effet créé à chaque run du script, ou complété (et on ne veut pas que les données deux deux scripts se suivent)

results_file_exists =  os.path.isfile("results.json")
if results_file_exists :
    os.remove("results.json")

crawler_settings = Settings()

crawler_settings.setmodule(my_settings)

process = CrawlerProcess(settings = crawler_settings)

process.crawl(spiderthon.spiderthon, sys.argv[2])
process.start()

#Copie des résultats obtenus dans un fichier au nom du candidat

candidate = sys.argv[1]
results_with_name = "crawling_results_for_{}.json".format(candidate)
shutil.copy("results.json", results_with_name)

