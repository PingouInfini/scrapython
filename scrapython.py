import os
import sys
import shutil
import common.settings as my_settings
import common.spiders.spiderthon as spiderthon
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings


#Suppression de l'ancien fichier de récupération des résultats du crawl des urls fournies
#Ce fichier est en effet créé à chaque run du script, ou complété (et on ne veut pas que les données deux deux scripts se suivent)

def launch_scrap(user, path_comparathon_metadata):
    results_file_exists =  os.path.isfile("results.json")
    if results_file_exists :
        os.remove("results.json")

    crawler_settings = Settings()

    crawler_settings.setmodule(my_settings)

    process = CrawlerProcess(settings = crawler_settings)

    process.crawl(spiderthon.spiderthon, path_comparathon_metadata)
    process.start()

    #Copie des résultats obtenus dans un fichier au nom du candidat

    results_with_name = "crawling_results_for_{}.json".format(user)
    shutil.copy("results.json", results_with_name)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
        sys.exit(1)

    user = sys.argv[1]
    path_comparathon_metadata = sys.argv[2]
    launch_scrap(user, path_comparathon_metadata)