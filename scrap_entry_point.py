import os
import sys
import shutil
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapython import settings as my_settings
from scrapython.spiders import spiderthon

work_file = "urls_list_file.txt"
work_file_exists = os.path.isfile(work_file)

if work_file_exists :
    os.remove(work_file)
else :
    open('urls_list_file.txt', 'a').close()

results_file_exists =  os.path.isfile("results.json")
if results_file_exists :
    os.remove("results.json")

urls_list_file = sys.argv[1]
shutil.copy(urls_list_file, work_file)

crawler_settings = Settings()

crawler_settings.setmodule(my_settings)

process = CrawlerProcess(settings = crawler_settings)

process.crawl(spiderthon.spiderthon)
process.start()



