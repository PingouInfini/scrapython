# scrapython
1- Créer un virtualenv spécifique au projet et l'activer

2- Installation de la lib Scrapy

    pip install scrapy

3- Installation de la lib Py32Win

    pip install pypiwin32
    
4 - Installation de la lib Beautiful Soup
    
    pip install bs4
    
5 - Lancement : 
    
    scrap_entry_point.py "<path/to/Urls_List_File.txt>"
    
6 - Retour d'un fichier results.json qui comporte du contenu extrait des pages HTML :  
* les titres (\<h1-6\>)
* le texte (\<p\>, \<span\>)
* les urls 


#####Fonctionnement général : 
Le fichier .txt à fournir ne doit comporter QUE des urls. Ce dernier est copié en un fichier "nettoyé" à chaque lancement (path fixe).
Les résultats sont retournés dans un fichier results.json, également "nettoyé" à chaque lancement.
