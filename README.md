# scrapython
1- Créer un virtualenv spécifique au projet et l'activer

2- Installation des librairies

    pip install -r requirements.txt
    
3 - Lancement : 
    
    scrap_entry_point.py "<candidate>" "<path/to/Urls_List_File.txt>"
    
4 - Retour d'un fichier results.json qui comporte du contenu extrait des pages HTML :  
* les titres (\<h1-6\>)
* le texte (\<p\>, \<span\>)
* les urls 


#####Fonctionnement général : 
Le fichier .txt à fournir ne doit comporter QUE des urls.
Les résultats sont retournés dans un fichier json "crawling_results_for_\<Candidate\>.json"
