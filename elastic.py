import json
from time import sleep
from elasticsearch import Elasticsearch
import logging
import os

es_endpoint = str(os.environ['ES_IP'])


def connect_elasticsearch():
    _es = Elasticsearch([{'host': es_endpoint, 'port': 9200}])
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es


def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "_doc": {
                "dynamic": "strict",
                "properties": {
                    "idBio": {
                        "type": "text"
                    },
                    "item": {
                        "type": "text"
                    }
                }
            }
        }
    }
    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def store_record(elastic_object, index_name, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, doc_type='_doc', body=record)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


def search(es_object, index_name, search):
    res = es_object.search(index=index_name, body=search)
    return res

#
# if __name__ == '__main__':
#     logging.basicConfig(level=logging.DEBUG)
#     search_object = {"query": {"multi_match": {"query": "127208 rap", "type": "cross_fields", "fields": ["idBio", "item"], "operator": "and"}}}
#     _es = connect_elasticsearch()
#     if create_index(_es, 'motclefs'):
#         out = store_record(_es, 'motclefs', json.dumps({"idBio": "127208", "item": "rap"}))
#         sleep(2)
#         result = search(_es, 'motclefs', json.dumps(search_object))
#         if not result['hits'].get('total'):
#             print("bonjour")
#         else:
#             print("au revoir")

