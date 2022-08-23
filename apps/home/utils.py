import requests
import json
from elasticsearch import Elasticsearch


# Create the client instance

def getData():
    client = Elasticsearch("http://localhost:9200",)
    resp = client.search(index="italia.csv", body={'size' : 6, 'query':{"match_all": {}}})
    return resp
def getDataSearch(colonnes,value):
    client = Elasticsearch("http://localhost:9200",)
    resp = client.search(index="italia.csv", body={'size' : 46, 'query':{
        "multi_match": {
        "query" : value,
        "fields": colonnes
       }
        }})
    return resp
def getFields():
    client = Elasticsearch("http://localhost:9200",)
    resp = client.search(index="italia.csv", body={'size' : 1, 'query':{"match_all": {}}})
    personnes = resp["hits"]["hits"]
    fields=[]
    for key in personnes[0]['_source']:
        fields.append(key)
    return fields
# def getData():
#:     index_name = "italia.csv"

#     headers = { 'Content-Type' : 'application/json' }

#     return response.json()
