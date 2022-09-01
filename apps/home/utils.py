import requests
import json
from elasticsearch import Elasticsearch


# Create the client instance
 
def getData():
    client = Elasticsearch("http://localhost:9200",)
    resp = client.search(index="elastic", body={'size' : 6000, 'query':{"match_all": {}}})
    return resp
def getDataSearch(colonnes,value):
    client = Elasticsearch("http://localhost:9200",)
    resp = client.search(index="elastic", body={'size' : 6000, 'query':{
        "multi_match": {
        "query" : value,
        "fields": colonnes
       }
        }})
    return resp
def getFields():
    # client = Elasticsearch("http://localhost:9200",)
    # resp = client.search(index="elastic", body={'size' : 1, 'query':{"match_all": {}}})
    # personnes = resp["hits"]["hits"]
    # fields=[]
    # for key in personnes[0]['_source']:
    #     fields.append(key)
    return  ["number","UID","first_name","last_name","gender","city_of_birth","city","status","company","last_publication","mail","date_of_birth"]

def getSearchMultiple(req):
    query = []
    for i in range(len(req)):
        query.append({"match": req[i]})
    print(query)
    client = Elasticsearch("http://localhost:9200",)
    resp = client.search(index="elastic", body={'size' : 6000, 'query':{
        "bool":{"must":query}
        }})
    return resp
