import requests
import json
from elasticsearch import Elasticsearch


# Create the client instance

def getData():
    client = Elasticsearch("http://localhost:9200",)
    resp = client.search(index="italia.csv", body={'size' : 10000, 'query':{"match_all": {}}})
    return resp
# def getData():
#     index_name = "italia.csv"

#     headers = { 'Content-Type' : 'application/json' }
#     response = requests.get( url="http://localhost:9200/"+ index_name + "/_doc/_search", headers =headers)
#     return response.json()