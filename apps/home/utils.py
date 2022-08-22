import requests
import json
from elasticsearch import Elasticsearch


# Create the client instance

def getData():
    client = Elasticsearch("http://localhost:9200",)
    resp = client.search(index="italia.txt", body={'size' : 6, 'query':{"match_all": {}}})
    return resp
# def getData():
#:     index_name = "italia.csv"

#     headers = { 'Content-Type' : 'application/json' }

#     return response.json()
