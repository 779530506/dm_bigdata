import requests
import json
from elasticsearch import Elasticsearch


# Create the client instance

import csv
import os

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
import pandas as pd
def process_csv(filename,sep,head,setHeader,columns):
    
    df = pd.read_csv(filename,sep=sep)
    # adding header
    #headerList = ['name','ages']
    rep ="/home/abdoulayesarr/Documents/Digital_management/nifi"
    #rep ="/home/data/Documents/dm/tmp"
    new_filename = f'{str(datetime.now())}.csv'
    filename=os.path.join(rep,new_filename)
    #delete colonne none
    header,colonne=[],[]
    for i in range(len(head)):
        if (head[i]!="delete"):
            header.append(head[i])
            colonne.append(columns[i])

    if setHeader=="oui":
        df.to_csv(filename, header=header,columns=colonne, index=False)
    else:
        df.to_csv(filename, index=False)

    # with open(filename, 'r') as f:
    #     reader = csv.DictReader(f)
    #     tab=[]
    #     for row in reader:
    #         tab.append(row)
    #     breakpoint()
    #     return tab
  
def getData():
    client = Elasticsearch("http://localhost:9200",)
    resp = client.search(index="dm", body={'size' : 6000, 'query':{"match_all": {}}})
    return resp
def getDataSearch(colonnes,value):
    client = Elasticsearch("http://localhost:9200",)
    resp = client.search(index="dm", body={'size' : 6000, 'query':{
        "multi_match": {
        "query" : value,
        "fields": colonnes
       }
        }})
    return resp
def getFields():
    # client = Elasticsearch("http://localhost:9200",)
    # resp = client.search(index="dm", body={'size' : 1, 'query':{"match_all": {}}})
    # personnes = resp["hits"]["hits"]
    # fields=[]
    # for key in personnes[0]['_source']:
    #     fields.append(key)
    return  ["UID","number","first_name","last_name","gender","city_of_birth","city","status","company","last_publication","mail","date_of_birth"]

def getSearchMultiple(req):
    query = []
    for i in range(len(req)):
        query.append({"match": req[i]})
    print(query)
    client = Elasticsearch("http://localhost:9200",)
    resp = client.search(index="dm", body={'size' : 6000, 'query':{
        "bool":{"must":query}
        }})
    return resp
