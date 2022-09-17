import requests
import json
from elasticsearch import Elasticsearch
from elasticsearch import Elasticsearch, helpers
from apps.home.toDatabase import create_doc_type,get_doc_type,update_doc_type,getAll_doc_type
from apps.home.merge_index import get_merged_records
# Create the client instance

import csv
import os

from collections import defaultdict
from datetime import datetime
import pandas as pd, numpy as np, uuid

''''
generator to push bulk data from a JSON
file into an Elasticsearch index
'''
def bulk_json_data(json_list, _index):
    for doc in json_list:
    # use a `yield` generator so that the data
    # isn't loaded into memory
        yield {
            "_index": _index,
            "_type": "_doc",
            "_id": uuid.uuid4(),
            "_source": doc
        }
'''
load data to elastic search
'''
def load_to_elastic(df,index):
    client = Elasticsearch("http://localhost:9200",timeout=30)
    #df = pd.read_csv(filename,sep=",",encoding= 'unicode_escape')
    df = df.replace(np.nan, '', regex=True)
    df2=df.to_dict("record")
    #index="digital"
    try:
        # make the bulk call, and get a response
        response = helpers.bulk(client, bulk_json_data(df2, index),chunk_size=100000)
        print ("\nbulk_json_data() RESPONSE:", response)

        return response
    except Exception as e:
        print("\nERROR:", e)


def process_csv(filename,sep,head,setHeader,columns):
    index=(filename.split("/")[-1]).split("_")[0] # recuperer le type
    df = pd.read_csv(filename,sep=sep,encoding= 'unicode_escape')

    #delete colonne none
    header,colonne=[],[]
    for i in range(len(head)):
        if (head[i]!="delete"):
            header.append(head[i])
            colonne.append(columns[i])

    if setHeader=="oui":
        df = df[colonne]
        df.columns = header
        #df.to_csv(filename, header=header,columns=colonne, index=False)
    # else:
    #     df.to_csv(filename, index=False)
    
    load_to_elastic(df,index)

def createOrUpdateDocType(name,statut):
    if name ==get_doc_type(name):
        update_doc_type(name,statut)
    else:
        create_doc_type(name,statut)
def getAllDocType():
    return getAll_doc_type()

def getData():
    client = Elasticsearch("http://localhost:9200",timeout=30)
    resp = client.search(index="digital",request_timeout=30, body={'size' : 6000, 'query':{"match_all": {}}})
    return resp
def getDataSearch(colonnes,value):
    client = Elasticsearch("http://localhost:9200",timeout=30)
    resp = client.search(index="digital", body={'size' : 6000, 'query':{
        "multi_match": {
        "query" : value,
        "fields": colonnes
       }
        }})
    return resp
def getFields():
    # client = Elasticsearch("http://localhost:9200",)
    # resp = client.search(index="digital", body={'size' : 1, 'query':{"match_all": {}}})
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
    client = Elasticsearch("http://localhost:9200",timeout=30)
    resp = client.search(index="digital", body={'size' : 6000, 'query':{
        "bool":{"must":query}
        }})
    return resp

def mergeIndex(index1,commonField):
    client = Elasticsearch("http://localhost:9200",timeout=30)
    index2 = "digital"
    # commonField="myId"
    # index1 = "test1"
    try:
        response=helpers.bulk(client, get_merged_records(client,index1, index2,commonField),
                    index="digital",
                    chunk_size=1000)
        return response
    except  Exception as e:
        raise Exception('Error jointure %s'%str(e))

