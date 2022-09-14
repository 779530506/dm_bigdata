
from elasticsearch import helpers
from elasticsearch.client import Elasticsearch
import logging

log = logging.getLogger(__name__)
def get_merged_records(es, in1, in2, commonField):
    lastEntityId=""
    mergedDoc=None
    mergeQuery={
       "query": {"match_all": { }},
       "sort": [{commonField: {"order": "asc"}}]
    }
    
    merged = False
    for doc in helpers.scan(es,index=in1+","+in2,query=mergeQuery,size=100,
                        scroll="1m",
                        preserve_order=True):

        docSrc=doc["_source"]
        thisEntityId = docSrc.get(commonField)
        
        if thisEntityId:
            if thisEntityId == lastEntityId:
                # Copy all fields from docSrc to merged doc
                #mergedDoc.update(docSrc)
                keys = mergedDoc.keys() | docSrc.keys()
                for k in keys:
                    if mergedDoc.get(k) and docSrc.get(k):
                        if str(mergedDoc.get(k)) in str(docSrc.get(k)):
                            mergedDoc[k]=docSrc.get(k)
                        elif str(docSrc.get(k)) in str(mergedDoc.get(k)) :
                            mergedDoc[k]=mergedDoc.get(k)

                        else:
                            mergedDoc[k]="{} , {}".format(mergedDoc.get(k),docSrc.get(k))
                    elif  mergedDoc.get(k):
                        mergedDoc[k]=mergedDoc.get(k)
                    elif  docSrc.get(k):
                        mergedDoc[k]=docSrc.get(k)

                merged = True
                if doc.get("_index") ==in2:
                    id_doc = doc.get("_id")
                    es.delete(in2, doc_type="_doc", id=id_doc)
                
                
            else:
                if mergedDoc is not None and merged :
                    merged = False
                    yield {
                        "_source":mergedDoc
                    }
                                
                mergedDoc = docSrc
                lastEntityId = thisEntityId



    # if mergedDoc is not None:
    #     # yield {
    #     #     '_op_type': 'index',
    #     #     "doc": mergedDoc
    #     # }
    #     yield {
    #         "_source":mergedDoc
    #     }