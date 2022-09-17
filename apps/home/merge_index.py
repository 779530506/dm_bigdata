from elasticsearch import helpers
from elasticsearch.client import Elasticsearch
import logging

log = logging.getLogger(__name__)
def get_merged_records(es, in1, in2, commonField):
    lastEntityId=""
    mergedDoc=None
    index_precedent=""
    mergeQuery={
       "query": {"match_all": { }},
       "sort": [{commonField: {"order": "asc"}}]
    }
    
    merged = 0
    for doc in helpers.scan(es,index=in1+","+in2,query=mergeQuery,size=100,
                        scroll="1m",
                        preserve_order=True):
        docSrc=doc["_source"]
        thisEntityId = docSrc.get(commonField)
        if thisEntityId:
            #if docSrc != mergedDoc:
            if thisEntityId == lastEntityId:
                # Copy all fields from docSrc to merged doc
                #mergedDoc.update(docSrc)
                keys = mergedDoc.keys() | docSrc.keys()
                keys.remove(commonField)
                for k in keys:
                    if (mergedDoc.get(k)!=None and docSrc.get(k) !=None) and (mergedDoc.get(k) != docSrc.get(k)):
                        if str(docSrc.get(k))  in str(mergedDoc.get(k)) :
                            mergedDoc[k]=mergedDoc.get(k)
                        elif  str(mergedDoc.get(k))  in str(docSrc.get(k)):
                            mergedDoc[k]=docSrc.get(k)
                        else:
                            mergedDoc[k] = "%s , %s "% (mergedDoc[k],docSrc.get(k))
                    elif  docSrc.get(k):
                        mergedDoc[k]=docSrc.get(k)
                   

                merged += 1
                if doc.get("_index") ==in2:
                    id_doc = doc.get("_id")
                    es.delete(in2, doc_type="_doc", id=id_doc)
                
                
            else:
                if mergedDoc is not None:
                    
                    if merged >=1:
                        yield {
                            "_source":mergedDoc
                        }
                        if index_precedent ==in2:
                            es.delete(in2, doc_type="_doc", id=id_precedent)
                    elif index_precedent!=in2:
                        yield {
                            "_source":mergedDoc
                        }
                    merged = 0
                    

                                
                mergedDoc = docSrc
                lastEntityId = thisEntityId
                index_precedent =doc.get("_index")
                id_precedent =doc.get("_id")

    
    if mergedDoc is not None and index_precedent!=in2 :
        
        yield {
            "_source":mergedDoc
        }