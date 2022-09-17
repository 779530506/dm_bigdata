from elasticsearch import helpers
from elasticsearch.client import Elasticsearch
import logging
from apps.home.thread import Compute
log = logging.getLogger(__name__)
def get_merged_records(es, index1, base_index, commonField):
    lastEntityId=""
    mergedDoc=None
    index_precedent=""
    id_to_delete=[]
    mergeQuery={
       "query": {"match_all": { }},
       "sort": [{commonField: {"order": "asc"}}]
    }
    
    merged = 0
    for doc in helpers.scan(es,index=index1+","+base_index,query=mergeQuery,size=100,
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
                if doc.get("_index") ==base_index:
                    id_doc = doc.get("_id")
                    #es.delete(base_index, doc_type="_doc", id=id_doc)
                    id_to_delete.append(doc.get("_id"))
                
                
            else:
                if mergedDoc is not None:
                    
                    if merged >=1:
                        yield {
                            "_source":mergedDoc
                        }
                        if index_precedent ==base_index:
                            #es.delete(base_index, doc_type="_doc", id=id_precedent)
                            id_to_delete.append(id_precedent)
                    elif index_precedent!=base_index:
                        yield {
                            "_source":mergedDoc
                        }
                    merged = 0
                    

                                
                mergedDoc = docSrc
                lastEntityId = thisEntityId
                index_precedent =doc.get("_index")
                id_precedent =doc.get("_id")

    #log.debug("iid to delete %s"%id_to_delete)
    thread_a = Compute(id_to_delete,base_index,es)
    thread_a.start()
    if mergedDoc is not None and index_precedent!=base_index :
        log.debug("%s"%index_precedent)
        yield {
            "_source":mergedDoc
        }