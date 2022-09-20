import time
from threading import Thread
from datetime import datetime
from elasticsearch import helpers
from elasticsearch.client import Elasticsearch

import logging

log = logging.getLogger(__name__)

class Compute(Thread):

    def __init__(self,id_to_delete,base_index,es):
        Thread.__init__(self)
        self.id_to_delete=id_to_delete
        self.es= es
        self.base_index= base_index


    def run(self):
        for id in self.id_to_delete:
            try:
                self.es.delete(self.base_index, doc_type="_doc", id=id)
                #log.info("delete success")
            except Exception as e:
                pass
                #log.error(str(e))
         



# class Compute(Thread):
#     def __init__(self,doc_type,file,sep,colonnes,header,cols):
#         Thread.__init__(self)
#         self.file=file
#         self.sep=sep
#         self.colonnes=colonnes
#         self.header = header
#         self.cols =cols
#         self.doc_type=doc_type

#     def run(self):
#         print("start")
#         time.sleep(5)
#         print(self.cols)

#         startDate = datetime.now()
#         try:
#             createOrUpdateDocType(self.doc_type,"pending....")
#         except Exception as e:
#             print(str(e))
#         process_csv(self.file,self.sep,self.colonnes,self.header,self.cols)
#         endDate = datetime.now() - startDate
#         tmin = round((endDate.total_seconds())/60,4)
#         createOrUpdateDocType(self.doc_type,"terminé en %s minute"%tmin)
#         print(tmin)