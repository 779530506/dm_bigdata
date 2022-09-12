import time
from threading import Thread
from apps.home.utils import createOrUpdateDocType,process_csv
from datetime import datetime
class Compute(Thread):
    def __init__(self,doc_type,file,sep,colonnes,header,cols):
        Thread.__init__(self)
        self.file=file
        self.sep=sep
        self.colonnes=colonnes
        self.header = header
        self.cols =cols
        self.doc_type=doc_type

    def run(self):
        print("start")
        time.sleep(5)
        print(self.cols)

        startDate = datetime.now()
        try:
            createOrUpdateDocType(self.doc_type,"pending....")
        except Exception as e:
            print(str(e))
        process_csv(self.file,self.sep,self.colonnes,self.header,self.cols)
        endDate = datetime.now() - startDate
        tmin = round((endDate.total_seconds())/60,4)
        createOrUpdateDocType(self.doc_type,"terminé en %s minute"%tmin)
        print(tmin)


