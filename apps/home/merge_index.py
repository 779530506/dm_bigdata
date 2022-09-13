
from elasticsearch import helpers
from elasticsearch.client import Elasticsearch

def get_merged_records(es, in1, in2, commonField):
    lastEntityId=""
    mergedDoc=None
    mergeQuery={
       "query": {
          "match_all": { }
       },
       "sort": [
          {
             commonField: {
                "order": "asc"
             }
          }
       ]
    }

    for doc in helpers.scan(es,
                        index=in1+","+in2,
                        query=mergeQuery,
                        size=1000,
                        scroll="1m",
                        preserve_order=True):

        

        docSrc=doc["_source"]
        thisEntityId = docSrc[commonField]
        if thisEntityId == lastEntityId:
            # Copy all fields from docSrc to merged doc
            mergedDoc.update(docSrc)
        else:
            if mergedDoc is not None:
                yield {
                    '_op_type': 'index',
                    "doc": mergedDoc
                }
            mergedDoc = docSrc
            lastEntityId = thisEntityId

    if mergedDoc is not None:
        yield {
            '_op_type': 'index',
            "doc": mergedDoc
        }

