
from elasticsearch import helpers
from elasticsearch.client import Elasticsearch
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("in1", help="Input index name 1")
parser.add_argument("in2", help="Input index name 2")
parser.add_argument("commonField", help="The field name common to both in1 and in2")
parser.add_argument("out", help="The index where fused results are written")
parser.add_argument("-host", help="The elasticsearch host", default="localhost:9200")

parser.add_argument("-writesPerBulk", help="The number of records to write in each bulk request", type=int,
                    default=1000)
parser.add_argument("-readsPerBulk", help="The number of input records per scroll page ", type=int,
                    default=1000)
parser.add_argument("-maxTimeToProcessScrollPage", help="The max time to process page of events", default="1m")
args = parser.parse_args()



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
                        size=args.readsPerBulk,
                        scroll=args.maxTimeToProcessScrollPage,
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




es = Elasticsearch(hosts=args.host)


helpers.bulk(es, get_merged_records(es, args.in1, args.in2, args.commonField),
             index=args.out,
             # Required for < 7.0 elasticsearch
             # doc_type="_doc",
             chunk_size=args.writesPerBulk)

