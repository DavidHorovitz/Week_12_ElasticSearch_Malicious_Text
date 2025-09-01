import os

ES_HOST=os.getenv("ES_HOST","http://127.0.0.1:9200")
ES_INDEX=os.getenv("ES_INDEX","my_elastic_index")



# res = a.es.search(index=a.ES_INDEX, query={"match_all": {}})
# print(res['hits']['hits'][:5])
#
# res = a.es.search(index=a.ES_INDEX, query={"match_all": {}})
# for hit in res['hits']['hits']:
#     print(hit['_source'])
#
# http://127.0.0.1:9200/my_elastic_index/_search?pretty