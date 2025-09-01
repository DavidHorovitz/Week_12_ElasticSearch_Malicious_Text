import pandas as pd
import elasticsearch

from elasticsearch import Elasticsearch
class Loed_to_elastic:
    def __init__(self):
        self.df=pd.read_csv("../data/tweets_injected 3.csv")
        self.dict_of_data=self.df.to_dict(orient="records")
        self.ES_HOST="http://127.0.0.1:9200"
        self.ES_INDEX="demo"
        self.es=Elasticsearch(self.ES_HOST, verify_certs=False)

    def connection_to_elastic(self):
        if self.es.ping():
            print("Elasticsearch is up!")
            return self.es
        else:
            print("Connection to Elasticsearch failed")
            return None

    def create_index_if_not_exists(self):
        if not self.es.indices.exists(index=self.ES_INDEX):
            mapping = {
                "mappings": {
                    "properties": {
                        "TweetID": {"type": "float"},
                        "CreateDate": {"type":  "date", "format": "yyyy-MM-dd HH:mm:ss"},
                        "Antisemitic": {"type": "integer"},
                        "text": {"type": "text"},
                        "sentiment": {"type": "keyword"},
                        "list_of_weapons": {"type": "text"}
                    }
                }
            }
            self.es.indices.create(index=self.ES_INDEX, body=mapping)
            print(f"✅ Index '{self.ES_INDEX}' created.")
        else:
            print(f"ℹ️ Index '{self.ES_INDEX}' already exists.")


# a=Loed_to_elastic()
# a.conection_to_elastic()