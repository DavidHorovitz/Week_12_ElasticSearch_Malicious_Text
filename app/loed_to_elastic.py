import pandas as pd
import elasticsearch
from elasticsearch import Elasticsearch,helpers
class Loed_to_elastic:
    def __init__(self):
        self.df=pd.read_csv("../data/tweets_injected 3.csv")
        self.dict_of_data=self.df.to_dict(orient="records")
        self.ES_HOST="http://127.0.0.1:9200"
        self.ES_INDEX="my_elastic_index"
        self.es=Elasticsearch(self.ES_HOST, verify_certs=False)

    def connection_to_elastic(self):
        if self.es.ping():
            print("Elasticsearch is up!")
            return self.es
        else:
            print("Connection to Elasticsearch failed")
            return None

    def create_index_if_not_exists(self):
        self.es.indices.delete(index=self.ES_INDEX, ignore_unavailable=True)
        if not self.es.indices.exists(index=self.ES_INDEX):
            mapping = {
                "mappings": {
                    # "properties": {
                    #     "TweetID": {"type": "float"},
                    #     "CreateDate": {"type":  "text"},
                    #     "Antisemitic": {"type": "integer"},
                    #     "text": {"type": "text"},
                    #     "sentiment": {"type": "keyword"},
                    #     "list_of_weapons": {"type": "text"}
                    # },
                    "properties": {
                        "Antisemitic": {
                            "type": "long"
                        },
                        "CreateDate": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "TweetID": {
                            "type": "float"
                        },
                        "text": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "sentiment": {"type": "keyword"},
                        "list_of_weapons": {"type": "text"}
                    }
                }
            }
            self.es.indices.create(index=self.ES_INDEX, body=mapping)
            print(f"Index '{self.ES_INDEX}' created.")
        else:
            print(f"Index '{self.ES_INDEX}' already exists.")

    def load_data(self):
        actions = []
        for doc in self.dict_of_data:
            action = {
                "_index": self.ES_INDEX,
                "_source": doc
            }
            actions.append(action)
        helpers.bulk(self.es, actions)
        print(f"Loaded {len(actions)} documents into index '{self.ES_INDEX}'")


a=Loed_to_elastic()
a.connection_to_elastic()
a.create_index_if_not_exists()
a.load_data()