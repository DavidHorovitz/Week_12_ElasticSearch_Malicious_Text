from elasticsearch import Elasticsearch
import config

class Delete_non_relevant_docs:
    def __init__(self):
        self.ES_HOST =config.ES_HOST
        self.ES_INDEX =config.ES_INDEX
        self.es = Elasticsearch(self.ES_HOST, verify_certs=False)

    def delete_docs(self):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"Antisemitic": 0}},
                        {"terms": {"sentiment": ["neutral", "positive"]}},
                        {"bool": {
                            "must_not": [
                                {"exists": {"field": "list_of_weapons"}},
                                {"term": {"list_of_weapons": ""}}
                            ]
                        }}
                    ]
                }
            }
        }

        response = self.es.delete_by_query(index=self.ES_INDEX, body=query)
        deleted_count = response.get("deleted", 0)
        print(f"Deleted {deleted_count} documents.")


deleter = Delete_non_relevant_docs()
deleter.delete_docs()
