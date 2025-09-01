import config
from elasticsearch import Elasticsearch,helpers
class Process_on_the_data:
    def __init__(self):
        self.ES_HOST = config.ES_HOST
        self.ES_INDEX = config.ES_INDEX
        self.es = Elasticsearch(self.ES_HOST, verify_certs=False)
        self.path_of_weapons = "../data/weapons_list.txt"

        with open(self.path_of_weapons, 'r', encoding='utf-8') as f:
            self.weapons_list = f.read().split('\n')
    def finding_weapons(self):
        query = {
            "query": {
                "bool": {
                    "should": []
                }
            },
            "highlight": {
                "fields": {
                    "text": {}
                }
            }
        }
        for weapon in self.weapons_list:
            query["query"]["bool"]["should"].append({"match": {"text": weapon}})

        return helpers.scan(self.es, index=self.ES_INDEX, query=query)

    def update_document_with_weapons(self, doc_id, weapons):

        return {
            "_op_type": "update",
            "_index": self.ES_INDEX,
            "_id": doc_id,
            "doc": {"list_of_weapons": weapons}
        }

    def insert_list_of_weapons(self):
        docs = self.finding_weapons()
        actions = []

        for doc in docs:
            doc_id = doc["_id"]
            text = doc["_source"].get("text", "")

            weapons_found = []
            words = text.split()
            for w in self.weapons_list:
                if w in words:
                    weapons_found.append(w)

            weapons_found = list(set(weapons_found))
            if weapons_found:
                action = self.update_document_with_weapons(doc_id, weapons_found)
                actions.append(action)
        if actions:
            helpers.bulk(self.es, actions)

        print(f"Updated {len(actions)} documents with weapons list.")


a=Process_on_the_data()
a.insert_list_of_weapons()






