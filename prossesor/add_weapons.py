import config
from elasticsearch import Elasticsearch,helpers

class Process_on_the_data:
    def __init__(self):
        self.ES_HOST = config.ES_HOST
        self.ES_INDEX = config.ES_INDEX
        self.es = Elasticsearch(self.ES_HOST, verify_certs=False)
        self.path_of_weapons="../data/weapons_list.txt"


    def finding_weapons(self,text):
        weapons_found = []
        words = text.split()
        with open(self.path_of_weapons,'r',encoding='utf-8') as f:
            weapons = f.read()
        weapons_list = weapons.split('\n')
        for weapon in words:
            if weapon in weapons_list:
                weapons_found.append(weapon)
        return weapons_found

    def update_document_with_weapons(self, doc_id, weapons):

        self.es.update(index=self.ES_INDEX,id=doc_id,body=
        {"doc":
         {"list_of_weapons": weapons}
            })




