from fastapi import FastAPI
from elasticsearch import Elasticsearch, helpers
from typing import Dict
import uvicorn
import config

app = FastAPI()
es = Elasticsearch(config.ES_HOST, verify_certs=False)
ES_INDEX = config.ES_INDEX

def check_processing_complete() -> bool:
    query = {
        "query": {
            "bool": {
                "should": [
                    {"bool": {"must_not": {"exists": {"field": "list_of_weapons"}}}},
                    {"term": {"list_of_weapons": []}}  # מסמכים עם רשימה ריקה
                ]
            }
        }
    }
    results = helpers.scan(es, index=ES_INDEX, query=query)
    return not any(True for _ in results)
@app.get("/antisemitic_with_weapons")
def antisemitic_with_weapons() -> Dict:

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"Antisemitic": 1}},
                    {"exists": {"field": "list_of_weapons"}}
                ]
            }
        }
    }

    results = helpers.scan(es, index=ES_INDEX, query=query)
    docs = [{"id": doc["_id"], **doc["_source"]} for doc in results]

    if not docs:
        return {"message": "No antisemitic documents with weapons found."}

    return {"count": len(docs), "documents": docs}


@app.get("/documents_with_2_or_more_weapons")
def documents_with_2_or_more_weapons() -> Dict:

    query = {
        "query": {
            "script": {
                "script": "doc['list_of_weapons'].size() >= 2"
            }
        }
    }

    results = helpers.scan(es, index=ES_INDEX, query=query)
    docs = [{"id": doc["_id"], **doc["_source"]} for doc in results]

    if not docs:
        return {"message": "No documents with 2 or more weapons found."}

    return {"count": len(docs), "documents": docs}


if __name__ == "__main__":

    uvicorn.run("main:app",host="127.0.0.1", port=8000, reload=True)
