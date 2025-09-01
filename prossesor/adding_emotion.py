from nltk.sentiment.vader import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch,helpers
import config

class Process_on_the_data:
    def __init__(self):
        self.ES_HOST=config.ES_HOST
        self.ES_INDEX=config.ES_INDEX
        self.es = Elasticsearch(self.ES_HOST, verify_certs=False)


    def get_sentiment(self,text):
        analyzer = SentimentIntensityAnalyzer()
        score =analyzer.polarity_scores(text)
        num = score['compound']
        if num < -0.5:
            print("negative")
            return 'negative'
        elif num < 0.5:
            print("neutral")
            return 'neutral'
        else:
            print('positive')
            return 'positive'

    def get_all_documents(self):
        query = {"query": {"match_all": {}}}
        print("!!!")
        return helpers.scan(self.es, index=self.ES_INDEX, query=query)

    def update_document_with_sentiment(self, doc_id,sentiment):
        self.es.update(
            index=self.ES_INDEX,
            id=doc_id,
            body={
                "doc": {
                    "sentiment": sentiment
                       }
                 })
        print("updated")

a = Process_on_the_data()
docs = a.get_all_documents()

for doc in docs:
    text = doc["_source"].get("text", "")
    sentiment = a.get_sentiment(text)
    doc_id = doc["_id"]
    a.update_document_with_sentiment(doc_id, sentiment)
