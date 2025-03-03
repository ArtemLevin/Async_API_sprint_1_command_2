import time
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError

if __name__ == '__main__':
    es_client = Elasticsearch(hosts=['http://elasticsearch:9200'], verify_certs=False, use_ssl=False)

    while True:
        try:
            if es_client.ping():
                print("Elasticsearch is up and running!")
                break
        except ConnectionError as e:
            print(f"Elasticsearch is not yet available. Error: {e}")

        time.sleep(1)