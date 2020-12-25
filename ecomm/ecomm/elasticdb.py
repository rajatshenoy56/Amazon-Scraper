from datetime import datetime
from elasticsearch import Elasticsearch
import pytz


class ElasticDB:
    es = None

    @classmethod
    def connect_elasticsearch(cls):
        cls.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        if cls.es.ping():
            print('Connected')
        else:
            print('could not connect!')

    @staticmethod
    def create_index(es_object, settings, index_name):
        created = False
        try:
            if not es_object.indices.exists(index_name):
                es_object.indices.create(index=index_name, ignore=400, body=settings)
                print('Created Index')
            created = True
        except Exception as ex:
            print(str(ex))
        finally:
            return created

    @staticmethod
    def store_record(es_object, index_name, record, pid):
        is_stored = True
        try:
            outcome = es_object.index(index=index_name, body=record, refresh=True, id=pid)
        except Exception as ex:
            print('Error in indexing data')
            print(str(ex))
            is_stored = False
        finally:
            return is_stored

    @staticmethod
    def search(es_object, index_name, search):
        result = es_object.search(index=index_name, body=search, size=9000)
        return result

    @staticmethod
    def create_run(es_object):
        settings = {
            "mappings": {
                "properties": {
                    "created_at": {
                        "type": "date",
                        "format": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ"
                    }
                }
            }
        }
        now = datetime.now(pytz.timezone("Asia/Calcutta"))
        now = datetime.strftime(now, "%Y-%m-%dT%H:%M:%S.%f%z")
        record = {"created_at": str(now)}
        es_object.indices.create(index='run_db', ignore=400, body=settings)
        run_id = es_object.index(index='run_db', body=record, refresh=True)["_id"]

        return run_id
