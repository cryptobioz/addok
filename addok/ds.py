import time
from prometheus_client import Gauge

from addok.config import config
from addok.db import DB, RedisProxy
from addok.helpers import keys


class RedisStore:

    def fetch(self, *keys):
        pipe = _DB.pipeline(transaction=False)
        for key in keys:
            pipe.get(key)
        for key, doc in zip(keys, pipe.execute()):
            if doc is not None:
                yield key, doc

    def upsert(self, *docs):
        pipe = _DB.pipeline(transaction=False)
        for key, blob in docs:
            pipe.set(key, blob)
        pipe.execute()

    def remove(self, *keys):
        pipe = _DB.pipeline(transaction=False)
        for key in keys:
            pipe.delete(key)
        pipe.execute()

    def flushdb(self):
        _DB.flushdb()


class DSProxy:
    instance = None

    def __getattr__(self, name):
        return getattr(self.instance, name)


_DB = RedisProxy()
DS = DSProxy()

average_upsert_duration_metric = Gauge("addok_average_upsert_duration", "Average upsert duration")
total_upsert_duration_raw_metric = 0
upsert_count_raw_metric = 0

average_remove_duration_metric = Gauge("addok_average_remove_duration", "Average remove duration")
total_remove_duration_raw_metric = 0
remove_count_raw_metric = 0

@config.on_load
def on_load():
    DS.instance = config.DOCUMENT_STORE()
    # Do not create connection if not using this store class.
    if config.DOCUMENT_STORE == RedisStore:
        params = config.REDIS.copy()
        params.update(config.REDIS.get('documents', {}))
        _DB.connect(
            host=params.get('host'),
            port=params.get('port'),
            db=params.get('db'),
            password=params.get('password'),
            unix_socket_path=params.get('unix_socket_path'),
        )


def store_documents(docs):
    to_upsert = []
    to_remove = []
    for doc in docs:
        if not doc:
            continue
        if '_id' not in doc:
            doc['_id'] = DB.next_id()
        key = keys.document_key(doc['_id'])
        if doc.get('_action') in ['delete', 'update']:
            to_remove.append(key)
        if doc.get('_action') in ['index', 'update', None]:
            to_upsert.append((key, config.DOCUMENT_SERIALIZER.dumps(doc)))
        yield doc
    if to_remove:
        start = time.time()
        DS.remove(*to_remove)
        end = time.time()
        remove_count_raw_metric += 1
        total_remove_duration_raw_metric += (end - start)/len(to_remove)
        average_remove_duration_metric.set(total_remove_duration_raw_metric/remove_count_raw_metric)
    if to_upsert:
        start = time.time()
        DS.upsert(*to_upsert)
        end = time.time()
        upsert_count_raw_metric += 1
        total_upsert_duration_raw_metric += (end - start)/len(to_upsert)
        average_upsert_duration_metric.set(total_upsert_duration_raw_metric/upsert_count_raw_metric)




def get_document(key):
    results = DS.fetch(key)
    try:
        _, doc = next(results)
    except StopIteration:
        return None
    return config.DOCUMENT_SERIALIZER.loads(doc)


def get_documents(*keys):
    for id_, blob in DS.fetch(*keys):
        yield id_, config.DOCUMENT_SERIALIZER.loads(blob)
