import redis
from hashids import Hashids
from prometheus_client import Gauge

from addok.config import config

hashids = Hashids()


class RedisProxy:
    instance = None
    Error = redis.RedisError

    def connect(self, *args, **kwargs):
        self.instance = redis.StrictRedis(*args, **kwargs)
        self.metrics_catalog = {
            "redis_key_count": Gauge('addok_redis_keys_count', 'Count of Redis keys'),
        }

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def next_id(self):
        next_id = self.incr('_id_sequence')
        return hashids.encode(next_id)

    def metrics(self):
        keys_count = self.instance.dbsize()
        self.metrics_catalog["redis_key_count"].set(keys_count)


DB = RedisProxy()


@config.on_load
def connect():
    params = config.REDIS.copy()
    params.update(config.REDIS.get('indexes', {}))
    DB.connect(
        host=params.get('host'),
        port=params.get('port'),
        db=params.get('db'),
        password=params.get('password'),
        unix_socket_path=params.get('unix_socket_path'),
    )
