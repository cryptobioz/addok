import os

from psycopg2 import pool, OperationalError
from psycopg2.extras import execute_values

from addok.config import config


class PSQLStore:
    def __init__(self, *args, **kwargs):
        self.pool = pool.SimpleConnectionPool(minconn=1, maxconn=5,
                                              dsn=config.PG_CONFIG)
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS
            {PG_TABLE} (key VARCHAR COLLATE "C", data bytea)
        '''.format(**config)
        create_index_query = '''
        CREATE UNIQUE INDEX IF NOT EXISTS
            {PG_TABLE}_key_idx ON {PG_TABLE} (key)
        '''.format(**config)
        with self.getconn() as conn, conn.cursor() as curs:
            curs.execute(create_table_query)
            curs.execute(create_index_query)
        self.pool = None

    def create_pool(self):
        self.pool = pool.SimpleConnectionPool(minconn=1, maxconn=5,
                                                dsn=config.PG_CONFIG)
        print("######## NEW POOL #########")

    def getconn(self):
        if self.pool is None:
            self.create_pool()

        conn = self.pool.getconn(key=os.getpid())
        try:
            c = conn.cursor()
        except OperationalError as err:
            self.create_pool()
            conn = self.pool.getconn(key=os.getpid())
        return conn

    def fetch(self, *keys):
        # Using ANY results in valid SQL if `keys` is empty.
        select_query = '''
        SELECT key, data FROM {PG_TABLE} WHERE key=ANY(%s)
        '''.format(**config)
        with self.getconn() as conn, conn.cursor() as curs:
            curs.execute(select_query, ([key.decode() for key in keys],))
            for key, data in curs.fetchall():
                yield key.encode(), data

    def upsert(self, *docs):
        """
        Potential performance boost, using copy_from:
        * https://gist.github.com/jsheedy/efa9a69926a754bebf0e9078fd085df6
        * https://gist.github.com/jsheedy/ed81cdf18190183b3b7d

        Or event copy_expert for mixed binary content:
        * http://stackoverflow.com/a/8150329
        """
        insert_into_query = '''
        INSERT INTO {PG_TABLE} (key, data) VALUES %s
            ON CONFLICT DO NOTHING
        '''.format(**config)
        with self.getconn() as conn, conn.cursor() as curs:
            execute_values(curs, insert_into_query, docs)

    def remove(self, *keys):
        delete_from_query = '''
        DELETE FROM {PG_TABLE} WHERE key=%s
        '''.format(**config)
        with self.getconn() as conn, conn.cursor() as curs:
            curs.executemany(delete_from_query, (keys, ))

    def flushdb(self):
        drop_table_query = '''
        DROP TABLE IF EXISTS {PG_TABLE}
        '''.format(**config)
        with self.getconn() as conn, conn.cursor() as curs:
            curs.execute(drop_table_query)


def preconfigure(config):
    config.DOCUMENT_STORE_PYPATH = 'addok_psql_store.PSQLStore'
    config.PG_CONFIG = 'dbname=addok user=addok host=localhost password=addok'
    config.PG_TABLE = 'addok'
