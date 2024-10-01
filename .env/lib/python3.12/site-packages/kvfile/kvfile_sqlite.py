import sqlite3
from typing import Iterator

from .cached import CachedKVFile
from .base import KVFileBase, KeySValueIterator
from .serializer import SerializerBase

class KVFileSQLite(KVFileBase):

    BATCH_SIZE = 1000

    def __init__(self, serializer: SerializerBase=None, location=None):
        super().__init__(serializer=serializer, location=location)
        if not self.filename.endswith('.sqlite'):
            self.filename += '.sqlite'
        self.db = sqlite3.connect(self.filename)
        self.cursor = self.db.cursor()
        try:
            self.cursor.execute('''CREATE TABLE d (key text PRIMARY KEY, value blob)''')
            self.cursor.execute('''CREATE UNIQUE INDEX i ON d (key)''')
        except sqlite3.OperationalError:
            pass
        self._needs_commit = set()

    def _close_db(self):
        if hasattr(self, 'db'):
            self.db.commit()
            if hasattr(self, 'cursor'):
                del self.cursor
            del self.db

    def _commitW(self, key):
        if len(self._needs_commit) == self.BATCH_SIZE:
            self.db.commit()
            self._needs_commit.clear()
        else:
            self._needs_commit.add(key)

    def _commitR(self, key=None):
        if len(self._needs_commit) and (key is None or key in self._needs_commit):
            self.db.commit()
            self._needs_commit.clear()

    def _get_db(self, key: str) -> bytes:
        self._commitR(key)
        ret = self.cursor.execute('''SELECT value FROM d WHERE key=?''',(key,)).fetchone()
        if ret is None:
            return None
        else:
            return ret[0]

    def _set_db(self, key: str, value: bytes) -> None:
        self.cursor.execute('''INSERT OR REPLACE INTO d VALUES (?, ?)''', (key, value))
        self._commitW(key)

    def _del_db(self, key: str) -> None:
        self.cursor.execute('''DELETE FROM d WHERE key=?''', (key,)).fetchone()
        self._commitW(key)

    def _db_items(self, reverse=False) -> KeySValueIterator:
        self._commitR()
        cursor = self.db.cursor()
        direction = 'DESC' if reverse else 'ASC'
        items = cursor.execute('''SELECT key, value FROM d ORDER BY key ''' + direction)
        for key, value in items:
            yield key, value

    def _keys(self, reverse=False) -> Iterator[str]:
        self._commitR()
        cursor = self.db.cursor()
        direction = 'DESC' if reverse else 'ASC'
        keys = cursor.execute('''SELECT key FROM d ORDER BY key ''' + direction)
        for key, in keys:
            yield key

    def _set_db_batch(self, batch: KeySValueIterator) -> None:
        self.cursor.executemany('''INSERT OR REPLACE INTO d VALUES (?, ?)''', batch)
        self._commitR()


class CachedKVFileSQLite(CachedKVFile):
    def __init__(self, serializer: SerializerBase=None, location=None, size=CachedKVFile.DEFAULT_CACHE_SIZE):
        super().__init__(KVFileSQLite, serializer=serializer, location=location, size=size)
