from typing import Iterator
import plyvel
from .base import KVFileBase, KeySValueIterator
from .cached import CachedKVFile
from .serializer import SerializerBase

class KVFileLevelDB(KVFileBase):

    def __init__(self, serializer: SerializerBase=None, location=None):
        super().__init__(serializer=serializer, location=location)
        self.db = plyvel.DB(self.dirname, create_if_missing=True)

    def _close_db(self):
        if hasattr(self, 'db'):
            self.db.close()
            del self.db

    def _get_db(self, key: str) -> bytes:
        return self.db.get(key.encode('utf8'))

    def _set_db(self, key: str, value: bytes) -> None:
        key = key.encode('utf8')
        self.db.put(key, value)

    def _del_db(self, key: str) -> None:
        key = key.encode('utf8')
        self.db.delete(key)

    def _keys(self, reverse=False) -> Iterator[str]:
        it = self.db.iterator(reverse=reverse)
        try:
            for key, _ in it:
                yield key.decode('utf8')
        finally:
            del it

    def _db_items(self, reverse=False) -> KeySValueIterator:
        it = self.db.iterator(reverse=reverse)
        try:
            for key, value in it:
                yield (key.decode('utf8'), value)
        finally:
            del it

    def _set_db_batch(self, batch: KeySValueIterator) -> None:
        write_batch = self.db.write_batch()
        for key, value in batch:
            write_batch.put(key.encode('utf-8'), value)
            write_batch.write()
            write_batch.clear()
        del write_batch


class CachedKVFileLevelDB(CachedKVFile):
    def __init__(self, serializer: SerializerBase=None, location=None, size=CachedKVFile.DEFAULT_CACHE_SIZE):
        super().__init__(KVFileLevelDB, serializer=serializer, location=location, size=size)
