import os
from collections import deque
from typing import Iterator, Tuple
import tempfile

from .serializer import DefaultSerializer
from .serializer_base import SerializerBase

KeyValueIterator = Iterator[Tuple[str, object]]
KeySValueIterator = Iterator[Tuple[str, bytes]]

class KVFileBase():

    DEFAULT_BATCH_SIZE = 1000

    def __init__(self, serializer: SerializerBase=None, location=None):
        if location is None:
            self.tmpdir = tempfile.TemporaryDirectory()
            self.dirname = self.tmpdir.name
            self.filename = os.path.join(self.dirname, 'kvfile.db')
        else:
            self.filename = location
            self.dirname = location
        self.serializer = serializer or DefaultSerializer()
        self.closed = False

    def close(self):
        if not self.closed:
            self._close_db()
            self.closed = True

    def __del__(self):
        self.close()

    def get(self, key: str, **kw) -> object:
        assert not self.closed
        ret = self._get_db(key)
        if ret is None:
            if 'default' in kw:
                return kw['default']
            raise KeyError()
        else:
            return self.serializer.deserialize(ret)

    def set(self, key: str, value: object):
        assert not self.closed
        value = self.serializer.serialize(value)
        self._set_db(key, value)

    def delete(self, key: str):
        assert not self.closed
        self._del_db(key)

    def insert(self, key_value_iterator: KeyValueIterator, batch_size=DEFAULT_BATCH_SIZE):
        assert not self.closed
        deque(self.insert_generator(key_value_iterator, batch_size), maxlen=0)

    def insert_generator(self, key_value_iterator: KeyValueIterator, batch_size=DEFAULT_BATCH_SIZE):
        assert not self.closed
        if batch_size == 1:
            for key, value in key_value_iterator:
                yield key, value
                self.set(key, value)
        else:
            batch = []
            for key, value in key_value_iterator:
                yield key, value
                value = self.serializer.serialize(value)
                batch.append((key, value))
                if len(batch) >= batch_size:
                    self._set_db_batch(batch)
                    batch.clear()
            if len(batch) > 0:
                self._set_db_batch(batch)

    def items(self, reverse=False) -> KeyValueIterator:
        assert not self.closed
        for key, value in self._db_items(reverse):
            yield key, self.serializer.deserialize(value)

    def keys(self, reverse=False) -> Iterator[str]:
        assert not self.closed
        return self._keys(reverse)

    # Implemented by subclasses:
    def _get_db(self, key: str) -> bytes:
        raise NotImplementedError()

    def _set_db(self, key: str, value: bytes) -> None:
        raise NotImplementedError()

    def _del_db(self, key: str) -> None:
        raise NotImplementedError()

    def _db_items(self, reverse=False) -> KeySValueIterator:
        raise NotImplementedError()

    def _close_db(self):
        raise NotImplementedError()

    def _set_db_batch(self, batch: KeySValueIterator) -> None:
        for key, value in batch:
            self._set_db(key, value)

    def _keys(self, reverse=False) -> Iterator[str]:
        raise NotImplementedError()
