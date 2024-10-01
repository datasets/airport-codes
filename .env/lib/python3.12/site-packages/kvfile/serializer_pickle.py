import pickle

from .serializer_base import SerializerBase


class PickleSerializer(SerializerBase):

    def serialize(self, obj: object) -> bytes:
        return pickle.dumps(obj)

    def deserialize(self, s: bytes) -> object:
        return pickle.loads(s)
