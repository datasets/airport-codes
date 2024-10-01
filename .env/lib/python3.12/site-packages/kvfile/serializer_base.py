

class SerializerBase():

    def serialize(self, obj: object) -> bytes:
        raise NotImplementedError()

    def deserialize(self, s: bytes) -> object:
        raise NotImplementedError()
