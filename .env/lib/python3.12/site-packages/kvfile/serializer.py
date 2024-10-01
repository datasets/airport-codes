from .serializer_base import SerializerBase
from .serializer_pickle import PickleSerializer
from .serializer_json import JsonSerializer

DefaultSerializer: SerializerBase = PickleSerializer