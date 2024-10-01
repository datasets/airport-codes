import datetime
import json

import decimal
import isodate

from .serializer_base import SerializerBase


DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
TIME_FORMAT = '%H:%M:%S'


class CommonJSONDecoder(json.JSONDecoder):
    """
    Common JSON Encoder
    json.loads(myString, cls=CommonJSONEncoder)
    """

    @classmethod
    def object_hook(cls, obj):
        if 'type{decimal}' in obj:
            try:
                return decimal.Decimal(obj['type{decimal}'])
            except decimal.InvalidOperation:
                pass
        if 'type{time}' in obj:
            try:
                return datetime.datetime \
                    .strptime(obj["type{time}"], TIME_FORMAT) \
                    .time()
            except ValueError:
                pass
        if 'type{datetime}' in obj:
            try:
                return datetime.datetime \
                    .strptime(obj["type{datetime}"], DATETIME_FORMAT)
            except ValueError:
                pass
        if 'type{date}' in obj:
            try:
                return datetime.datetime \
                    .strptime(obj["type{date}"], DATE_FORMAT) \
                    .date()
            except ValueError:
                pass
        if 'type{duration}' in obj:
            try:
                return isodate.parse_duration(obj["type{duration}"])
            except ValueError:
                pass
        if 'type{set}' in obj:
            try:
                return set(obj['type{set}'])
            except ValueError:
                pass

        return obj

    def __init__(self, **kwargs):
        kwargs['object_hook'] = self.object_hook
        super(CommonJSONDecoder, self).__init__(**kwargs)


class CommonJSONEncoder(json.JSONEncoder):
    """
    Common JSON Encoder
    json.dumps(myString, cls=CommonJSONEncoder)
    """

    def default(self, obj):

        if isinstance(obj, decimal.Decimal):
            return {'type{decimal}': str(obj)}
        elif isinstance(obj, datetime.time):
            return {'type{time}': obj.strftime(TIME_FORMAT)}
        elif isinstance(obj, datetime.datetime):
            return {'type{datetime}': obj.strftime(DATETIME_FORMAT)}
        elif isinstance(obj, datetime.date):
            return {'type{date}': obj.strftime(DATE_FORMAT)}
        elif isinstance(obj, (isodate.Duration, datetime.timedelta)):
            return {'type{duration}': isodate.duration_isoformat(obj)}
        elif isinstance(obj, set):
            return {'type{set}': list(obj)}
        return super().default(obj)


class JsonSerializer(SerializerBase):

    def __init__(self,
                 decoder=CommonJSONDecoder,
                 encoder=CommonJSONEncoder):
        self.encoder = encoder
        self.decoder = decoder

    def serialize(self, obj: object) -> bytes:
        return json.dumps(obj, cls=self.encoder, sort_keys=True, ensure_ascii=False).encode('utf-8')

    def deserialize(self, s: bytes) -> object:
        return json.loads(s.decode('utf-8'), cls=self.decoder)
