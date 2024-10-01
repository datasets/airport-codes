import datetime
import json

import decimal
import isodate

# On some platforms, pre year 1000 dates are not formatted using 4 digits (which is later unparseable)
try:
    if datetime.date(1, 1, 1).strftime('%04Y') == '4Y':
        DATE_F_FORMAT = '%Y-%m-%d'
        DATETIME_F_FORMAT = '%Y-%m-%dT%H:%M:%S'
    else:
        DATE_F_FORMAT = '%04Y-%m-%d'
        DATETIME_F_FORMAT = '%04Y-%m-%dT%H:%M:%S'
except ValueError:
    DATE_F_FORMAT = '%Y-%m-%d'
    DATETIME_F_FORMAT = '%Y-%m-%dT%H:%M:%S'

DATE_FORMAT = DATE_F_FORMAT
DATETIME_FORMAT = DATETIME_F_FORMAT
TIME_FORMAT = '%H:%M:%S'

DATE_P_FORMAT = '%Y-%m-%d'
DATETIME_P_FORMAT = '%Y-%m-%dT%H:%M:%S'
TIME_P_FORMAT = TIME_F_FORMAT = TIME_FORMAT


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
                    .strptime(obj['type{time}'], TIME_P_FORMAT) \
                    .time()
            except ValueError:
                pass
        if 'type{datetime}' in obj:
            try:
                (isoformat, tzofs, tzname) = obj['type{datetime}']
                parsed = datetime.datetime \
                    .strptime(isoformat, DATETIME_P_FORMAT)
                if tzname is not None:
                    return datetime.datetime \
                        .combine(parsed.date(), parsed.time(),
                                 datetime.timezone(datetime.timedelta(seconds=tzofs), tzname))
                else:
                    return parsed
            except ValueError:
                pass
        if 'type{date}' in obj:
            try:
                return datetime.datetime \
                    .strptime(obj['type{date}'], DATE_P_FORMAT) \
                    .date()
            except ValueError:
                pass
        if 'type{duration}' in obj:
            try:
                return isodate.parse_duration(obj['type{duration}'])
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
            return {'type{time}': obj.strftime(TIME_F_FORMAT)}
        elif isinstance(obj, datetime.datetime):
            return {'type{datetime}':
                    (obj.strftime(DATETIME_F_FORMAT),
                     obj.utcoffset().seconds if obj.utcoffset() is not None else None,
                     obj.tzname())}
        elif isinstance(obj, datetime.date):
            return {'type{date}': obj.strftime(DATE_F_FORMAT)}
        elif isinstance(obj, (isodate.Duration, datetime.timedelta)):
            return {'type{duration}': isodate.duration_isoformat(obj)}
        elif isinstance(obj, set):
            return {'type{set}': list(obj)}
        return super().default(obj)


class ejson():

    @staticmethod
    def dumps(*args, **kwargs):
        kwargs['cls'] = CommonJSONEncoder
        return json.dumps(*args, **kwargs)

    @staticmethod
    def loads(*args, **kwargs):
        kwargs['cls'] = CommonJSONDecoder
        return json.loads(*args, **kwargs)

    @staticmethod
    def dump(*args, **kwargs):
        kwargs['cls'] = CommonJSONEncoder
        return json.dump(*args, **kwargs)

    @staticmethod
    def load(*args, **kwargs):
        kwargs['cls'] = CommonJSONDecoder
        return json.load(*args, **kwargs)
