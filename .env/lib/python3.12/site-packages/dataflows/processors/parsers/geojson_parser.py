import json
import decimal

from tabulator.parsers.json import JSONParser


class DecimalJSONEncoder(json.JSONEncoder):
    def default(self, obj):

        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)


class GeoJsonParser(JSONParser):
    options = []

    def __init__(self, loader, force_parse=False):
        super().__init__(loader, force_parse=force_parse, property='features')

    @property
    def extended_rows(self):
        iterator = super().extended_rows
        for row_number, keys, values in iterator:
            row = dict(zip(keys, values))
            properties = row.get('properties', dict())
            properties['__geometry'] = json.dumps(row.get('geometry'), cls=DecimalJSONEncoder)
            items = list(properties.items())
            yield row_number, list(x[0] for x in items), list(x[1] for x in items)
