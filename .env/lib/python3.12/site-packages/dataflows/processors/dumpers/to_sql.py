import datetime
import decimal
import os
import logging
import copy
import json

from tableschema_sql import Storage
from sqlalchemy import create_engine

from tableschema.exceptions import ValidationError

from ...base import ResourceWrapper

from .dumper_base import DumperBase


def jsonize(obj):
    return json.dumps(obj)


def strize(obj):
    if isinstance(obj, dict):
        return dict(
            (k, strize(v))
            for k, v in obj.items()
        )
    elif isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, (list, set)):
        return [strize(x) for x in obj]
    elif obj is None:
        return None
    assert False, "Don't know how to handle object %r" % obj


OBJECT_FIXERS = {
    'sqlite': [strize, jsonize],
    'postgresql': [strize]
}


class SQLDumper(DumperBase):

    def __init__(self,
                 tables,
                 engine='env://DATAFLOWS_DB_ENGINE',
                 updated_column=None, updated_id_column=None,
                 **options):
        super(SQLDumper, self).__init__(options)
        table_to_resource = tables

        if isinstance(engine, str):
            if engine.startswith('env://'):
                env_var = engine[6:]
                engine = os.environ.get(env_var)
                if engine is None:
                    raise ValueError("Couldn't connect to DB - "
                                     "Please set your '%s' environment variable" % env_var)

            self.engine = create_engine(engine)
            # check connection
            with self.engine.connect():
                pass
        else:
            self.engine = engine

        for k, v in table_to_resource.items():
            v['table-name'] = k

        self.converted_resources = \
            dict((v['resource-name'], v) for v in table_to_resource.values())

        self.updated_column = updated_column
        self.updated_id_column = updated_id_column
        self.batch_size = options.get('batch_size', 1000)
        self.use_bloom_filter = options.get('use_bloom_filter', True)

    def normalize_for_engine(self, dialect, resource, schema_descriptor):
        actions = {}
        for field in schema_descriptor['fields']:
            if field['type'] in ['array', 'object']:
                assert dialect in OBJECT_FIXERS, "Don't know how to handle %r connection dialect" % dialect
                actions.setdefault(field['name'], []).extend(OBJECT_FIXERS[dialect])

        for row in resource:
            for name, action_list in actions.items():
                for action in action_list:
                    row[name] = action(row.get(name))

            yield row

    def process_resource(self, resource: ResourceWrapper):
        resource_name = resource.res.name
        if resource_name not in self.converted_resources:
            return resource
        else:
            converted_resource = self.converted_resources[resource_name]
            mode = converted_resource.get('mode', 'rewrite')
            table_name = converted_resource['table-name']
            indexes_fields = converted_resource.get('indexes_fields', None)
            storage = Storage(self.engine, prefix=table_name)
            if mode == 'rewrite' and '' in storage.buckets:
                storage.delete('')
            schema_descriptor = resource.res.descriptor['schema']
            schema = self.normalize_schema_for_engine(self.engine.dialect.name,
                                                      schema_descriptor)
            if '' not in storage.buckets:
                logging.info('Creating DB table %s', table_name)
                try:
                    storage.create('', schema, indexes_fields=indexes_fields)
                except ValidationError as e:
                    logging.error('Error validating schema %r', schema_descriptor)
                    for err in e.errors:
                        logging.error('Error validating schema: %s', err)
                    raise
            else:
                storage.describe('', schema)

            update_keys = None
            if mode == 'update':
                update_keys = converted_resource.get('update_keys')
                if update_keys is None:
                    update_keys = schema_descriptor.get('primaryKey', [])
            logging.info('Writing to DB %s -> %s (mode=%s, keys=%s)',
                         resource_name, table_name, mode, update_keys)
            return map(self.get_output_row,
                       storage.write(
                           '',
                           self.normalize_for_engine(self.engine.dialect.name,
                                                     resource, schema_descriptor),
                           keyed=True, as_generator=True,
                           update_keys=update_keys,
                           buffer_size=self.batch_size,
                           use_bloom_filter=self.use_bloom_filter,
                       ))

    def get_output_row(self, written):
        row, updated, updated_id = written.row, written.updated, written.updated_id
        if self.updated_column:
            row[self.updated_column] = updated
        if self.updated_id_column:
            row[self.updated_id_column] = updated_id
        return row

    def normalize_schema_for_engine(self, dialect, schema):
        schema = copy.deepcopy(schema)
        for field in schema['fields']:
            if dialect == 'sqlite' and field['type'] in ['object', 'array']:
                field['type'] = 'string'
        return schema
