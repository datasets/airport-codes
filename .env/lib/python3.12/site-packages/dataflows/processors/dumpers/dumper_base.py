import os
import hashlib
import json

from ... import DataStreamProcessor, ResourceWrapper, schema_validator


class DumperBase(DataStreamProcessor):

    def __init__(self, options={}):
        super(DumperBase, self).__init__()
        counters = options.get('counters', {})
        self.datapackage_rowcount = counters.get('datapackage-rowcount', 'count_of_rows')
        self.datapackage_bytes = counters.get('datapackage-bytes', 'bytes')
        self.datapackage_hash = counters.get('datapackage-hash', 'hash')
        self.resource_rowcount = counters.get('resource-rowcount', 'count_of_rows')
        self.resource_bytes = counters.get('resource-bytes', 'bytes')
        self.resource_hash = counters.get('resource-hash', 'hash')
        self.add_filehash_to_path = options.get('add_filehash_to_path', False)
        self.pretty_descriptor = options.get('pretty_descriptor', True)
        self.schema_validator_options = options.get('validator_options', {})

    @staticmethod
    def get_attr(obj, prop, default=None):
        if prop is None:
            return
        prop = prop.split('.')
        while len(prop) > 1:
            obj = obj.get(prop.pop(0), {})
        prop = prop.pop(0)
        return obj.get(prop, default)

    @staticmethod
    def set_attr(obj, prop, value):
        if prop is None:
            return
        prop = prop.split('.')
        while len(prop) > 1:
            obj = obj.setdefault(prop.pop(0), {})
        prop = prop.pop(0)
        obj[prop] = value

    @staticmethod
    def inc_attr(obj, prop, value):
        if prop is None:
            return
        prop = prop.split('.')
        while len(prop) > 1:
            obj = obj.setdefault(prop.pop(0), {})
        prop = prop.pop(0)
        obj.setdefault(prop, 0)
        obj[prop] += value

    @staticmethod
    def insert_hash_in_path(descriptor, hash):
        path = descriptor.get('path')
        if isinstance(path, list):
            if len(path) > 0:
                path = path[0]

        assert isinstance(path, str), '%r' % path

        dir_name = os.path.dirname(path)
        file_name = os.path.basename(path)
        descriptor['path'] = os.path.join(dir_name, hash, file_name)

    def row_counter(self, resource, iterator):
        counter = 0
        for row in iterator:
            counter += 1
            yield row
        DumperBase.inc_attr(self.datapackage.descriptor, self.datapackage_rowcount, counter)
        DumperBase.inc_attr(resource.res.descriptor, self.resource_rowcount, counter)
        resource.res.commit()
        self.datapackage.commit()

    def process_resources(self, resources):
        self.initialize()

        resource: ResourceWrapper = None
        for resource in resources:
            ret = self.process_resource(
                        ResourceWrapper(
                            resource.res,
                            schema_validator(resource.res, resource,
                                             **self.schema_validator_options)
                        )
            )
            ret = self.row_counter(resource, ret)
            yield ret

        # Calculate datapackage hash
        if self.datapackage_hash:
            datapackage_hash = hashlib.md5(
                        json.dumps(self.datapackage.descriptor,
                                   indent=2 if self.pretty_descriptor else None,
                                   sort_keys=True,
                                   ensure_ascii=True).encode('ascii')
                    ).hexdigest()
            DumperBase.set_attr(self.datapackage.descriptor, self.datapackage_hash, datapackage_hash)

        self.handle_datapackage()
        self.finalize()

    def handle_datapackage(self):
        self.datapackage.commit()
        self.stats['count_of_rows'] = DumperBase.get_attr(self.datapackage.descriptor, self.datapackage_rowcount)
        self.stats['bytes'] = DumperBase.get_attr(self.datapackage.descriptor, self.datapackage_bytes)
        self.stats['hash'] = DumperBase.get_attr(self.datapackage.descriptor, self.datapackage_hash)
        self.stats['dataset_name'] = self.datapackage.descriptor.get('name')

    def initialize(self):
        pass

    def finalize(self):
        pass
