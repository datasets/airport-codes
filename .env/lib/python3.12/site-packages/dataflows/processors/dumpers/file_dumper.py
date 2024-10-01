import os
import json
import tempfile
import hashlib
from typing import Dict
from dataflows.base.resource_wrapper import ResourceWrapper

from datapackage import Resource

from .dumper_base import DumperBase
from .formats import CSVFormat, JSONFormat, GeoJSONFormat, ExcelFormat, FileFormat


# see https://stackoverflow.com/questions/7150826/how-can-i-get-the-default-file-permissions-in-python
def UmaskNamedTemporaryFile(*args, **kargs):
    fdesc = tempfile.NamedTemporaryFile(*args, **kargs)
    # we need to set umask to get its current value. As noted
    # by Florian Brucker (comment), this is a potential security
    # issue, as it affects all the threads. Considering that it is
    # less a problem to create a file with permissions 000 than 666,
    # we use 666 as the umask temporary value.
    umask = os.umask(0o666)
    os.umask(umask)
    os.chmod(fdesc.name, 0o666 & ~umask)
    return fdesc


class FileDumper(DumperBase):

    def __init__(self, options: dict):
        super(FileDumper, self).__init__(options)
        self.force_format = options.pop('force_format', True)
        self.forced_format = options.pop('format', 'csv')
        self.temporal_format_property = options.pop('temporal_format_property', None)
        self.use_titles = options.pop('use_titles', False)
        self.writer_options = options.pop('options', dict())
        self.custom_formatters = options.pop('file_formatters', dict())

    def process_datapackage(self, datapackage):
        datapackage = \
            super(FileDumper, self).process_datapackage(datapackage)

        self.file_formatters: Dict[str, FileFormat] = {}

        # Make sure all resources are proper CSVs
        resource: Resource = None
        for i, resource in enumerate(datapackage.resources):
            if self.force_format:
                file_format = self.forced_format
            else:
                _, file_format = os.path.splitext(resource.source)
                file_format = file_format[1:]
            file_formatter = self.custom_formatters.get(file_format) or {
                'csv': CSVFormat,
                'json': JSONFormat,
                'geojson': GeoJSONFormat,
                'excel': ExcelFormat,
                'xlsx': ExcelFormat,
            }.get(file_format)
            if file_formatter is not None:
                self.file_formatters[resource.name] = file_formatter
                self.file_formatters[resource.name].prepare_resource(resource)
                resource.commit()
                datapackage.descriptor['resources'][i] = resource.descriptor

        return datapackage

    def handle_datapackage(self):

        # Handle temporal_format_property
        if self.temporal_format_property:
            for resource in self.datapackage.descriptor['resources']:
                for field in resource['schema']['fields']:
                    if field.get('type') in ['datetime', 'date', 'time']:
                        format = field.pop(self.temporal_format_property, None)
                        if format:
                            field['format'] = format
            self.datapackage.commit()

        temp_file = UmaskNamedTemporaryFile(mode='w+', delete=False, encoding='utf-8')
        indent = 2 if self.pretty_descriptor else None
        json.dump(self.datapackage.descriptor, temp_file, indent=indent, sort_keys=True, ensure_ascii=False)
        temp_file_name = temp_file.name
        filesize = temp_file.tell()
        temp_file.close()
        DumperBase.inc_attr(self.datapackage.descriptor, self.datapackage_bytes, filesize)
        self.write_file_to_output(temp_file_name, 'datapackage.json')
        # if location is not None:
        #     stats.setdefault(STATS_DPP_KEY, {})[STATS_OUT_DP_URL_KEY] = location
        os.unlink(temp_file_name)
        super(FileDumper, self).handle_datapackage()

    def write_file_to_output(self, filename, path):
        raise NotImplementedError()

    def rows_processor(self, resource, writer, temp_file):
        for row in resource:
            writer.write_row(row)
            yield row
        writer.finalize_file()

        # Get resource descriptor
        resource_descriptor = resource.res.descriptor
        for descriptor in self.datapackage.descriptor['resources']:
            if descriptor['name'] == resource.res.descriptor['name']:
                resource_descriptor = descriptor

        # File size:
        filesize = temp_file.tell()
        DumperBase.inc_attr(self.datapackage.descriptor, self.datapackage_bytes, filesize)
        DumperBase.inc_attr(resource_descriptor, self.resource_bytes, filesize)

        # File Hash:
        if self.resource_hash:
            hasher = FileDumper.hash_handler(temp_file)
            # Update path with hash
            if self.add_filehash_to_path:
                DumperBase.insert_hash_in_path(resource_descriptor, hasher.hexdigest())
            DumperBase.set_attr(resource_descriptor, self.resource_hash, hasher.hexdigest())

        # Finalise
        filename = temp_file.name
        temp_file.close()
        self.write_file_to_output(filename, resource.res.source)
        os.unlink(filename)

    def process_resource(self, resource: ResourceWrapper):
        if resource.res.name in self.file_formatters:
            schema = resource.res.schema

            file_formatter = self.file_formatters[resource.res.name]

            temp_file = UmaskNamedTemporaryFile(
                mode=file_formatter.FILE_MODE, delete=False,
                newline='' if 'b' not in file_formatter.FILE_MODE else None
            )
            writer_kwargs = self.writer_options
            if self.use_titles:
                writer_kwargs['use_titles'] = True
            writer_kwargs['temporal_format_property'] = self.temporal_format_property
            writer_kwargs['resource'] = resource.res
            writer = file_formatter(temp_file, schema, **writer_kwargs)

            return self.rows_processor(resource,
                                       writer,
                                       temp_file)
        else:
            return resource

    @staticmethod
    def hash_handler(tfile):
        tfile.seek(0)
        hasher = hashlib.md5()
        data = 'x'
        while len(data) > 0:
            data = tfile.read(1024)
            if isinstance(data, str):
                hasher.update(data.encode('utf8'))
            elif isinstance(data, bytes):
                hasher.update(data)
        return hasher
