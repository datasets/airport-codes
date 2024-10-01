import os
import warnings
import datetime

from datapackage import Package
from tabulator import Stream
from tableschema.schema import Schema
from .. import DataStreamProcessor
from ..base.exceptions import SourceLoadError
from ..base.schema_validator import schema_validator, ignore, drop, raise_exception, clear
from ..helpers.resource_matcher import ResourceMatcher

from .parsers import XMLParser, ExcelXMLParser, ExtendedSQLParser, GeoJsonParser


class StringsGuesser():
    def cast(self, value):
        return [('string', 'default', 0)]


class TypesGuesser():
    def cast(self, value):
        jts_type = {
            str: 'string',
            int: 'integer',
            float: 'number',
            list: 'array',
            dict: 'object',
            tuple: 'array',
            bool: 'boolean',
            datetime.datetime: 'datetime',
            datetime.date: 'date',
        }.get(type(value))
        ret = [('any', 'default', 0)]
        if jts_type is not None:
            ret.append(('jts_type', 'default', 1))
        return ret


class load(DataStreamProcessor):

    INFER_STRINGS = 'strings'
    INFER_PYTHON_TYPES = 'pytypes'
    INFER_FULL = 'full'

    CAST_TO_STRINGS = 'strings'
    CAST_DO_NOTHING = 'nothing'
    CAST_WITH_SCHEMA = 'schema'

    ERRORS_IGNORE = ignore
    ERRORS_DROP = drop
    ERRORS_RAISE = raise_exception
    ERRORS_CLEAR = clear

    def __init__(self, load_source, name=None, resources=None, strip=True, limit_rows=None,
                 infer_strategy=None, cast_strategy=None,
                 override_schema=None, override_fields=None,
                 extract_missing_values=None,
                 deduplicate_headers=False,
                 on_error=raise_exception,
                 **options):
        super(load, self).__init__()
        self.load_source = load_source

        self.name = name
        self.strip = strip
        self.limit_rows = limit_rows
        self.options = options
        self.resources = resources
        self.override_schema = override_schema
        self.override_fields = override_fields
        self.deduplicate_headers = deduplicate_headers

        # Extract missing values
        self.extract_missing_values = None
        if extract_missing_values is not None:
            if isinstance(extract_missing_values, bool):
                extract_missing_values = {}
            extract_missing_values.setdefault('source', None)
            extract_missing_values.setdefault('target', 'missingValues')
            extract_missing_values.setdefault('values', [])
            if isinstance(extract_missing_values.get('source'), str):
                extract_missing_values['source'] = [extract_missing_values['source']]
            self.extract_missing_values = extract_missing_values

        self.load_dp = None
        self.resource_descriptors = []
        self.iterators = []

        if 'force_strings' in options:
            warnings.warn('force_strings is being deprecated, use infer_strategy & cast_strategy instead',
                          DeprecationWarning)
            if options['force_strings']:
                infer_strategy = self.INFER_STRINGS
                cast_strategy = self.CAST_TO_STRINGS

        if 'validate' in options:
            warnings.warn('validate is being deprecated, use cast_strategy & on_error instead',
                          DeprecationWarning)
            if options['validate']:
                cast_strategy = self.CAST_WITH_SCHEMA

        # Force strings from stream for the INFER_STRINGS strategy
        if infer_strategy == self.INFER_STRINGS:
            self.options['force_strings'] = True

        self.guesser = {
            self.INFER_FULL: None,
            self.INFER_PYTHON_TYPES: TypesGuesser,
            self.INFER_STRINGS: StringsGuesser,
        }[infer_strategy or self.INFER_FULL]

        self.caster = {
            self.CAST_DO_NOTHING: lambda res, it: it,
            self.CAST_WITH_SCHEMA: lambda res, it: schema_validator(res, it, on_error=on_error),
            self.CAST_TO_STRINGS: lambda res, it: self.stringer(it)
        }[cast_strategy or self.CAST_DO_NOTHING]

    def process_datapackage(self, dp: Package):
        try:
            return self.safe_process_datapackage(dp)
        except Exception as e:
            raise SourceLoadError('Failed to load source {!r} and options {!r}: {}'
                                  .format(self.load_source, self.options, e)) from e

    @classmethod
    def get_custom_parsers(cls, custom_parsers=None):
        custom_parsers = custom_parsers or dict()
        custom_parsers.setdefault('xml', XMLParser)
        custom_parsers.setdefault('excel-xml', ExcelXMLParser)
        custom_parsers.setdefault('sql', ExtendedSQLParser)
        custom_parsers.setdefault('geojson', GeoJsonParser)
        return custom_parsers

    def safe_process_datapackage(self, dp: Package):

        # If loading from datapackage & resource iterator:
        if isinstance(self.load_source, tuple):
            datapackage_descriptor, resource_iterator = self.load_source
            resources = datapackage_descriptor['resources']
            resource_matcher = ResourceMatcher(self.resources, datapackage_descriptor)
            for resource_descriptor in datapackage_descriptor['resources']:
                if resource_matcher.match(resource_descriptor['name']):
                    self.resource_descriptors.append(resource_descriptor)
            self.iterators = (resource for resource, descriptor in zip(resource_iterator, resources)
                              if resource_matcher.match(descriptor['name']))

        # If load_source is string:
        else:
            # Handle Environment vars if necessary:
            if self.load_source.startswith('env://'):
                env_var = self.load_source[6:]
                self.load_source = os.environ.get(env_var)
                if self.load_source is None:
                    raise ValueError(f"Couldn't find value for env var '{env_var}'")

            # Loading from datapackage:
            if os.path.basename(self.load_source) == 'datapackage.json' or self.options.get('format') == 'datapackage':
                self.load_dp = Package(self.load_source)
                resource_matcher = ResourceMatcher(self.resources, self.load_dp)
                for resource in self.load_dp.resources:
                    if resource_matcher.match(resource.name):
                        self.resource_descriptors.append(resource.descriptor)
                        self.iterators.append(resource.iter(keyed=True, cast=True))

            # Loading for any other source
            else:
                path = os.path.basename(self.load_source)
                path = os.path.splitext(path)[0]
                descriptor = dict(path=self.name or path,
                                  profile='tabular-data-resource')
                self.resource_descriptors.append(descriptor)
                descriptor['name'] = self.name or path
                if 'encoding' in self.options:
                    descriptor['encoding'] = self.options['encoding']
                self.options['custom_parsers'] = self.get_custom_parsers(self.options.get('custom_parsers'))
                self.options.setdefault('ignore_blank_headers', True)
                if 'headers' not in self.options:
                    self.options.setdefault('skip_rows', [{'type': 'preset', 'value': 'auto'}])
                self.options.setdefault('headers', 1)
                self.options.setdefault('sample_size', 1000)
                stream: Stream = Stream(self.load_source, **self.options).open()
                if len(stream.headers) != len(set(stream.headers)):
                    if not self.deduplicate_headers:
                        raise ValueError(
                            'Found duplicate headers.' +
                            'Use the `deduplicate_headers` flag (found headers=%r)' % stream.headers)
                    stream.headers = self.rename_duplicate_headers(stream.headers)
                schema = Schema(self.override_schema or {}).infer(
                    stream.sample, headers=stream.headers,
                    confidence=1, guesser_cls=self.guesser)
                # restore schema field names to original headers
                for header, field in zip(stream.headers, schema['fields']):
                    field['name'] = header
                if self.override_schema:
                    schema.update(self.override_schema)
                if self.override_fields:
                    fields = schema.get('fields', [])
                    for field in fields:
                        field.update(self.override_fields.get(field['name'], {}))
                if self.extract_missing_values:
                    missing_values = schema.get('missingValues', [])
                    if not self.extract_missing_values['values']:
                        self.extract_missing_values['values'] = missing_values
                    schema['fields'].append({
                        'name': self.extract_missing_values['target'],
                        'type': 'object',
                        'format': 'default',
                        'values': self.extract_missing_values['values'],
                    })
                descriptor['schema'] = schema
                descriptor['format'] = self.options.get('format', stream.format)
                descriptor['path'] += '.{}'.format(stream.format)
                self.iterators.append(stream.iter(keyed=True))
        dp.descriptor.setdefault('resources', []).extend(self.resource_descriptors)
        return dp

    def stripper(self, iterator):
        whitespace = set(' \t\n\r')
        for r in iterator:
            for k, v in r.items():
                if v and isinstance(v, str) and (v[-1] in whitespace or v[0] in whitespace):
                    r[k] = v.strip()
            yield r
            # yield dict(
            #     (k, v.strip()) if isinstance(v, str) else (k, v)
            #     for k, v in r.items()
            # )

    def limiter(self, iterator):
        count = 0
        for row in iterator:
            yield row
            count += 1
            if count >= self.limit_rows:
                break

    def stringer(self, iterator):
        for r in iterator:
            yield dict(
                (k, str(v)) if not isinstance(v, str) else (k, v)
                for k, v in r.items()
            )

    def missing_values_extractor(self, iterator):
        source = self.extract_missing_values['source']
        target = self.extract_missing_values['target']
        values = self.extract_missing_values['values']
        for row in iterator:
            mapping = {}
            if values:
                for key, value in row.items():
                    if not source or key in source:
                        if value in values:
                            mapping[key] = value
            row[target] = mapping
            yield row

    def process_resources(self, resources):
        yield from super(load, self).process_resources(resources)
        for descriptor, it in zip(self.resource_descriptors, self.iterators):
            if self.extract_missing_values:
                it = self.missing_values_extractor(it)
            it = self.caster(descriptor, it)
            if self.strip:
                it = self.stripper(it)
            if self.limit_rows:
                it = self.limiter(it)
            yield it

    @staticmethod
    def rename_duplicate_headers(duplicate_headers):
        counter = {}
        headers = []
        for header in duplicate_headers:
            counter.setdefault(header, 0)
            counter[header] += 1
            if counter[header] > 1:
                if counter[header] == 2:
                    headers[headers.index(header)] = '%s (%s)' % (header, 1)
                header = '%s (%s)' % (header, counter[header])
            headers.append(header)
        return headers
