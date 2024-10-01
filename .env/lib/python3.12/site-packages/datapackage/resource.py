from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import io
import os
import six
import json
import warnings
try:
    from cchardet import detect
except ImportError:
    from chardet import detect
import requests
from copy import deepcopy
from tableschema import Table, Storage
from six.moves.urllib.parse import urljoin, urlparse
from six.moves.urllib.request import urlopen
from .profile import Profile
from . import exceptions
from . import helpers
from . import config


# Module API

class Resource(object):
    """Resource represenation

    # Arguments
        descriptor (str/dict): data resource descriptor as local path, url or object
        base_path (str): base path for all relative paths
        strict (bool):
            strict flag to alter validation behavior.  Setting it to `true`
            leads to throwing errors on any operation with invalid descriptor
        unsafe (bool):
            if `True` unsafe paths will be allowed. For more inforamtion
            https\\://specs.frictionlessdata.io/data-resource/#data-location.
            Default to `False`
        storage (str/tableschema.Storage): storage name like `sql` or storage instance
        options (dict): storage options to use for storage creation

    # Raises
        DataPackageException: raises error if something goes wrong

    """

    # Public

    def __init__(self, descriptor={}, base_path=None, strict=False, unsafe=False, storage=None,
                 # Internal
                 package=None, **options):

        # Get base path
        if base_path is None:
            base_path = helpers.get_descriptor_base_path(descriptor)

        # Instantiate storage
        if storage and not isinstance(storage, Storage):
            storage = Storage.connect(storage, **options)

        # Process descriptor
        descriptor = helpers.retrieve_descriptor(descriptor)
        descriptor = helpers.dereference_resource_descriptor(descriptor, base_path)

        # Handle deprecated resource.path.url
        if descriptor.get('url'):
            warnings.warn(
                'Resource property "url: <url>" is deprecated. '
                'Please use "path: <url>" instead.',
                UserWarning)
            descriptor['path'] = descriptor['url']
            del descriptor['url']

        # Set attributes
        self.__current_descriptor = deepcopy(descriptor)
        self.__next_descriptor = deepcopy(descriptor)
        self.__base_path = base_path
        self.__package = package
        self.__storage = storage
        self.__relations = None
        self.__strict = strict
        self.__unsafe = unsafe
        self.__table = None
        self.__errors = []
        self.__table_options = options

        # Build resource
        self.__build()

    @property
    def package(self):
        """Package instance if the resource belongs to some package

        # Returns
            Package/None: a package instance if available

        """
        return self.__package

    @property
    def valid(self):
        """Validation status

        Always true in strict mode.

        # Returns
            bool: validation status

        """
        return not bool(self.__errors)

    @property
    def errors(self):
        """Validation errors

        Always empty in strict mode.

        # Returns
            Exception[]: validation errors

        """
        return self.__errors

    @property
    def profile(self):
        """Resource's profile

        # Returns
            Profile: an instance of `Profile` class

        """
        return self.__profile

    @property
    def descriptor(self):
        """Package's descriptor

        # Returns
            dict: descriptor

        """
        # Never use self.descriptor inside self class (!!!)
        return self.__next_descriptor

    @property
    def group(self):
        """Group name

        # Returns
            str: group name

        """
        return self.__current_descriptor.get('group')

    @property
    def name(self):
        """Resource name

        # Returns
            str: name

        """
        return self.__current_descriptor.get('name')

    @property
    def inline(self):
        """Whether resource inline

        # Returns
            bool: returns true if resource is inline

        """
        return self.__source_inspection.get('inline', False)

    @property
    def local(self):
        """Whether resource local

        # Returns
            bool: returns true if resource is local

        """
        return self.__source_inspection.get('local', False)

    @property
    def remote(self):
        """Whether resource remote

        # Returns
            bool: returns true if resource is remote

        """
        return self.__source_inspection.get('remote', False)

    @property
    def multipart(self):
        """Whether resource multipart

        # Returns
            bool: returns true if resource is multipart

        """
        return self.__source_inspection.get('multipart', False)

    @property
    def tabular(self):
        """Whether resource tabular

        # Returns
            bool: returns true if resource is tabular

        """
        if self.__current_descriptor.get('profile') == 'tabular-data-resource':
            return True
        if not self.__strict:
            if self.__current_descriptor.get('format') in config.TABULAR_FORMATS:
                return True
            if self.__source_inspection.get('tabular', False):
                return True
        return False

    @property
    def source(self):
        """Resource's source

        Combination of `resource.source` and `resource.inline/local/remote/multipart`
        provides predictable interface to work with resource data.

        # Returns
            list/str: returns `data` or `path` property

        """
        return self.__source_inspection.get('source')

    @property
    def headers(self):
        """Resource's headers

        > Only for tabular resources (reading has to be started first or it's `None`)

        # Returns
            str[]/None: returns data source headers

        """
        if not self.tabular:
            return None
        return self.__get_table().headers

    @property
    def schema(self):
        """Resource's schema

        > Only for tabular resources

        For tabular resources it returns `Schema` instance to interact with data schema.
        Read API documentation - [tableschema.Schema](https://github.com/frictionlessdata/tableschema-py#schema).

        # Returns
            tableschema.Schema: schema

        """
        if not self.tabular:
            return None
        return self.__get_table().schema

    def iter(self, integrity=False, relations=False, **options):
        """Iterates through the resource data and emits rows cast based on table schema.

        > Only for tabular resources

        # Arguments

            keyed (bool):
                yield keyed rows in a form of `{header1\\: value1, header2\\: value2}`
                (default is false; the form of rows is `[value1, value2]`)

            extended (bool):
                yield extended rows in a for of `[rowNumber, [header1, header2], [value1, value2]]`
                (default is false; the form of rows is `[value1, value2]`)

            cast (bool):
                disable data casting if false
                (default is true)

            integrity (bool):
                if true actual size in BYTES and SHA256 hash of the file
                will be checked against `descriptor.bytes` and `descriptor.hash`
                (other hashing algorithms are not supported and will be skipped silently)

            relations (bool):
                if true foreign key fields will be checked and resolved to its references

            foreign_keys_values (dict):
                three-level dictionary of foreign key references optimized
                to speed up validation process in a form of
                `{resource1\\: {(fk_field1, fk_field2)\\: {(value1, value2)\\: {one_keyedrow}, ... }}}`.
                If not provided but relations is true, it will be created
                before the validation process by *index_foreign_keys_values* method

            exc_handler (func):
                optional custom exception handler callable.
                Can be used to defer raising errors (i.e. "fail late"), e.g.
                for data validation purposes. Must support the signature below

        # Custom exception handler

        ```python
        def exc_handler(exc, row_number=None, row_data=None, error_data=None):
            '''Custom exception handler (example)

            # Arguments:
                exc(Exception):
                    Deferred exception instance
                row_number(int):
                    Data row number that triggers exception exc
                row_data(OrderedDict):
                    Invalid data row source data
                error_data(OrderedDict):
                    Data row source data field subset responsible for the error, if
                    applicable (e.g. invalid primary or foreign key fields). May be
                    identical to row_data.
            '''
            # ...
        ```

        # Raises
            DataPackageException: base class of any error
            CastError: data cast error
            IntegrityError: integrity checking error
            UniqueKeyError: unique key constraint violation
            UnresolvedFKError: unresolved foreign key reference error

        # Returns
            Iterator[list]: yields rows

        """

        # Error for non tabular
        if not self.tabular:
            message = 'Methods iter/read are not supported for non tabular data'
            raise exceptions.DataPackageException(message)

        # Get integrity
        if integrity:
            integrity = self.__get_integrity()

        # Get relations
        if relations:
            relations = self.__get_relations()

        return self.__get_table().iter(
            integrity=integrity, relations=relations, **options)

    def read(self, integrity=False, relations=False, foreign_keys_values=False, **options):
        """Read the whole resource and return as array of rows

        > Only for tabular resources
        > It has the same API as `resource.iter` except for

        # Arguments
            limit (int): limit count of rows to read and return

        # Returns
            list[]: returns rows

        """

        # Error for non tabular
        if not self.tabular:
            message = 'Methods iter/read are not supported for non tabular data'
            raise exceptions.DataPackageException(message)

        # Get integrity
        if integrity:
            integrity = self.__get_integrity()

        # Get relations
        if relations and not foreign_keys_values:
            relations = self.__get_relations()

        return self.__get_table().read(
            integrity=integrity, relations=relations,
            foreign_keys_values=foreign_keys_values, **options)

    def check_integrity(self):
        """Checks resource integrity

        > Only for tabular resources

        It checks size in BYTES and SHA256 hash of the file
        against `descriptor.bytes` and `descriptor.hash`
        (other hashing algorithms are not supported and will be skipped silently).

        # Raises
            exceptions.IntegrityError: raises if there are integrity issues

        # Returns
            bool: returns True if no issues

        """
        # This function will benefit from rebasing it on `resource.raw_iter
        for row in self.iter(integrity=True, cast=False):
            pass
        return True

    def check_relations(self, foreign_keys_values=False):
        """Check relations

        > Only for tabular resources

        It checks foreign keys and raises an exception if there are integrity issues.

        # Raises
            exceptions.RelationError: raises if there are relation issues

        # Returns
            bool: returns True if no issues

        """
        for row in self.iter(relations=True, foreign_keys_values=foreign_keys_values):
            pass
        return True

    def drop_relations(self):
        """Drop relations

        > Only for tabular resources

        Remove relations data from memory

        # Returns
            bool: returns True

        """
        self.__relations = False
        return self.__relations is False

    def raw_iter(self, stream=False):
        """Iterate over data chunks as bytes.

        If `stream` is true File-like object will be returned.

        # Arguments
            stream (bool): File-like object will be returned

        # Returns
            bytes[]/filelike: returns bytes[]/filelike

        """

        # Error for inline
        if self.inline:
            message = 'Methods raw_iter/raw_read are not supported for inline data'
            raise exceptions.DataPackageException(message)

        # Get filelike
        if self.multipart:
            filelike = _MultipartSource(self)
        elif self.remote:
            if self.__table_options.get('http_session'):
                http_session = self.__table_options['http_session']
            else:
                http_session = requests.Session()
                http_session.headers = config.HTTP_HEADERS
            res = http_session.get(self.source, stream=True)
            filelike = res.raw
        else:
            filelike = io.open(self.source, 'rb')

        return filelike

    def raw_read(self):
        """Returns resource data as bytes.

        # Returns
            bytes: returns resource data in bytes

        """
        contents = b''
        with self.raw_iter() as filelike:
            for chunk in filelike:
                contents += chunk
        return contents

    def infer(self, **options):
        """Infer resource metadata

        Like name, format, mediatype, encoding, schema and profile.
        It commits this changes into resource instance.

        # Arguments
            options:
                options will be passed to `tableschema.infer` call,
                for more control on results (e.g. for setting `limit`, `confidence` etc.).

        # Returns
            dict: returns resource descriptor

        """
        descriptor = deepcopy(self.__current_descriptor)

        # Blank -> Stop
        if self.__source_inspection.get('blank'):
            return descriptor

        # Name
        if not descriptor.get('name'):
            descriptor['name'] = self.__source_inspection['name']

        # Only for non inline/storage
        if not self.inline and not self.__storage:

            # Format
            if not descriptor.get('format'):
                descriptor['format'] = self.__source_inspection['format']

            # Mediatype
            if not descriptor.get('mediatype'):
                descriptor['mediatype'] = 'text/%s' % descriptor['format']

            # Encoding
            if not descriptor.get('encoding'):
                contents = b''
                with self.raw_iter(stream=True) as stream:
                    for chunk in stream:
                        contents += chunk
                        if len(contents) > 1000: break
                encoding = detect(contents)['encoding']
                if encoding is not None:
                    encoding = encoding.lower()
                    descriptor['encoding'] = 'utf-8' if encoding == 'ascii' else encoding

        # Schema
        if not descriptor.get('schema'):
            if self.tabular:
                descriptor['schema'] = self.__get_table().infer(**options)

        # Profile
        if descriptor.get('profile') == config.DEFAULT_RESOURCE_PROFILE:
            if self.tabular:
                descriptor['profile'] = 'tabular-data-resource'

        # Save descriptor
        self.__current_descriptor = descriptor
        self.__build()

        return descriptor

    def commit(self, strict=None):
        """Update resource instance if there are in-place changes in the descriptor.

        # Arguments
            strict (bool): alter `strict` mode for further work

        # Raises
            DataPackageException: raises error if something goes wrong

        # Returns
            bool: returns true on success and false if not modified

        """
        if strict is not None:
            self.__strict = strict
        elif self.__current_descriptor == self.__next_descriptor:
            return False
        self.__current_descriptor = deepcopy(self.__next_descriptor)
        self.__table = None
        self.__build()
        return True

    def save(self, target, storage=None, to_base_path=False, **options):
        """Saves this resource

        Into storage if `storage` argument is passed or
        saves this resource's descriptor to json file otherwise.

        # Arguments
            target (str):
                path where to save a resource
            storage (str/tableschema.Storage):
                storage name like `sql` or storage instance
            to_base_path (bool):
                save the resource to the resource's base path
                using the "<base_path>/<target>" route
            options (dict):
                storage options to use for storage creation

        # Raises
            DataPackageException: raises error if something goes wrong

        # Returns
            bool: returns true on success
        """

        # Save resource to storage
        if storage is not None:
            if self.tabular:
                self.infer()
                storage.create(target, self.schema.descriptor, force=True)
                storage.write(target, self.iter())

        # Save descriptor to json
        else:
            mode = 'w'
            encoding = 'utf-8'
            if six.PY2:
                mode = 'wb'
                encoding = None
            json_target = target
            if not os.path.isabs(json_target) and to_base_path:
                if not self.__unsafe and not helpers.is_safe_path(target):
                    raise exceptions.DataPackageException('Target path "%s" is not safe', target)
                json_target = os.path.join(self.__base_path, target)
            else:
                helpers.ensure_dir(target)
            with io.open(json_target, mode=mode, encoding=encoding) as file:
                json.dump(self.__current_descriptor, file, indent=4)

    # Private

    def __build(self):

        # Process descriptor
        expand = helpers.expand_resource_descriptor
        self.__current_descriptor = expand(self.__current_descriptor)
        self.__next_descriptor = deepcopy(self.__current_descriptor)

        # Inspect source
        self.__source_inspection = _inspect_source(
            self.__current_descriptor.get('data'),
            self.__current_descriptor.get('path'),
            base_path=self.__base_path,
            unsafe=self.__unsafe,
            storage=self.__storage)

        # Instantiate profile
        self.__profile = Profile(self.__current_descriptor.get('profile'))

        # Validate descriptor
        try:
            self.__profile.validate(self.__current_descriptor)
            self.__errors = []
        except exceptions.ValidationError as exception:
            self.__errors = exception.errors
            if self.__strict:
                raise exception

    def __get_table(self):
        if not self.__table:

            # Non tabular -> None
            if not self.tabular:
                return None

            # Get source/schema
            source = self.source
            if self.multipart:
                source = _MultipartSource(self)
            schema = self.__current_descriptor.get('schema')

            # Storage resource
            if self.__storage is not None:
                self.__table = Table(source, schema=schema, storage=self.__storage)

            # General resource
            else:
                options = self.__table_options
                descriptor = self.__current_descriptor
                # TODO: this option is experimental
                options['scheme'] = descriptor.get('scheme')
                options['format'] = descriptor.get('format', 'csv')
                if descriptor.get('data'):
                    options['format'] = 'inline'
                if descriptor.get('encoding'):
                    options['encoding'] = descriptor['encoding']
                if descriptor.get('compression'):
                    options['compression'] = descriptor['compression']
                # TODO: these options are experimental
                options['pick_fields'] = descriptor.get(
                    'pickFields', options.get('pick_fields', None))
                options['skip_fields'] = descriptor.get(
                    'skipFields', options.get('skip_fields', None))
                options['pick_rows'] = descriptor.get(
                    'pickRows', options.get('pick_rows', []))
                options['skip_rows'] = descriptor.get(
                    'skipRows', options.get('skip_rows', []))
                # TODO: these options are depricated
                options['pick_fields'] = descriptor.get(
                    'pickColumns', options.get('pick_columns', None))
                options['skip_fields'] = descriptor.get(
                    'skipColumns', options.get('skip_columns', None))
                dialect = descriptor.get('dialect')
                if dialect:
                    if not dialect.get('header', config.DEFAULT_DIALECT['header']):
                        fields = descriptor.get('schema', {}).get('fields', [])
                        options['headers'] = [field['name'] for field in fields] or None
                    for key in _DIALECT_KEYS:
                        if key in dialect:
                            options[key.lower()] = dialect[key]
                self.__table = Table(source, schema=schema, **options)

        return self.__table

    def __get_integrity(self):
        return {
            'size': self.__current_descriptor.get('bytes'),
            'hash': helpers.extract_sha256_hash(self.__current_descriptor.get('hash')),
        }

    def __get_relations(self):
        if not self.__relations:

            # Prepare resources
            resources = {}
            if self.__get_table() and self.__get_table().schema:
                for fk in self.__get_table().schema.foreign_keys:
                    resources.setdefault(fk['reference']['resource'], [])
                    for field in fk['reference']['fields']:
                        resources[fk['reference']['resource']].append(field)

            # Fill relations
            self.__relations = {}
            for resource, fields in resources.items():
                if resource and not self.__package:
                    continue
                self.__relations.setdefault(resource, [])
                data = self.__package.get_resource(resource) if resource else self
                if data.tabular:
                    self.__relations[resource] = data.read(keyed=True)

        return self.__relations

    def get_foreign_keys_values(self):
        # need to access it from groups for optimization
        return self.__get_table().index_foreign_keys_values(self.__get_relations())

    # Deprecated

    @property
    def table(self):
        """Return resource table
        """

        # Deprecate
        warnings.warn(
            'Property "resource.table" is deprecated. '
            'Please use "resource.iter/read" directly.',
            UserWarning)

        return self.__get_table()

    @property
    def data(self):
        """Return resource data
        """

        # Deprecate
        warnings.warn(
            'Property "resource.data" is deprecated. '
            'Please use "resource.read(keyed=True)" instead.',
            UserWarning)

        return self.read(keyed=True)


# Internal

_DIALECT_KEYS = [
    'delimiter',
    'doubleQuote',
    'lineTerminator',
    'quoteChar',
    'escapeChar',
    'skipInitialSpace',
]


def _inspect_source(data, path, base_path=None, unsafe=False, storage=None):
    inspection = {}

    # Normalize path
    if path and not isinstance(path, list):
        path = [path]

    # Blank
    if not data and not path:
        inspection['source'] = None
        inspection['blank'] = True

    # Storage
    elif storage is not None:
        inspection['name'] = path[0]
        inspection['source'] = path[0]
        inspection['tabular'] = True

    # Inline
    elif data is not None:
        inspection['name'] = 'inline'
        inspection['source'] = data
        inspection['inline'] = True
        inspection['tabular'] = isinstance(data, list)

    # Local/Remote
    elif len(path) == 1:

        # Remote
        if urlparse(path[0]).scheme in config.REMOTE_SCHEMES:
            inspection['source'] = path[0]
            inspection['remote'] = True
        elif base_path and urlparse(base_path).scheme in config.REMOTE_SCHEMES:
            norm_base_path = base_path if base_path.endswith('/') else base_path + '/'
            inspection['source'] = urljoin(norm_base_path, path[0])
            inspection['remote'] = True

        # Local
        else:

            # Path is not safe
            if not unsafe and not helpers.is_safe_path(path[0]):
                raise exceptions.DataPackageException(
                    'Local path "%s" is not safe' % path[0])

            # Not base path
            if not base_path:
                raise exceptions.DataPackageException(
                    'Local path "%s" requires base path' % path[0])

            inspection['source'] = os.path.join(base_path, path[0])
            inspection['local'] = True

        # Inspect
        filename = os.path.basename(path[0])
        inspection['format'] = os.path.splitext(filename)[1][1:]
        inspection['name'] = os.path.splitext(filename)[0]
        inspection['tabular'] = inspection['format'] in config.TABULAR_FORMATS

    # Multipart Local/Remote
    elif len(path) > 1:
        inspect = lambda item: _inspect_source(None, item, base_path=base_path, unsafe=unsafe)
        inspections = list(map(inspect, path))
        inspection.update(inspections[0])
        inspection['source'] = list(map(lambda item: item['source'], inspections))
        inspection['multipart'] = True

    return inspection


class _MultipartSource(object):

    # Public

    def __init__(self, resource):
        # testing if we have headers
        if resource.tabular \
           and (resource.descriptor.get('dialect') and resource.descriptor.get('dialect').get('header')
               or (not resource.descriptor.get('dialect') and config.DEFAULT_DIALECT['header'])):
            remove_chunk_header_row = True
        else:
            remove_chunk_header_row = False
        self.__source = resource.source
        self.__remote = resource.remote
        self.__remove_chunk_header_row = remove_chunk_header_row
        self.__rows = self.__iter_rows()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def __iter__(self):
        return self.__rows

    @property
    def closed(self):
        return False

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    def close(self):
        pass

    def flush(self):
        pass

    def read1(self, size):
        return self.read(size)

    def seek(self, offset):
        assert offset == 0
        self.__rows = self.__iter_rows()

    def read(self, size):
        res = b''
        while True:
            try:
                res += next(self.__rows)
            except StopIteration:
                break
            if len(res) > size:
                break
        return res

    # Private

    def __iter_rows(self):
        streams = []
        if self.__remote:
            streams = (urlopen(chunk) for chunk in self.__source)
        else:
            streams = (io.open(chunk, 'rb') for chunk in self.__source)
        firstStream = True
        header_row = None
        for stream, chunk in zip(streams, self.__source):
            firstRow = True
            for row in stream:
                if not row.endswith(b'\n'):
                    row += b'\n'
                # if tabular, skip header row in the concatenation stream
                if firstRow and self.__remove_chunk_header_row:
                    if firstStream:
                        # store the first stream header row and yield it
                        header_row = row
                        yield row
                    elif row == header_row:
                        # remove header row of new stream is same as header from first stream
                        pass
                    else:
                        # yield this first row but warn the user for deprecated situation
                        # TODO: this warning might be removed in future releases ?
                        warnings.warn("""%s has no headers whereas header = True.
                            Deprecated legacy multi-part mode for tabular data.
                            Headers will be required in chunks/multiparts in future.""" % chunk, UserWarning)
                        yield row
                else:
                    yield row
                firstRow = False
            firstStream = False
