from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import io
import re
import six
import json
import copy
import glob
import shutil
import zipfile
import requests
import warnings
import tempfile
from copy import deepcopy
from tableschema import Storage
from .resource import Resource
from .profile import Profile
from .group import Group
from . import exceptions
from . import helpers
from . import config


# Module API

class Package(object):
    """ Package representation

    # Arguments
        descriptor (str/dict): data package descriptor as local path, url or object
        base_path (str): base path for all relative paths
        strict (bool): strict flag to alter validation behavior.
            Setting it to `True` leads to throwing errors
            on any operation with invalid descriptor
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

    def __init__(self, descriptor=None, base_path=None, strict=False, unsafe=False, storage=None,
                 # Deprecated
                 schema=None, default_base_path=None, **options):

        # Handle deprecated schema argument
        if schema is not None:
            warnings.warn(
                'Argument "schema" is deprecated. '
                'Please use "descriptor.profile" property.',
                UserWarning)
            if isinstance(schema, six.string_types):
                if schema in ['base', 'default']:
                    schema = 'data-package'
                elif schema == 'tabular':
                    schema = 'tabular-data-package'
                elif schema == 'fiscal':
                    schema = 'fiscal-data-package'
                if descriptor is None:
                    descriptor = {}
                descriptor['profile'] = schema

        # Handle deprecated default_base_path argument
        if default_base_path is not None:
            warnings.warn(
                'Argument "default_base_path" is deprecated. '
                'Please use "base_path" argument.',
                UserWarning)
            base_path = default_base_path

        # Extract from zip
        tempdir, descriptor = _extract_zip_if_possible(descriptor)
        if tempdir:
            self.__tempdir = tempdir

        # Get base path
        if base_path is None:
            base_path = helpers.get_descriptor_base_path(descriptor)

        # Instantiate storage
        if storage and not isinstance(storage, Storage):
            storage = Storage.connect(storage, **options)

        # Get descriptor from storage
        if storage and not descriptor:
            descriptor = {'resources': []}
            for bucket in storage.buckets:
                descriptor['resources'].append({'path': bucket})

        # Process descriptor
        descriptor = helpers.retrieve_descriptor(descriptor)
        descriptor = helpers.dereference_package_descriptor(descriptor, base_path)

        # Handle deprecated resource.path/url
        for resource in descriptor.get('resources', []):
            url = resource.pop('url', None)
            if url is not None:
                warnings.warn(
                    'Resource property "url: <url>" is deprecated. '
                    'Please use "path: [url]" instead (as array).',
                    UserWarning)
                resource['path'] = [url]

        # Set attributes
        self.__current_descriptor = deepcopy(descriptor)
        self.__next_descriptor = deepcopy(descriptor)
        self.__base_path = base_path
        self.__storage = storage
        self.__strict = strict
        self.__unsafe = unsafe
        self.__resources = []
        self.__errors = []

        # Build package
        self.__build()

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
        """Package's profile

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
        # Never use self.descriptor inside this class (!!!)
        return self.__next_descriptor

    @property
    def base_path(self):
        """Package's base path

        # Returns
            str/None: returns the data package base path

        """
        return self.__base_path

    @property
    def resources(self):
        """Package's resources

        # Returns
            Resource[]: returns an array of `Resource` instances

        """
        return self.__resources

    @property
    def resource_names(self):
        """Package's resource names

        # Returns
            str[]: returns an array of resource names

        """
        return [resource.name for resource in self.resources]

    def get_resource(self, name):
        """Get data package resource by name.

        # Arguments
            name (str): data resource name

        # Returns
            Resource/None: returns `Resource` instances or null if not found

        """
        for resource in self.resources:
            if resource.name == name:
                return resource
        return None

    def add_resource(self, descriptor):
        """Add new resource to data package.

        The data package descriptor will be validated with newly added resource descriptor.

        # Arguments
            descriptor (dict): data resource descriptor

        # Raises
            DataPackageException: raises error if something goes wrong

        # Returns
            Resource/None: returns added `Resource` instance or null if not added

        """
        self.__current_descriptor.setdefault('resources', [])
        self.__current_descriptor['resources'].append(descriptor)
        self.__build()
        return self.__resources[-1]

    def remove_resource(self, name):
        """Remove data package resource by name.

        The data package descriptor will be validated after resource descriptor removal.

        # Arguments
            name (str): data resource name

        # Raises
            DataPackageException: raises error if something goes wrong

        # Returns
            Resource/None: returns removed `Resource` instances or null if not found

        """
        resource = self.get_resource(name)
        if resource:
            predicat = lambda resource: resource.get('name') != name
            self.__current_descriptor['resources'] = list(filter(
                predicat, self.__current_descriptor['resources']))
            self.__build()
        return resource

    def get_group(self, name):
        """Returns a group of tabular resources by name.

        For more information about groups see [Group](#group).

        # Arguments
            name (str): name of a group of resources

        # Raises
            DataPackageException: raises error if something goes wrong

        # Returns
            Group/None: returns a `Group` instance or null if not found

        """
        resources = [resource
            for resource in self.resources
            if resource.tabular and resource.group == name]
        if not resources:
            return None
        return Group(resources)

    def infer(self, pattern=False):
        """Infer a data package metadata.

        > Argument `pattern` works only for local files

        If `pattern` is not provided only existent resources will be inferred
        (added metadata like encoding, profile etc). If `pattern` is provided
        new resoures with file names mathing the pattern will be added and inferred.
        It commits changes to data package instance.

        # Arguments
            pattern (str): glob pattern for new resources

        # Returns
            dict: returns data package descriptor

        """

        # Files
        if pattern:

            # No base path
            if not self.__base_path:
                message = 'Base path is required for pattern infer'
                raise exceptions.DataPackageException(message)

            # Add resources
            options = {'recursive': True} if '**' in pattern else {}
            for path in glob.glob(os.path.join(self.__base_path, pattern), **options):
                self.add_resource({'path': os.path.relpath(path, self.__base_path)})

        # Resources
        for index, resource in enumerate(self.resources):
            descriptor = resource.infer()
            self.__current_descriptor['resources'][index] = descriptor
            self.__build()

        # Profile
        if self.__next_descriptor['profile'] == config.DEFAULT_DATA_PACKAGE_PROFILE:
            if self.resources and all(map(lambda resource: resource.tabular, self.resources)):
                self.__current_descriptor['profile'] = 'tabular-data-package'
                self.__build()

        return self.__current_descriptor

    def commit(self, strict=None):
        """Update data package instance if there are in-place changes in the descriptor.

        # Example

        ```python
        package = Package({
            'name': 'package',
            'resources': [{'name': 'resource', 'data': ['data']}]
        })

        package.name # package
        package.descriptor['name'] = 'renamed-package'
        package.name # package
        package.commit()
        package.name # renamed-package
        ```

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
        self.__build()
        return True

    def save(self, target=None, storage=None, merge_groups=False, to_base_path=False, **options):
        """Saves this data package

        It saves it to storage if `storage` argument is passed or
        saves this data package's descriptor to json file if `target` arguments
        ends with `.json` or saves this data package to zip file otherwise.

        # Example

        It creates a zip file into ``file_or_path`` with the contents
        of this Data Package and its resources. Every resource which content
        lives in the local filesystem will be copied to the zip file.
        Consider the following Data Package descriptor:

        ```json
        {
            "name": "gdp",
            "resources": [
                {"name": "local", "format": "CSV", "path": "data.csv"},
                {"name": "inline", "data": [4, 8, 15, 16, 23, 42]},
                {"name": "remote", "url": "http://someplace.com/data.csv"}
            ]
        }
        ```

        The final structure of the zip file will be:

        ```
        ./datapackage.json
        ./data/local.csv
        ```

        With the contents of `datapackage.json` being the same as
        returned `datapackage.descriptor`. The resources' file names are generated
        based on their `name` and `format` fields if they exist.
        If the resource has no `name`, it'll be used `resource-X`,
        where `X` is the index of the resource in the `resources` list (starting at zero).
        If the resource has `format`, it'll be lowercased and appended to the `name`,
        becoming "`name.format`".

        # Arguments
            target (string/filelike):
                the file path or a file-like object where
                the contents of this Data Package will be saved into.
            storage (str/tableschema.Storage):
                storage name like `sql` or storage instance
            merge_groups (bool):
                save all the group's tabular resoruces into one bucket
                if a storage is provided (for example into one SQL table).
                Read more about [Group](#group).
            to_base_path (bool):
                save the package to the package's base path
                using the "<base_path>/<target>" route
            options (dict):
                storage options to use for storage creation

        # Raises
            DataPackageException: raises if there was some error writing the package

        # Returns
            bool/Storage: on success return true or a `Storage` instance
        """

        # Save package to storage
        if storage is not None:
            if not isinstance(storage, Storage):
                storage = Storage.connect(storage, **options)
            buckets = []
            schemas = []
            sources = []
            group_names = []
            for resource in self.resources:
                if not resource.tabular:
                    continue
                if merge_groups and resource.group:
                    if resource.group in group_names:
                        continue
                    group = self.get_group(resource.group)
                    name = group.name
                    schema = group.schema
                    source = group.iter
                    group_names.append(name)
                else:
                    resource.infer()
                    name = resource.name
                    schema = resource.schema
                    source = resource.iter
                buckets.append(_slugify_resource_name(name))
                schemas.append(schema.descriptor)
                sources.append(source)
            schemas = list(map(_slugify_foreign_key, schemas))
            storage.create(buckets, schemas, force=True)
            for bucket in storage.buckets:
                source = sources[buckets.index(bucket)]
                storage.write(bucket, source())
            return storage

        # Save descriptor to json
        elif str(target).endswith('.json'):
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
                helpers.ensure_dir(json_target)
            with io.open(json_target, mode=mode, encoding=encoding) as file:
                json.dump(self.__current_descriptor, file, indent=4)

        # Save package to zip
        else:
            try:
                with zipfile.ZipFile(target, 'w') as z:
                    descriptor = json.loads(json.dumps(self.__current_descriptor))
                    for index, resource in enumerate(self.resources):
                        if not resource.name:
                            continue
                        if not resource.local:
                            continue
                        path = os.path.abspath(resource.source)
                        basename = resource.descriptor.get('name')
                        resource_format = resource.descriptor.get('format')
                        if resource_format:
                            basename = '.'.join([basename, resource_format.lower()])
                        path_inside_dp = os.path.join('data', basename)
                        z.write(path, path_inside_dp)
                        descriptor['resources'][index]['path'] = path_inside_dp
                    z.writestr('datapackage.json', json.dumps(descriptor, indent=4))
            except (IOError, zipfile.BadZipfile, zipfile.LargeZipFile) as exception:
                six.raise_from(exceptions.DataPackageException(exception), exception)

        return True

    # Private

    def __del__(self):
        if hasattr(self, '_tempdir') and os.path.exists(self.__tempdir):
            shutil.rmtree(self.__tempdir, ignore_errors=True)

    def __build(self):

        # Process descriptor
        expand = helpers.expand_package_descriptor
        self.__current_descriptor = expand(self.__current_descriptor)
        self.__next_descriptor = deepcopy(self.__current_descriptor)

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

        # Update resource
        descriptors = self.__current_descriptor.get('resources', [])
        self.__resources = self.__resources[:len(descriptors)]
        iterator = enumerate(six.moves.zip_longest(list(self.__resources), descriptors))
        for index, (resource, descriptor) in iterator:
            if (not resource or resource.descriptor != descriptor or
                    (resource.schema and resource.schema.foreign_keys)):
                updated_resource = Resource(descriptor,
                    base_path=self.__base_path,
                    strict=self.__strict,
                    unsafe=self.__unsafe,
                    storage=self.__storage,
                    package=self)
                if not resource:
                    self.__resources.append(updated_resource)
                else:
                    self.__resources[index] = updated_resource

    # Deprecated

    def safe(self):
        # True: datapackage is always safe.

        # Deprecate
        warnings.warn(
            'Property "package.safe" is deprecated. '
            'Now it\'s always safe.',
            UserWarning)

        return True

    @property
    def schema(self):
        # Schema: This data package's schema.

        # Deprecate
        warnings.warn(
            'Property "package.schema" is deprecated.',
            UserWarning)

        return self.__profile

    @property
    def attributes(self):
        # tuple: Attributes defined in the schema and the data package.

        # Deprecate
        warnings.warn(
            'Property "package.attributes" is deprecated.',
            UserWarning)

        # Get attributes
        attributes = set(self.to_dict().keys())
        try:
            attributes.update(self.profile.properties.keys())
        except AttributeError:
            pass

        return tuple(attributes)

    @property
    def required_attributes(self):
        # tuple: The schema's required attributed.

        # Deprecate
        warnings.warn(
            'Property "package.required_attributes" is deprecated.',
            UserWarning)
        required = ()

        # Get required
        try:
            if self.profile.required is not None:
                required = tuple(self.profile.required)
        except AttributeError:
            pass

        return required

    def validate(self):
        # Validate this Data Package.

        # Deprecate
        warnings.warn(
            'Property "package.validate" is deprecated.',
            UserWarning)

        descriptor = self.to_dict()
        self.profile.validate(descriptor)

    def iter_errors(self):
        # Lazily yields each ValidationError for the received data dict.

        # Deprecate
        warnings.warn(
            'Property "package.iter_errors" is deprecated.',
            UserWarning)

        return self.profile.iter_errors(self.to_dict())

    def to_dict(self):
        # dict: Convert this Data Package to dict.

        # Deprecate
        warnings.warn(
            'Property "package.to_dict" is deprecated.',
            UserWarning)

        return copy.deepcopy(self.descriptor)

    def to_json(self):
        # str: Convert this Data Package to a JSON string.

        # Deprecate
        warnings.warn(
            'Property "package.to_json" is deprecated.',
            UserWarning)

        return json.dumps(self.descriptor)


# Internal

def _extract_zip_if_possible(descriptor):
    """If descriptor is a path to zip file extract and return (tempdir, descriptor)
    """
    tempdir = None
    result = descriptor
    try:
        if isinstance(descriptor, six.string_types):
            res = requests.get(descriptor)
            res.raise_for_status()
            result = res.content
    except (IOError,
            ValueError,
            requests.exceptions.RequestException):
        pass
    try:
        the_zip = result
        if isinstance(the_zip, bytes):
            try:
                os.path.isfile(the_zip)
            except (TypeError, ValueError):
                # the_zip contains the zip file contents
                the_zip = io.BytesIO(the_zip)
        if zipfile.is_zipfile(the_zip):
            with zipfile.ZipFile(the_zip, 'r') as z:
                _validate_zip(z)
                descriptor_path = [
                    f for f in z.namelist() if f.endswith('datapackage.json')][0]
                tempdir = tempfile.mkdtemp('-datapackage')
                z.extractall(tempdir)
                result = os.path.join(tempdir, descriptor_path)
        else:
            result = descriptor
    except (TypeError,
            zipfile.BadZipfile):
        pass
    if hasattr(descriptor, 'seek'):
        # Rewind descriptor if it's a file, as we read it for testing if it's
        # a zip file
        descriptor.seek(0)
    return (tempdir, result)


def _validate_zip(the_zip):
    """Validate zipped data package
    """
    datapackage_jsons = [f for f in the_zip.namelist() if f.endswith('datapackage.json')]
    if len(datapackage_jsons) != 1:
        msg = 'DataPackage must have only one "datapackage.json" (had {n})'
        raise exceptions.DataPackageException(msg.format(n=len(datapackage_jsons)))


def _slugify_resource_name(name):
    """Slugify resource name
    """
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)


def _slugify_foreign_key(schema):
    """Slugify foreign key
    """
    for foreign_key in schema.get('foreignKeys', []):
        foreign_key['reference']['resource'] = _slugify_resource_name(
            foreign_key['reference'].get('resource', ''))
    return schema
