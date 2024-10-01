from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import io
import os
import json
import six
import requests
from .exceptions import RegistryError


# Module API

class Registry(object):
    '''Allow loading Data Package profiles from a registry.

    Args:
        registry_path_or_url (str): Path or URL to the registry's CSV file. It
            defaults to the local registry cache path.

    Raises:
        RegistryError: If there was some problem opening the registry file or
            its format was incorrect.
    '''

    # Public

    DEFAULT_REGISTRY_URL = 'https://specs.frictionlessdata.io/schemas/registry.json'
    DEFAULT_REGISTRY_PATH = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'profiles',
        'registry.json'
    )

    def __init__(self, registry_path_or_url=DEFAULT_REGISTRY_PATH):
        if os.path.isfile(registry_path_or_url):
            self._BASE_PATH = os.path.dirname(
                os.path.abspath(registry_path_or_url)
            )
        try:
            self._profiles = {}
            self._registry = self._get_registry(registry_path_or_url)
        except (IOError, ValueError) as e:
            six.raise_from(RegistryError(e), e)

    @property
    def available_profiles(self):
        '''dict: The available profiles' metadata keyed by their ids.'''
        return self._registry

    @property
    def base_path(self):
        '''str: The base path of this Registry (None if it's remote).'''
        try:
            return self._BASE_PATH
        except AttributeError:
            pass

    def get(self, profile_id):
        '''Returns the profile with the received ID as a dict

        If a local copy of the profile exists, it'll be returned. If not, it'll
        be downloaded from the web. The results are cached, so any subsequent
        calls won't hit the filesystem or the web.

        Args:
            profile_id (str): The ID of the profile you want.

        Raises:
            RegistryError: If there was some problem opening the profile file
                or its format was incorrect.
        '''
        if profile_id not in self._profiles:
            try:
                self._profiles[profile_id] = self._get_profile(profile_id)
            except (ValueError,
                    IOError) as e:
                six.raise_from(RegistryError(e), e)
        return self._profiles[profile_id]

    # Internal

    def _get_profile(self, profile_id):
        '''dict: Return the profile with the received ID as a dict (None if it
        doesn't exist).'''
        profile_metadata = self._registry.get(profile_id)
        if not profile_metadata:
            return

        path = self._get_absolute_path(profile_metadata.get('schema_path'))
        url = profile_metadata.get('schema')
        if path:
            try:
                return self._load_json_file(path)
            except IOError as local_exc:
                if not url:
                    raise local_exc

                try:
                    return self._load_json_url(url)
                except IOError:
                    msg = (
                        'Error loading profile locally at "{path}" '
                        'and remotely at "{url}".'
                    ).format(path=path, url=url)
                    six.raise_from(IOError(msg), local_exc)
        elif url:
            return self._load_json_url(url)

    def _get_registry(self, registry_path_or_url):
        '''dict: Return the registry as dict with profiles keyed by id.'''
        if registry_path_or_url.startswith('http'):
            profiles = self._load_json_url(registry_path_or_url)
        else:
            profiles = self._load_json_file(registry_path_or_url)
        try:
            registry = {}
            for profile in profiles:
                registry[profile['id']] = profile
            return registry
        except KeyError as e:
            msg = (
                'Registry at "{path}" has no "id" column.'
            ).format(path=registry_path_or_url)
            six.raise_from(ValueError(msg), e)

    def _get_absolute_path(self, relative_path):
        '''str: Return the received relative_path joined with the base path
        (None if there were some error).'''
        try:
            return os.path.join(self.base_path, relative_path)
        except (AttributeError, TypeError):
            pass

    def _load_json_file(self, path):
        with io.open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _load_json_url(self, url):
        '''dict: Return the JSON at the local path or URL as a dict.'''
        res = requests.get(url)
        res.raise_for_status()

        return res.json()
