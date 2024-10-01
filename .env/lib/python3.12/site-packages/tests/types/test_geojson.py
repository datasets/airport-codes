# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from mock import patch
import pytest
from tableschema import types
from tableschema.config import ERROR
from tableschema.profile import Profile


# Tests

@pytest.mark.parametrize('format, value, result', [
    ('default',
        {'properties': {'Ã': 'Ã'}, 'type': 'Feature', 'geometry': None},
        {'properties': {'Ã': 'Ã'}, 'type': 'Feature', 'geometry': None}),
    ('default',
        '{"geometry": null, "type": "Feature", "properties": {"\\u00c3": "\\u00c3"}}',
        {'properties': {'Ã': 'Ã'}, 'type': 'Feature', 'geometry': None}),
    ('default', {'coordinates': [0, 0, 0], 'type': 'Point'}, ERROR),
    ('default', 'string', ERROR),
    ('default', 1, ERROR),
    ('default', '3.14', ERROR),
    ('default', '', ERROR),
    ('default', {}, ERROR),
    ('default', '{}', ERROR),
    ('topojson',
        {'type': 'LineString', 'arcs': [42]},
        {'type': 'LineString', 'arcs': [42]}),
    ('topojson',
        '{"type": "LineString", "arcs": [42]}',
        {'type': 'LineString', 'arcs': [42]}),
    ('topojson', 'string', ERROR),
    ('topojson', 1, ERROR),
    ('topojson', '3.14', ERROR),
    ('topojson', '', ERROR),
])
def test_cast_geojson(format, value, result):
    assert types.cast_geojson(format, value) == result


@pytest.mark.parametrize('format, value, validates', [
    ('default', '', False),
    ('default', '""', False),
    ('default', '3.14', False),
    ('default', '{}', True),
    ('default', {}, True),
])
def test_validation(format, value, validates):
    """Only json object shaped inputs call Profile.validate()."""
    err = Exception('fake validation error')
    with patch.object(Profile, 'validate', side_effect=err) as mock_validate:
        assert types.cast_geojson(format, value) == ERROR
        assert mock_validate.call_count == int(validates)
