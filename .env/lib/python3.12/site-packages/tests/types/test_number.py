# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from decimal import Decimal
from tableschema import types
from tableschema.config import ERROR


# Tests

@pytest.mark.parametrize('format, value, result, options', [
    ('default', Decimal(1), Decimal(1), {}),
    ('default', 1, Decimal(1), {}),
    ('default', 1.0, Decimal(1), {}),
    ('default', 1 << 63, Decimal(1 << 63), {}),
    ('default', '1', Decimal(1), {}),
    ('default', '10.00', Decimal(10), {}),
    ('default', '10.50', Decimal(10.5), {}),
    ('default', 24.122667, Decimal('24.122667'), {}),
    ('default', '100%', Decimal(100), {'bareNumber': False}),
    ('default', '1000‰', Decimal(1000), {'bareNumber': False}),
    ('default', '-1000', Decimal(-1000), {}),
    ('default', '1,000', Decimal(1000), {'groupChar': ','}),
    ('default', '10,000.00', Decimal(10000), {'groupChar': ','}),
    ('default', '10,000,000.50', Decimal(10000000.5), {'groupChar': ','}),
    ('default', '10#000.00', Decimal(10000), {'groupChar': '#'}),
    ('default', '10#000#000.50', Decimal(10000000.5), {'groupChar': '#'}),
    ('default', '10.50', Decimal(10.5), {'groupChar': '#'}),
    ('default', '1#000', Decimal(1000), {'groupChar': '#'}),
    ('default', '10#000@00', Decimal(10000), {'groupChar': '#', 'decimalChar': '@'}),
    ('default', '10#000#000@50', Decimal(10000000.5), {'groupChar': '#', 'decimalChar': '@'}),
    ('default', '10@50', Decimal(10.5), {'groupChar': '#', 'decimalChar': '@'}),
    ('default', '1#000', Decimal(1000), {'groupChar': '#', 'decimalChar': '@'}),
    ('default', '10,000.00', Decimal(10000), {'groupChar': ',', 'bareNumber': False}),
    ('default', '10,000,000.00', Decimal(10000000), {'groupChar': ',', 'bareNumber': False}),
    ('default', '10.000.000,00', Decimal(10000000), {'groupChar': '.', 'decimalChar': ','}),
    ('default', '$10000.00', Decimal(10000), {'bareNumber': False}),
    ('default', '  10,000.00 €', Decimal(10000), {'groupChar': ',', 'bareNumber': False}),
    ('default', '10 000,00', Decimal(10000), {'groupChar': ' ', 'decimalChar': ','}),
    ('default', '10 000 000,00', Decimal(10000000), {'groupChar': ' ', 'decimalChar': ','}),
    ('default', '10000,00 ₪', Decimal(10000), {'groupChar': ' ', 'decimalChar': ',', 'bareNumber': False}),
    ('default', '  10 000,00 £', Decimal(10000), {'groupChar': ' ', 'decimalChar': ',', 'bareNumber': False}),
    ('default', True, ERROR, {}),
    ('default', False, ERROR, {}),
    ('default', '10,000a.00', ERROR, {}),
    ('default', '10+000.00', ERROR, {}),
    ('default', '$10:000.00', ERROR, {}),
    ('default', 'string', ERROR, {}),
    ('default', '', ERROR, {}),
])
def test_cast_number(format, value, result, options):
    assert types.cast_number(format, value, **options) == result
