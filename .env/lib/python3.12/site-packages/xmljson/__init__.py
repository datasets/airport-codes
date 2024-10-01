# -*- coding: utf-8 -*-

import sys
from collections import Counter, OrderedDict
try:
    from lxml.etree import Element
except ImportError:
    from xml.etree.cElementTree import Element

__author__ = 'S Anand'
__email__ = 'root.node@gmail.com'
__version__ = '0.2.1'

# Python 3: define unicode() as str()
if sys.version_info[0] == 3:
    unicode = str
    basestring = str


class XMLData(object):
    def __init__(self, xml_fromstring=True, xml_tostring=True, element=None, dict_type=None,
                 list_type=None, attr_prefix=None, text_content=None, simple_text=False,
                 invalid_tags=None):
        # xml_fromstring == False(y) => '1' -> '1'
        # xml_fromstring == True     => '1' -> 1
        # xml_fromstring == fn       => '1' -> fn(1)
        if callable(xml_fromstring):
            self._fromstring = xml_fromstring
        elif not xml_fromstring:
            self._fromstring = lambda v: v
        # custom conversion function to convert data string to XML string
        if callable(xml_tostring):
            self._tostring = xml_tostring
        # custom etree.Element to use
        self.element = Element if element is None else element
        # dict constructor (e.g. OrderedDict, defaultdict)
        self.dict = OrderedDict if dict_type is None else dict_type
        # list constructor (e.g. UserList)
        self.list = list if list_type is None else list_type
        # Prefix attributes with a string (e.g. '$')
        self.attr_prefix = attr_prefix
        # Key that stores text content (e.g. '$t')
        self.text_content = text_content
        # simple_text == False or None or 0 => '<x>a</x>' = {'x': {'a': {}}}
        # simple_text == True               => '<x>a</x>' = {'x': 'a'}
        self.simple_text = simple_text
        # invalid_tags == 'drop' => tags like $ are ignored
        if invalid_tags == 'drop':
            self._element = self.element
            self.element = self._make_valid_element
        elif invalid_tags is not None:
            raise TypeError('invalid_tags can be "drop" or None, not "%s"' % invalid_tags)

    def _make_valid_element(self, key):
        try:
            return self._element(key)
        except (TypeError, ValueError):
            pass

    @staticmethod
    def _tostring(value):
        '''Convert value to XML compatible string'''
        if value is True:
            value = 'true'
        elif value is False:
            value = 'false'
        elif value is None:
            value = ''
        return unicode(value)       # noqa: convert to whatever native unicode repr

    @staticmethod
    def _fromstring(value):
        '''Convert XML string value to None, boolean, int or float'''
        # NOTE: Is this even possible ?
        if value is None:
            return None

        # FIXME: In XML, booleans are either 0/false or 1/true (lower-case !)
        if value.lower() == 'true':
            return True
        elif value.lower() == 'false':
            return False

        # FIXME: Using int() or float() is eating whitespaces unintendedly here
        try:
            return int(value)
        except ValueError:
            pass

        try:
            # Test for infinity and NaN values
            if float('-inf') < float(value) < float('inf'):
                return float(value)
        except ValueError:
            pass

        return value

    def etree(self, data, root=None):
        '''Convert data structure into a list of etree.Element'''
        result = self.list() if root is None else root
        if isinstance(data, (self.dict, dict)):
            for key, value in data.items():
                value_is_list = isinstance(value, (self.list, list))
                value_is_dict = isinstance(value, (self.dict, dict))
                # Add attributes and text to result (if root)
                if root is not None:
                    # Handle attribute prefixes (BadgerFish)
                    if self.attr_prefix is not None:
                        if key.startswith(self.attr_prefix):
                            key = key.lstrip(self.attr_prefix)
                            # @xmlns: {$: xxx, svg: yyy} becomes xmlns="xxx" xmlns:svg="yyy"
                            if value_is_dict:
                                raise ValueError('XML namespaces not yet supported')
                            else:
                                result.set(key, self._tostring(value))
                            continue
                    # Handle text content (BadgerFish, GData)
                    if self.text_content is not None:
                        if key == self.text_content:
                            result.text = self._tostring(value)
                            continue
                    # Treat scalars as text content, not children (GData)
                    if self.attr_prefix is None and self.text_content is not None:
                        if not value_is_dict and not value_is_list:
                            result.set(key, self._tostring(value))
                            continue
                # Add other keys as one or more children
                values = value if value_is_list else [value]
                for value in values:
                    elem = self.element(key)
                    if elem is None:
                        continue
                    result.append(elem)
                    # Treat scalars as text content, not children (Parker)
                    if not isinstance(value, (self.dict, dict, self.list, list)):
                        if self.text_content:
                            value = {self.text_content: value}
                    self.etree(value, root=elem)
        else:
            if self.text_content is None and root is not None:
                root.text = self._tostring(data)
            else:
                elem = self.element(self._tostring(data))
                if elem is not None:
                    result.append(elem)
        return result

    def data(self, root):
        '''Convert etree.Element into a dictionary'''
        value = self.dict()
        children = [node for node in root if isinstance(node.tag, basestring)]
        for attr, attrval in root.attrib.items():
            attr = attr if self.attr_prefix is None else self.attr_prefix + attr
            value[attr] = self._fromstring(attrval)
        if root.text and self.text_content is not None:
            text = root.text
            if text.strip():
                if self.simple_text and len(children) == len(root.attrib) == 0:
                    value = self._fromstring(text)
                else:
                    value[self.text_content] = self._fromstring(text)
        count = Counter(child.tag for child in children)
        for child in children:
            if count[child.tag] == 1:
                value.update(self.data(child))
            else:
                result = value.setdefault(child.tag, self.list())
                result += self.data(child).values()
        # if simple_text, elements with no children nor attrs become '', not {}
        if isinstance(value, dict) and not value and self.simple_text:
            value = ''
        return self.dict([(root.tag, value)])


class BadgerFish(XMLData):
    '''Converts between XML and data using the BadgerFish convention'''
    def __init__(self, **kwargs):
        super(BadgerFish, self).__init__(attr_prefix='@', text_content='$', **kwargs)


class GData(XMLData):
    '''Converts between XML and data using the GData convention'''
    def __init__(self, **kwargs):
        super(GData, self).__init__(text_content='$t', **kwargs)


class Yahoo(XMLData):
    '''Converts between XML and data using the Yahoo convention'''
    def __init__(self, **kwargs):
        kwargs.setdefault('xml_fromstring', False)
        super(Yahoo, self).__init__(text_content='content', simple_text=True, **kwargs)


class Parker(XMLData):
    '''Converts between XML and data using the Parker convention'''
    def __init__(self, **kwargs):
        super(Parker, self).__init__(**kwargs)

    def data(self, root, preserve_root=False):
        '''Convert etree.Element into a dictionary'''
        # If preserve_root is False, return the root element. This is easiest
        # done by wrapping the XML in a dummy root element that will be ignored.
        if preserve_root:
            new_root = root.makeelement('dummy_root', {})
            new_root.insert(0, root)
            root = new_root

        # If no children, just return the text
        children = [node for node in root if isinstance(node.tag, basestring)]
        if len(children) == 0:
            return self._fromstring(root.text)

        # Element names become object properties
        count = Counter(child.tag for child in children)
        result = self.dict()
        for child in children:
            if count[child.tag] == 1:
                result[child.tag] = self.data(child)
            else:
                result.setdefault(child.tag, self.list()).append(self.data(child))

        return result


class Abdera(XMLData):
    '''Converts between XML and data using the Abdera convention'''
    def __init__(self, **kwargs):
        super(Abdera, self).__init__(simple_text=True, text_content=True, **kwargs)

    def data(self, root):
        '''Convert etree.Element into a dictionary'''

        value = self.dict()

        # Add attributes specific 'attributes' key
        if root.attrib:
            value['attributes'] = self.dict()
            for attr, attrval in root.attrib.items():
                value['attributes'][unicode(attr)] = self._fromstring(attrval)

        # Add children to specific 'children' key
        children_list = self.list()
        children = [node for node in root if isinstance(node.tag, basestring)]

        # Add root text
        if root.text and self.text_content is not None:
            text = root.text
            if text.strip():
                if self.simple_text and len(children) == len(root.attrib) == 0:
                    value = self._fromstring(text)
                else:
                    children_list = [self._fromstring(text), ]

        for child in children:
            child_data = self.data(child)
            children_list.append(child_data)

        # Flatten children
        if len(root.attrib) == 0 and len(children_list) == 1:
            value = children_list[0]

        elif len(children_list) > 0:
            value['children'] = children_list

        return self.dict([(unicode(root.tag), value)])


# The difference between Cobra and Abdera is that Cobra _always_ has 'attributes' keys,
# 'children' key is remove when only one child and everything is a string.
# https://github.com/datacenter/cobra/blob/master/cobra/internal/codec/jsoncodec.py
class Cobra(XMLData):
    '''Converts between XML and data using the Cobra convention'''
    def __init__(self, **kwargs):
        super(Cobra, self).__init__(simple_text=True, text_content=True,
                                    xml_fromstring=False, **kwargs)

    def etree(self, data, root=None):
        '''Convert data structure into a list of etree.Element'''
        result = self.list() if root is None else root
        if isinstance(data, (self.dict, dict)):
            for key, value in data.items():
                if isinstance(value, (self.dict, dict)):
                    elem = self.element(key)
                    if elem is None:
                        continue
                    result.append(elem)

                    if 'attributes' in value:
                        for k, v in value['attributes'].items():
                            elem.set(k, self._tostring(v))
                    # else:
                    #     raise ValueError('Cobra requires "attributes" key for each element')

                    if 'children' in value:
                        for v in value['children']:
                            self.etree(v, root=elem)
                else:
                    elem = self.element(key)
                    if elem is None:
                        continue
                    elem.text = self._tostring(value)
                    result.append(elem)
        else:
            if root is not None:
                root.text = self._tostring(data)
            else:
                elem = self.element(self._tostring(data))
                if elem is not None:
                    result.append(elem)

        return result

    def data(self, root):
        '''Convert etree.Element into a dictionary'''

        value = self.dict()

        # Add attributes to 'attributes' key (sorted!) even when empty
        value['attributes'] = self.dict()
        if root.attrib:
            for attr in sorted(root.attrib):
                value['attributes'][unicode(attr)] = root.attrib[attr]

        # Add children to specific 'children' key
        children_list = self.list()
        children = [node for node in root if isinstance(node.tag, basestring)]

        # Add root text
        if root.text and self.text_content is not None:
            text = root.text
            if text.strip():
                if self.simple_text and len(children) == len(root.attrib) == 0:
                    value = self._fromstring(text)
                else:
                    children_list = [self._fromstring(text), ]

        count = Counter(child.tag for child in children)
        for child in children:
            child_data = self.data(child)
            if (count[child.tag] == 1 and
                    len(children_list) > 1 and
                    isinstance(children_list[-1], dict)):
                # Merge keys to existing dictionary
                children_list[-1].update(child_data)
            else:
                # Add additional text
                children_list.append(self.data(child))

        if len(children_list) > 0:
            value['children'] = children_list

        return self.dict([(unicode(root.tag), value)])


abdera = Abdera()
badgerfish = BadgerFish()
cobra = Cobra()
gdata = GData()
parker = Parker()
yahoo = Yahoo()
