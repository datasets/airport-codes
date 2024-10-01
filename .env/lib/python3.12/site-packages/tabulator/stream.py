# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import re
import six
import gzip
import zipfile
import tempfile
import warnings
from copy import copy
from itertools import chain
from collections import deque, Counter
from .loaders.stream import StreamLoader
from . import exceptions
from . import helpers
from . import config


# Module API

# TODO: merge pick/skip rows logic
class Stream(object):
    """Stream of tabular data.

    This is the main `tabulator` class. It loads a data source, and allows you
    to stream its parsed contents.

    # Arguments

        source (str):
            Path to file as ``<scheme>\\://path/to/file.<format>``.
            If not explicitly set, the scheme (file, http, ...) and
            format (csv, xls, ...) are inferred from the source string.

        headers (Union[int, List[int], List[str]], optional):
            Either a row
            number or list of row numbers (in case of multi-line headers) to be
            considered as headers (rows start counting at 1), or the actual
            headers defined a list of strings. If not set, all rows will be
            treated as containing values.

        scheme (str, optional):
            Scheme for loading the file (file, http, ...).
            If not set, it'll be inferred from `source`.

        format (str, optional):
            File source's format (csv, xls, ...). If not
            set, it'll be inferred from `source`. inferred

        encoding (str, optional):
            Source encoding. If not set, it'll be inferred.

        compression (str, optional):
            Source file compression (zip, ...). If not set, it'll be inferred.

        pick_rows (List[Union[int, str, dict]], optional):
            The same as `skip_rows` but it's for picking rows instead of skipping.

        skip_rows (List[Union[int, str, dict]], optional):
            List of row numbers, strings and regex patterns as dicts to skip.
            If a string, it'll skip rows that their first cells begin with it e.g. '#' and '//'.
            To skip only completely blank rows use `{'type'\\: 'preset', 'value'\\: 'blank'}`
            To provide a regex pattern use  `{'type'\\: 'regex', 'value'\\: '^#'}`
            For example\\: `skip_rows=[1, '# comment', {'type'\\: 'regex', 'value'\\: '^# (regex|comment)'}]`

        pick_fields (List[Union[int, str]], optional):
            When passed, ignores all columns with headers
            that the given list DOES NOT include

        skip_fields (List[Union[int, str]], optional):
            When passed, ignores all columns with headers
            that the given list includes. If it contains an empty string it will skip
            empty headers

        sample_size (int, optional):
            Controls the number of sample rows used to
            infer properties from the data (headers, encoding, etc.). Set to
            ``0`` to disable sampling, in which case nothing will be inferred
            from the data. Defaults to ``config.DEFAULT_SAMPLE_SIZE``.

        bytes_sample_size (int, optional):
            Same as `sample_size`, but instead
            of number of rows, controls number of bytes. Defaults to
            ``config.DEFAULT_BYTES_SAMPLE_SIZE``.

        allow_html (bool, optional):
            Allow the file source to be an HTML page.
            If False, raises ``exceptions.FormatError`` if the loaded file is
            an HTML page. Defaults to False.

        multiline_headers_joiner (str, optional):
            When passed, it's used to join multiline headers
            as `<passed-value>.join(header1_1, header1_2)`
            Defaults to ' ' (space).

        multiline_headers_duplicates (bool, optional):
            By default tabulator will exclude a cell of a miltilne header from joining
            if it's exactly the same as the previous seen value in this field.
            Enabling this option will force duplicates inclusion
            Defaults to False.

        hashing_algorithm (func, optional):
            It supports: md5, sha1, sha256, sha512
            Defaults to sha256

        force_strings (bool, optional):
            When True, casts all data to strings.
            Defaults to False.

        force_parse (bool, optional):
            When True, don't raise exceptions when
            parsing malformed rows, simply returning an empty value. Defaults
            to False.

        post_parse (List[function], optional):
            List of generator functions that
            receives a list of rows and headers, processes them, and yields
            them (or not). Useful to pre-process the data. Defaults to None.

        custom_loaders (dict, optional):
            Dictionary with keys as scheme names,
            and values as their respective ``Loader`` class implementations.
            Defaults to None.

        custom_parsers (dict, optional):
            Dictionary with keys as format names,
            and values as their respective ``Parser`` class implementations.
            Defaults to None.

        custom_writers (dict, optional):
            Dictionary with keys as writer format
            names, and values as their respective ``Writer`` class
            implementations. Defaults to None.

        **options (Any, optional): Extra options passed to the loaders and parsers.

    """

    # Public

    def __init__(self,
                 source,
                 headers=None,
                 scheme=None,
                 format=None,
                 encoding=None,
                 compression=None,
                 allow_html=False,
                 sample_size=config.DEFAULT_SAMPLE_SIZE,
                 bytes_sample_size=config.DEFAULT_BYTES_SAMPLE_SIZE,
                 ignore_blank_headers=False,
                 ignore_listed_headers=None,
                 ignore_not_listed_headers=None,
                 multiline_headers_joiner=' ',
                 multiline_headers_duplicates=False,
                 hashing_algorithm='sha256',
                 force_strings=False,
                 force_parse=False,
                 pick_columns=None,
                 skip_columns=None,
                 pick_fields=None,
                 skip_fields=None,
                 limit_fields=None,
                 offset_fields=None,
                 pick_rows=None,
                 skip_rows=None,
                 limit_rows=None,
                 offset_rows=None,
                 post_parse=[],
                 custom_loaders={},
                 custom_parsers={},
                 custom_writers={},
                 **options):

        # Translate aliases
        if pick_fields is not None:
            pick_columns = pick_fields
        if skip_fields is not None:
            skip_columns = skip_fields
        if pick_columns is not None:
            ignore_not_listed_headers = pick_columns
        if skip_columns is not None:
            ignore_listed_headers = skip_columns
            if '' in skip_columns:
                ignore_blank_headers = True

        # Set headers
        self.__headers = None
        self.__headers_row = None
        self.__headers_row_last = None
        if isinstance(headers, int):
            self.__headers_row = headers
            self.__headers_row_last = headers
        elif isinstance(headers, (tuple, list)):
            if (len(headers) == 2 and
                    isinstance(headers[0], int) and
                    isinstance(headers[1], int)):
                self.__headers_row = headers[0]
                self.__headers_row_last = headers[1]
            else:
                self.__headers = list(headers)

        # Set pick rows
        self.__pick_rows = pick_rows
        self.__pick_rows_by_numbers = []
        self.__pick_rows_by_patterns = []
        self.__pick_rows_by_comments = []
        self.__pick_rows_by_presets = {}
        for directive in copy(pick_rows or []):
            if isinstance(directive, int):
                self.__pick_rows_by_numbers.append(directive)
            elif isinstance(directive, dict):
                if directive['type'] == 'regex':
                    self.__pick_rows_by_patterns.append(re.compile(directive['value']))
                elif directive['type'] == 'preset' and directive['value'] == 'blank':
                    self.__pick_rows_by_presets['blank'] = True
                else:
                    raise ValueError('Not supported pick rows: %s' % directive)
            else:
                self.__pick_rows_by_comments.append(str(directive))

        # Set skip rows
        self.__skip_rows = skip_rows
        self.__skip_rows_by_numbers = []
        self.__skip_rows_by_patterns = []
        self.__skip_rows_by_comments = []
        self.__skip_rows_by_presets = {}
        for directive in copy(skip_rows or []):
            if isinstance(directive, int):
                self.__skip_rows_by_numbers.append(directive)
            elif isinstance(directive, dict):
                if directive['type'] == 'regex':
                    self.__skip_rows_by_patterns.append(re.compile(directive['value']))
                elif directive['type'] == 'preset' and directive['value'] == 'blank':
                    self.__skip_rows_by_presets['blank'] = True
                elif directive['type'] == 'preset' and directive['value'] == 'auto':
                    self.__skip_rows_by_presets['auto'] = True
                else:
                    raise ValueError('Not supported skip rows: %s' % directive)
            else:
                self.__skip_rows_by_comments.append(str(directive))

        # Support for pathlib.Path
        if hasattr(source, 'joinpath'):
            source = str(source)

        # Set attributes
        self.__source = source
        self.__scheme = scheme
        self.__format = format
        self.__encoding = encoding
        self.__compression = compression
        self.__allow_html = allow_html
        self.__sample_size = sample_size
        self.__bytes_sample_size = bytes_sample_size
        self.__ignore_blank_headers = ignore_blank_headers
        self.__ignore_listed_headers = ignore_listed_headers
        self.__ignore_not_listed_headers = ignore_not_listed_headers
        self.__multiline_headers_joiner = multiline_headers_joiner
        self.__multiline_headers_duplicates = multiline_headers_duplicates
        self.__ignored_headers_indexes = []
        self.__hashing_algorithm = hashing_algorithm
        self.__force_strings = force_strings
        self.__force_parse = force_parse
        self.__limit_fields = limit_fields
        self.__offset_fields = offset_fields
        self.__limit_rows = limit_rows
        self.__offset_rows = offset_rows
        self.__post_parse = copy(post_parse)
        self.__custom_loaders = copy(custom_loaders)
        self.__custom_parsers = copy(custom_parsers)
        self.__custom_writers = copy(custom_writers)
        self.__actual_scheme = scheme
        self.__actual_format = format
        self.__actual_encoding = encoding
        self.__actual_compression = compression
        self.__options = options
        self.__sample_extended_rows = []
        self.__field_positions = None
        self.__loader = None
        self.__parser = None
        self.__row_number = 0
        self.__stats = None

    def __enter__(self):
        if self.closed:
            self.open()
        return self

    def __exit__(self, type, value, traceback):
        if not self.closed:
            self.close()

    def __iter__(self):
        return self.iter()

    @property
    def closed(self):
        """Returns True if the underlying stream is closed, False otherwise.

        # Returns
            bool: whether closed

        """
        return not self.__parser or self.__parser.closed

    def open(self):
        """Opens the stream for reading.

        # Raises:
            TabulatorException: if an error

        """
        source = self.__source
        options = copy(self.__options)

        # Programming error assertions
        assert self.__hashing_algorithm in config.SUPPORTED_HASHING_ALGORITHMS

        # Validate compression
        if self.__compression:
            if self.__compression not in config.SUPPORTED_COMPRESSION:
                message = 'Not supported compression "%s"' % self.__compression
                raise exceptions.CompressionError(message)

        # Get scheme and format if not already given
        compression = None
        if self.__scheme is None or self.__format is None:
            detected_scheme, detected_format = helpers.detect_scheme_and_format(source)
            scheme = self.__scheme or detected_scheme
            format = self.__format or detected_format
            # Get compression
            for type in config.SUPPORTED_COMPRESSION:
                if self.__compression == type or detected_format == type:
                    compression = type
        else:
            scheme = self.__scheme
            format = self.__format

        # Initiate loader
        self.__loader = None
        if scheme is not None:
            loader_class = self.__custom_loaders.get(scheme)
            if loader_class is None:
                if scheme not in config.LOADERS:
                    message = 'Scheme "%s" is not supported' % scheme
                    raise exceptions.SchemeError(message)
                loader_path = config.LOADERS[scheme]
                if loader_path:
                    loader_class = helpers.import_attribute(loader_path)
            if loader_class is not None:
                loader_options = helpers.extract_options(options, loader_class.options)
                if compression and 'http_stream' in loader_class.options:
                    loader_options['http_stream'] = False
                self.__loader = loader_class(
                    bytes_sample_size=self.__bytes_sample_size,
                    **loader_options)

        # Zip compression
        if compression == 'zip' and six.PY3:
            source = self.__loader.load(source, mode='b')
            with zipfile.ZipFile(source) as archive:
                name = archive.namelist()[0]
                if 'filename' in options.keys():
                    name = options['filename']
                    del options['filename']
                with archive.open(name) as file:
                    source = tempfile.NamedTemporaryFile(suffix='.' + name)
                    for line in file:
                        source.write(line)
                    source.seek(0)
            # We redefine loader/format/schema after decompression
            self.__loader = StreamLoader(bytes_sample_size=self.__bytes_sample_size)
            format = self.__format or helpers.detect_scheme_and_format(source.name)[1]
            scheme = 'stream'

        # Gzip compression
        elif compression == 'gz' and six.PY3:
            name = ''
            if isinstance(source, str):
                name = source.replace('.gz', '')
            source = gzip.open(self.__loader.load(source, mode='b'))
            # We redefine loader/format/schema after decompression
            self.__loader = StreamLoader(bytes_sample_size=self.__bytes_sample_size)
            format = self.__format or helpers.detect_scheme_and_format(name)[1]
            scheme = 'stream'

        # Not supported compression
        elif compression:
            message = 'Compression "%s" is not supported for your Python version'
            raise exceptions.TabulatorException(message % compression)

        # Attach stats to the loader
        if getattr(self.__loader, 'attach_stats', None):
            self.__stats = {'size': 0, 'hash': '', 'hashing_algorithm': self.__hashing_algorithm}
            getattr(self.__loader, 'attach_stats')(self.__stats)

        # Initiate parser
        parser_class = self.__custom_parsers.get(format)
        if parser_class is None:
            if format not in config.PARSERS:
                # If not existent it's a not-found error
                # Next line will raise IOError/HTTPError
                chars = self.__loader.load(source)
                chars.close()
                # Otherwise it's a format error
                message = 'Format "%s" is not supported' % format
                raise exceptions.FormatError(message)
            parser_class = helpers.import_attribute(config.PARSERS[format])
        parser_options = helpers.extract_options(options, parser_class.options)
        self.__parser = parser_class(self.__loader,
                force_parse=self.__force_parse,
                **parser_options)

        # Bad options
        if options:
            message = 'Not supported option(s) "%s" for scheme "%s" and format "%s"'
            message = message % (', '.join(options), scheme, format)
            warnings.warn(message, UserWarning)

        # Open and setup
        self.__parser.open(source, encoding=self.__encoding)
        self.__extract_sample()
        self.__extract_headers()
        if not self.__allow_html:
            self.__detect_html()

        # Set scheme/format/encoding
        self.__actual_scheme = scheme
        self.__actual_format = format
        self.__actual_encoding = self.__parser.encoding
        self.__actual_compression = compression

        return self

    def close(self):
        """Closes the stream.
        """
        self.__parser.close()
        self.__row_number = 0

    def reset(self):
        """Resets the stream pointer to the beginning of the file.
        """
        if self.__row_number > self.__sample_size:
            self.__stats = {'size': 0, 'hash': ''}
            self.__parser.reset()
            self.__extract_sample()
            self.__extract_headers()
        self.__row_number = 0

    @property
    def source(self):
        """Source

        # Returns
            any: stream source

        """
        return self.__source

    @property
    def headers(self):
        """Headers

        # Returns
            str[]/None: headers if available

        """
        return self.__headers

    @headers.setter
    def headers(self, headers):
        """Set headers

        # Arguments
            str[]: headers

        """
        self.__headers = headers

    @property
    def scheme(self):
        """Path's scheme

        # Returns
            str: scheme

        """
        return self.__actual_scheme or 'inline'

    @property
    def format(self):
        """Path's format

        # Returns
            str: format

        """
        return self.__actual_format or 'inline'

    @property
    def encoding(self):
        """Stream's encoding

        # Returns
            str: encoding

        """
        return self.__actual_encoding or 'no'

    @property
    def compression(self):
        """Stream's compression ("no" if no compression)

        # Returns
            str: compression

        """
        return self.__actual_compression or 'no'

    @property
    def fragment(self):
        """Path's fragment

        # Returns
            str: fragment

        """
        if self.__parser:
            return getattr(self.__parser, 'fragment', None)
        return None

    @property
    def dialect(self):
        """Dialect (if available)

        # Returns
            dict/None: dialect

        """
        if self.__parser:
            return getattr(self.__parser, 'dialect', {})
        return None

    @property
    def size(self):
        """Returns the BYTE count of the read chunks if available

        # Returns
            int/None: BYTE count

        """
        if self.__stats:
            return self.__stats['size']

    @property
    def hash(self):
        """Returns the SHA256 (or according to the "hashing_algorithm" parameter)
        hash of the read chunks if available

        # Returns
            str/None: bytes hash

        """
        if self.__stats:
            return self.__stats['hash']

    @property
    def sample(self):
        """Returns the stream's rows used as sample.

        These sample rows are used internally to infer characteristics of the
        source file (e.g. encoding, headers, ...).

        # Returns
            list[]: sample

        """
        sample = []
        iterator = iter(self.__sample_extended_rows)
        iterator = self.__apply_processors(iterator)
        for row_number, headers, row in iterator:
            sample.append(row)
        return sample

    @property
    def field_positions(self):
        if self.__field_positions is None:
            self.__field_positions = []
            if self.__headers:
                size = len(self.__headers) + len(self.__ignored_headers_indexes)
                for index in range(size):
                    if index not in self.__ignored_headers_indexes:
                        self.__field_positions.append(index + 1)
        return self.__field_positions

    @property
    def hashing_algorithm(self):
        return self.__hashing_algorithm

    def iter(self, keyed=False, extended=False):
        """Iterate over the rows.

        Each row is returned in a format that depends on the arguments `keyed`
        and `extended`. By default, each row is returned as list of their
        values.

        # Arguments
            keyed (bool, optional):
                When True, each returned row will be a
                `dict` mapping the header name to its value in the current row.
                For example, `[{'name'\\: 'J Smith', 'value'\\: '10'}]`. Ignored if
                ``extended`` is True. Defaults to False.
            extended (bool, optional):
                When True, returns each row as a tuple
                with row number (starts at 1), list of headers, and list of row
                values. For example, `(1, ['name', 'value'], ['J Smith', '10'])`.
                Defaults to False.

        # Raises
            exceptions.TabulatorException: If the stream is closed.

        # Returns
            Iterator[Union[List[Any], Dict[str, Any], Tuple[int, List[str], List[Any]]]]:
                The row itself. The format depends on the values of `keyed` and
                `extended` arguments.

        """

        # Error if closed
        if self.closed:
            message = 'Stream is closed. Please call "stream.open()" first.'
            raise exceptions.TabulatorException(message)

        # Create iterator
        iterator = chain(
            self.__sample_extended_rows,
            self.__parser.extended_rows)
        iterator = self.__apply_processors(iterator)

        # Yield rows from iterator
        try:
            count = 0
            for row_number, headers, row in iterator:
                if row_number > self.__row_number:
                    count += 1
                    if self.__limit_rows or self.__offset_rows:
                        offset = self.__offset_rows or 0
                        limit = self.__limit_rows + offset if self.__limit_rows else None
                        if offset and count <= offset:
                            continue
                        if limit and count > limit:
                            break
                    self.__row_number = row_number
                    if extended:
                        yield (row_number, headers, row)
                    elif keyed:
                        yield dict(zip(headers, row))
                    else:
                        yield row
        except UnicodeError as error:
            message = 'Cannot parse the source "%s" using "%s" encoding at "%s"'
            raise exceptions.EncodingError(message % (self.__source, error.encoding, error.start))
        except Exception as error:
            raise exceptions.SourceError(str(error))

    def read(self, keyed=False, extended=False, limit=None):
        """Returns a list of rows.

        # Arguments
            keyed (bool, optional): See :func:`Stream.iter`.
            extended (bool, optional): See :func:`Stream.iter`.
            limit (int, optional):
                Number of rows to return. If None, returns all rows. Defaults to None.

        # Returns
            List[Union[List[Any], Dict[str, Any], Tuple[int, List[str], List[Any]]]]:
                The list of rows. The format depends on the values of `keyed`
                and `extended` arguments.
        """
        result = []
        rows = self.iter(keyed=keyed, extended=extended)
        for count, row in enumerate(rows, start=1):
            result.append(row)
            if count == limit:
                break
        return result

    def save(self, target, format=None,  encoding=None, **options):
        """Save stream to the local filesystem.

        # Arguments
            target (str): Path where to save the stream.
            format (str, optional):
                The format the stream will be saved as. If
                None, detects from the ``target`` path. Defaults to None.
            encoding (str, optional):
                Saved file encoding. Defaults to ``config.DEFAULT_ENCODING``.
            **options: Extra options passed to the writer.

        # Returns
            count (int?): Written rows count if available
        """

        # Get encoding/format
        if encoding is None:
            encoding = config.DEFAULT_ENCODING
        if format is None:
            _, format = helpers.detect_scheme_and_format(target)

        # Prepare writer class
        writer_class = self.__custom_writers.get(format)
        if writer_class is None:
            if format not in config.WRITERS:
                message = 'Format "%s" is not supported' % format
                raise exceptions.FormatError(message)
            writer_class = helpers.import_attribute(config.WRITERS[format])

        # Prepare writer options
        writer_options = helpers.extract_options(options, writer_class.options)
        if options:
            message = 'Not supported options "%s" for format "%s"'
            message = message % (', '.join(options), format)
            raise exceptions.TabulatorException(message)

        # Write data to target
        writer = writer_class(**writer_options)
        return writer.write(self.iter(), target, headers=self.headers, encoding=encoding)

    # Private

    def __extract_sample(self):

        # Sample is not requested
        if not self.__sample_size:
            return

        # Extract sample rows
        self.__sample_extended_rows = []
        for _ in range(self.__sample_size):
            try:
                row_number, headers, row = next(self.__parser.extended_rows)
                if self.__headers_row and self.__headers_row >= row_number:
                    if self.__check_if_row_for_skipping(row_number, headers, row):
                        self.__headers_row += 1
                        self.__headers_row_last += 1
                self.__sample_extended_rows.append((row_number, headers, row))
            except StopIteration:
                break
            except UnicodeError as error:
                message = 'Cannot parse the source "%s" using "%s" encoding at "%s"'
                raise exceptions.EncodingError(message % (self.__source, error.encoding, error.start))
            except Exception as error:
                raise exceptions.SourceError(str(error))

        if self.__skip_rows_by_presets.get('auto'):
            row_lengths = []
            for _, _, row in self.__sample_extended_rows:
                row_length = len(row)
                while row_length > 0 and row[row_length - 1] in [None, '']:
                    row_length -= 1
                row_lengths.append(row_length)
            most_common_length = Counter(row_lengths).most_common(1)[0][0]
            for i, (row_number, _, row) in enumerate(self.__sample_extended_rows):
                if row_lengths[i] < most_common_length:
                    self.__headers_row += 1
                    self.__headers_row_last += 1
                else:
                    break

    def __extract_headers(self):

        # Headers row is not set
        if not self.__headers_row:
            return

        # Sample is too short
        if self.__headers_row > self.__sample_size:
            message = 'Headers row (%s) can\'t be more than sample_size (%s)'
            message = message % (self.__headers_row, self.__sample_size)
            raise exceptions.TabulatorException(message)

        # Get headers from data
        last_merged = {}
        keyed_source = False
        for row_number, headers, row in self.__sample_extended_rows:
            keyed_source = keyed_source or headers is not None
            headers = headers if keyed_source else row
            for index, header in enumerate(headers):
                if header is not None:
                    headers[index] = six.text_type(header).strip()
            if row_number == self.__headers_row:
                self.__headers = headers
                last_merged = {index: header for index, header in enumerate(headers)}
            if row_number > self.__headers_row:
                for index in range(0, len(self.__headers)):
                    if len(headers) > index and headers[index] is not None:
                        if not self.__headers[index]:
                            self.__headers[index] = headers[index]
                        else:
                            if (self.__multiline_headers_duplicates or
                                    last_merged.get(index) != headers[index]):
                                self.__headers[index] += (
                                    self.__multiline_headers_joiner + headers[index])
                        last_merged[index] = headers[index]
            if row_number == self.__headers_row_last:
                break

        # Ignore headers
        if (self.__ignore_blank_headers or
                self.__ignore_listed_headers is not None or
                self.__ignore_not_listed_headers is not None):
            self.__ignored_headers_indexes = []
            raw_headers, self.__headers = self.__headers, []
            for index, header in list(enumerate(raw_headers)):
                ignore = False
                # Ignore blank headers
                if header in ['', None]:
                    ignore = True
                # Ignore listed headers
                if self.__ignore_listed_headers is not None:
                    if (header in self.__ignore_listed_headers or
                            index + 1 in self.__ignore_listed_headers):
                        ignore = True
                    # Regex
                    for item in self.__ignore_listed_headers:
                        if isinstance(item, dict) and item.get('type') == 'regex':
                            if bool(re.search(item['value'], header)):
                                ignore = True
                # Ignore not-listed headers
                if self.__ignore_not_listed_headers is not None:
                    if (header not in self.__ignore_not_listed_headers and
                            index + 1 not in self.__ignore_not_listed_headers):
                        ignore = True
                    # Regex
                    for item in self.__ignore_not_listed_headers:
                        if isinstance(item, dict) and item.get('type') == 'regex':
                            if bool(re.search(item['value'], header)):
                                ignore = False
                # Add to the list and skip
                if ignore:
                    self.__ignored_headers_indexes.append(index)
                    continue
                self.__headers.append(header)
            self.__ignored_headers_indexes = list(sorted(self.__ignored_headers_indexes, reverse=True))

        # Limit/offset fields
        if self.__limit_fields or self.__offset_fields:
            ignore = []
            headers = []
            min = self.__offset_fields or 0
            max = self.__limit_fields + min if self.__limit_fields else len(self.__headers)
            for position, header in enumerate(self.__headers, start=1):
                if position <= min:
                    ignore.append(position - 1)
                    continue
                if position > max:
                    ignore.append(position - 1)
                    continue
                headers.append(header)
            for index in ignore:
                if index not in self.__ignored_headers_indexes:
                    self.__ignored_headers_indexes.append(index)
            self.__ignored_headers_indexes = list(sorted(self.__ignored_headers_indexes, reverse=True))
            self.__headers = headers

        # Remove headers from data
        if not keyed_source:
            del self.__sample_extended_rows[:self.__headers_row_last]

        # Stringify headers
        if isinstance(self.__headers, list):
            str_headers = []
            for header in self.__headers:
                str_headers.append(six.text_type(header) if header is not None else '')
            self.__headers = str_headers

    def __detect_html(self):

        # Prepare text
        text = ''
        for row_number, headers, row in self.__sample_extended_rows:
            for value in row:
                if isinstance(value, six.string_types):
                    text += value

        # Detect html content
        html_source = helpers.detect_html(text)
        if html_source:
            message = 'Format has been detected as HTML (not supported)'
            raise exceptions.FormatError(message)

    def __apply_processors(self, iterator):

        # Base processor
        def builtin_processor(extended_rows):
            for row_number, headers, row in extended_rows:

                # Sync headers/row
                if headers != self.__headers:
                    if headers and self.__headers:
                        keyed_row = dict(zip(headers, row))
                        row = [keyed_row.get(header) for header in self.__headers]
                    elif self.__ignored_headers_indexes:
                        row = [value for index, value in enumerate(row) if index not in self.__ignored_headers_indexes]
                    headers = self.__headers

                # Skip rows by numbers/comments
                if self.__check_if_row_for_skipping(row_number, headers, row):
                    continue

                yield (row_number, headers, row)

        # Skip nagative rows processor
        def skip_negative_rows(extended_rows):
            '''
            This processor will skip rows which counts from the end, e.g.
            -1: skip last row, -2: skip pre-last row, etc.
            Rows to skip are taken from  Stream.__skip_rows_by_numbers
            '''
            rows_to_skip = [n for n in self.__skip_rows_by_numbers if n < 0]
            buffer_size = abs(min(rows_to_skip))
            # collections.deque - takes O[1] time to push/pop values from any side.
            buffer = deque()

            # use buffer to save last rows
            for row in extended_rows:
                buffer.append(row)
                if len(buffer) > buffer_size:
                    yield buffer.popleft()

            # Now squeeze out the buffer
            n = len(buffer)
            for i, row in enumerate(buffer):
                if i - n not in rows_to_skip:
                    yield row

        # Force values to strings processor
        def force_strings_processor(extended_rows):
            for row_number, headers, row in extended_rows:
                row = list(map(helpers.stringify_value, row))
                yield (row_number, headers, row)

        # Form a processors list
        processors = [builtin_processor]
        # if we have to delete some rows with negative index (counting from the end)
        if [n for n in self.__skip_rows_by_numbers if n < 0]:
            processors.insert(0, skip_negative_rows)
        if self.__post_parse:
            processors += self.__post_parse
        if self.__force_strings:
            processors.append(force_strings_processor)

        # Apply processors to iterator
        for processor in processors:
            iterator = processor(iterator)

        return iterator

    def __check_if_row_for_skipping(self, row_number, headers, row):

        # Pick rows
        if self.__pick_rows:

            # Skip by number
            if row_number in self.__pick_rows_by_numbers:
                return False

            # Get first cell
            cell = row[0] if row else None

            # Handle blank cell/row
            if cell in [None, '']:
                if '' in self.__pick_rows_by_comments:
                    return False
                if self.__pick_rows_by_presets.get('blank'):
                    if not list(filter(lambda cell: cell not in [None, ''], row)):
                        return False
                return True

            # Pick by pattern
            for pattern in self.__pick_rows_by_patterns:
                if bool(pattern.search(cell)):
                    return False

            # Pick by comment
            for comment in filter(None, self.__pick_rows_by_comments):
                if six.text_type(cell).startswith(comment):
                    return False

            # Default
            return True

        # Skip rows
        if self.__skip_rows:

            # Skip by number
            if row_number in self.__skip_rows_by_numbers:
                return True

            # Get first cell
            cell = row[0] if row else None

            # Handle blank cell/row
            if cell in [None, '']:
                if '' in self.__skip_rows_by_comments:
                    return True
                if self.__skip_rows_by_presets.get('blank'):
                    if not list(filter(lambda cell: cell not in [None, ''], row)):
                        return True
                return False

            # Skip by pattern
            for pattern in self.__skip_rows_by_patterns:
                if bool(pattern.search(cell)):
                    return True

            # Skip by comment
            for comment in filter(None, self.__skip_rows_by_comments):
                if six.text_type(cell).startswith(comment):
                    return True

            # Default
            return False

        # No pick/skip
        return False
