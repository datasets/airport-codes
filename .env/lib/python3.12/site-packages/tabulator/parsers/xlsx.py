# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import io
import six
import shutil
import atexit
import openpyxl
import datetime
import re
from itertools import chain
from tempfile import NamedTemporaryFile
from ..parser import Parser
from .. import exceptions
from .. import helpers


# Module API


class XLSXParser(Parser):
    """Parser to parse Excel modern `xlsx` data format.
    """

    # Public

    options = [
        "sheet",
        "workbook_cache",
        "fill_merged_cells",
        "preserve_formatting",
        "adjust_floating_point_error",
    ]

    def __init__(
        self,
        loader,
        force_parse=False,
        sheet=1,
        workbook_cache=None,
        fill_merged_cells=False,
        preserve_formatting=False,
        adjust_floating_point_error=False,
    ):
        self.__loader = loader
        self.__sheet_pointer = sheet
        self.__workbook_cache = workbook_cache
        self.__fill_merged_cells = fill_merged_cells
        self.__preserve_formatting = preserve_formatting
        self.__adjust_floating_point_error = adjust_floating_point_error
        self.__extended_rows = None
        self.__encoding = None
        self.__fragment = None
        self.__force_parse = force_parse
        self.__bytes = None

    @property
    def closed(self):
        return self.__bytes is None or self.__bytes.closed

    def open(self, source, encoding=None):
        self.close()
        self.__encoding = encoding

        # Remote
        # Create copy for remote source
        # For remote stream we need local copy (will be deleted on close by Python)
        # https://docs.python.org/3.5/library/tempfile.html#tempfile.TemporaryFile
        if getattr(self.__loader, "remote", False):
            # Cached
            if self.__workbook_cache is not None and source in self.__workbook_cache:
                self.__bytes = io.open(self.__workbook_cache[source], "rb")
            # Not cached
            else:
                prefix = "tabulator-"
                delete = self.__workbook_cache is None
                source_bytes = self.__loader.load(source, mode="b", encoding=encoding)
                target_bytes = NamedTemporaryFile(prefix=prefix, delete=delete)
                shutil.copyfileobj(source_bytes, target_bytes)
                source_bytes.close()
                target_bytes.seek(0)
                self.__bytes = target_bytes
                if self.__workbook_cache is not None:
                    self.__workbook_cache[source] = target_bytes.name
                    atexit.register(os.remove, target_bytes.name)

        # Local
        else:
            self.__bytes = self.__loader.load(source, mode="b", encoding=encoding)

        # Get book
        # To fill merged cells we can't use read-only because
        # `sheet.merged_cell_ranges` is not available in this mode
        self.__book = openpyxl.load_workbook(
            self.__bytes, read_only=not self.__fill_merged_cells, data_only=True
        )

        # Get sheet
        try:
            if isinstance(self.__sheet_pointer, six.string_types):
                self.__sheet = self.__book[self.__sheet_pointer]
            else:
                self.__sheet = self.__book.worksheets[self.__sheet_pointer - 1]
        except (KeyError, IndexError):
            message = 'Excel document "%s" doesn\'t have a sheet "%s"'
            raise exceptions.SourceError(message % (source, self.__sheet_pointer))
        self.__fragment = self.__sheet.title
        self.__process_merged_cells()

        # Reset parser
        self.reset()

    def close(self):
        if not self.closed:
            self.__bytes.close()

    def reset(self):
        helpers.reset_stream(self.__bytes)
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def encoding(self):
        return self.__encoding

    @property
    def fragment(self):
        return self.__fragment

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):
        for row_number, row in enumerate(self.__sheet.iter_rows(), start=1):
            yield (
                row_number,
                None,
                extract_row_values(
                    row, self.__preserve_formatting, self.__adjust_floating_point_error,
                ),
            )

    def __process_merged_cells(self):
        if self.__fill_merged_cells:
            for merged_cell_range in list(self.__sheet.merged_cells.ranges):
                merged_cell_range = str(merged_cell_range)
                self.__sheet.unmerge_cells(merged_cell_range)
                merged_rows = openpyxl.utils.rows_from_range(merged_cell_range)
                coordinates = list(chain.from_iterable(merged_rows))
                value = self.__sheet[coordinates[0]].value
                for coordinate in coordinates:
                    cell = self.__sheet[coordinate]
                    cell.value = value


# Internal

EXCEL_CODES = {
    "yyyy": "%Y",
    "yy": "%y",
    "dddd": "%A",
    "ddd": "%a",
    "dd": "%d",
    "d": "%-d",
    # Different from excel as there is no J-D in strftime
    "mmmmmm": "%b",
    "mmmm": "%B",
    "mmm": "%b",
    "hh": "%H",
    "h": "%-H",
    "ss": "%S",
    "s": "%-S",
    # Possibly different from excel as there is no am/pm in strftime
    "am/pm": "%p",
    # Different from excel as there is no A/P or a/p in strftime
    "a/p": "%p",
}

EXCEL_MINUTE_CODES = {
    "mm": "%M",
    "m": "%-M",
}
EXCEL_MONTH_CODES = {
    "mm": "%m",
    "m": "%-m",
}

EXCEL_MISC_CHARS = [
    "$",
    "+",
    "(",
    ":",
    "^",
    "'",
    "{",
    "<",
    "=",
    "-",
    "/",
    ")",
    "!",
    "&",
    "~",
    "}",
    ">",
    " ",
]

EXCEL_ESCAPE_CHAR = "\\"
EXCEL_SECTION_DIVIDER = ";"


def convert_excel_date_format_string(excel_date):
    """
    Created using documentation here:
    https://support.office.com/en-us/article/review-guidelines-for-customizing-a-number-format-c0a1d1fa-d3f4-4018-96b7-9c9354dd99f5

    """
    # The python date string that is being built
    python_date = ""
    # The excel code currently being parsed
    excel_code = ""
    prev_code = ""
    # If the previous character was the escape character
    char_escaped = False
    # If we are in a quotation block (surrounded by "")
    quotation_block = False
    # Variables used for checking if a code should be a minute or a month
    checking_minute_or_month = False
    minute_or_month_buffer = ""

    for c in excel_date:
        ec = excel_code.lower()
        # The previous character was an escape, the next character should be added normally
        if char_escaped:
            if checking_minute_or_month:
                minute_or_month_buffer += c
            else:
                python_date += c
            char_escaped = False
            continue
        # Inside a quotation block
        if quotation_block:
            if c == '"':
                # Quotation block should now end
                quotation_block = False
            elif checking_minute_or_month:
                minute_or_month_buffer += c
            else:
                python_date += c
            continue
        # The start of a quotation block
        if c == '"':
            quotation_block = True
            continue
        if c == EXCEL_SECTION_DIVIDER:
            # We ignore excel sections for datetimes
            break

        is_escape_char = c == EXCEL_ESCAPE_CHAR
        # The am/pm and a/p code add some complications, need to make sure we are not that code
        is_misc_char = c in EXCEL_MISC_CHARS and (
            c != "/" or (ec != "am" and ec != "a")
        )
        new_excel_code = False

        # Handle a new code without a different characeter in between
        if (
            ec
            and not is_escape_char
            and not is_misc_char
            # If the code does not start with c, we are in a new code
            and not ec.startswith(c.lower())
            # other than the case where we are building up
            # am/pm (minus the case where it is fully built), we are in a new code
            and (not ec.startswith("a") or ec == "am/pm")
        ):
            new_excel_code = True

        # Code is finished, check if it is a proper code
        if (is_escape_char or is_misc_char or new_excel_code) and ec:
            # Checking if the previous code should have been minute or month
            if checking_minute_or_month:
                if ec == "ss" or ec == "s":
                    # It should be a minute!
                    minute_or_month_buffer = (
                        EXCEL_MINUTE_CODES[prev_code] + minute_or_month_buffer
                    )
                else:
                    # It should be a months!
                    minute_or_month_buffer = (
                        EXCEL_MONTH_CODES[prev_code] + minute_or_month_buffer
                    )
                python_date += minute_or_month_buffer
                checking_minute_or_month = False
                minute_or_month_buffer = ""

            if ec in EXCEL_CODES:
                python_date += EXCEL_CODES[ec]
            # Handle months/minutes differently
            elif ec in EXCEL_MINUTE_CODES:
                # If preceded by hours, we know this is referring to minutes
                if prev_code == "h" or prev_code == "hh":
                    python_date += EXCEL_MINUTE_CODES[ec]
                else:
                    # Have to check if the next code is ss or s
                    checking_minute_or_month = True
                    minute_or_month_buffer = ""
            else:
                # Have to abandon this attempt to convert because the code is not recognized
                return None
            prev_code = ec
            excel_code = ""
        if is_escape_char:
            char_escaped = True
        elif is_misc_char:
            # Add the misc char
            if checking_minute_or_month:
                minute_or_month_buffer += c
            else:
                python_date += c
        else:
            # Just add to the code
            excel_code += c

    # Complete, check if there is still a buffer
    if checking_minute_or_month:
        # We know it's a month because there were no more codes after
        minute_or_month_buffer = EXCEL_MONTH_CODES[prev_code] + minute_or_month_buffer
        python_date += minute_or_month_buffer
    if excel_code:
        ec = excel_code.lower()
        if ec in EXCEL_CODES:
            python_date += EXCEL_CODES[ec]
        elif ec in EXCEL_MINUTE_CODES:
            if prev_code == "h" or prev_code == "hh":
                python_date += EXCEL_MINUTE_CODES[ec]
            else:
                python_date += EXCEL_MONTH_CODES[ec]
        else:
            return None
    return python_date


def eformat(f, prec, exp_digits):
    """
    Formats to Scientific Notation, including precise exponent digits

    """
    s = "%.*e" % (prec, f)
    mantissa, exp = s.split("e")
    # add 1 to digits as 1 is taken by sign +/-
    return "%sE%+0*d" % (mantissa, exp_digits + 1, int(exp))


def convert_excel_number_format_string(
    excel_number, value,
):
    """
    A basic attempt to convert excel number_format to a number string

    The important goal here is to get proper amount of rounding
    """
    if "@" in excel_number:
        # We don't try to parse complicated strings
        return str(value)
    percentage = False
    if excel_number.endswith("%"):
        value = value * 100
        excel_number = excel_number[:-1]
        percentage = True
    if excel_number == "General":
        return value
    multi_codes = excel_number.split(";")
    if value < 0 and len(multi_codes) > 1:
        excel_number = multi_codes[1]
    else:
        excel_number = multi_codes[0]

    code = excel_number.split(".")

    if len(code) > 2:
        return None
    if len(code) < 2:
        # No decimals
        new_value = "{0:.0f}".format(value)

    # Currently we do not support "engineering notation"
    elif re.match(r"^#+0*E\+0*$", code[1]):
        return value
    elif re.match(r"^0*E\+0*$", code[1]):
        # Handle scientific notation

        # Note, it will only actually be returned as a string if
        # type is not inferred

        prec = len(code[1]) - len(code[1].lstrip("0"))
        exp_digits = len(code[1]) - len(code[1].rstrip("0"))
        return eformat(value, prec, exp_digits)

    else:
        decimal_section = code[1]
        # Only pay attention to the 0, # and ? characters as they provide precision information
        decimal_section = "".join(d for d in decimal_section if d in ["0", "#", "?"])

        # Count the number of hashes at the end of the decimal_section in order to know how
        # the number should be truncated
        number_hash = 0
        for i in reversed(range(len(decimal_section))):
            if decimal_section[i] == "#":
                number_hash += 1
            else:
                break
        string_format_code = "{0:." + str(len(decimal_section)) + "f}"
        new_value = string_format_code.format(value)
        if number_hash > 0:
            for i in range(number_hash):
                if new_value.endswith("0"):
                    new_value = new_value[:-1]
    if percentage:
        return new_value + "%"

    return new_value


def extract_row_values(
    row, preserve_formatting=False, adjust_floating_point_error=False,
):
    if preserve_formatting:
        values = []
        for cell in row:
            number_format = cell.number_format or ""
            value = cell.value

            if isinstance(cell.value, datetime.datetime) or isinstance(
                cell.value, datetime.time
            ):
                temporal_format = convert_excel_date_format_string(number_format)
                if temporal_format:
                    value = cell.value.strftime(temporal_format)
            elif (
                adjust_floating_point_error
                and isinstance(cell.value, float)
                and number_format == "General"
            ):
                # We have a float with format General
                # Calculate the number of integer digits
                integer_digits = len(str(int(cell.value)))
                # Set the precision to 15 minus the number of integer digits
                precision = 15 - (integer_digits)
                value = round(cell.value, precision)
            elif isinstance(cell.value, (int, float)):
                new_value = convert_excel_number_format_string(
                    number_format, cell.value,
                )
                if new_value:
                    value = new_value
            values.append(value)
        return values
    return list(cell.value for cell in row)
