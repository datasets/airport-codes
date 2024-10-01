from tabulate import tabulate
from ..helpers.resource_matcher import ResourceMatcher


try:
    from IPython import get_ipython    
    from IPython.core.display import display_html as _display_html
    get_ipython()
    def display_html(data):
        _display_html(data, raw=True)
except (NameError, ImportError):
    def display_html(data):
        print(data)


def _header_print(header, kwargs):
    if kwargs.get('tablefmt') == 'html':
        display_html(f'<h3>{header}</h3>')
    else:
        print(f'{header}:')


def _table_print(data, kwargs):
    if kwargs.get('tablefmt') == 'html':
        display_html(data)
    else:
        print(data)


def truncate_cell(value, max_size):
    value = str(value)
    if max_size is not None and len(value) > max_size:
        return value[:max_size] + ' ...'
    else:
        return value


def printer(num_rows=10, last_rows=None, fields=None, resources=None,
            header_print=_header_print, table_print=_table_print, max_cell_size=100, **kwargs):

    def func(rows):
        spec = rows.res

        if not ResourceMatcher(resources, spec.descriptor).match(spec.name):
            yield from rows
            return

        header_print(spec.name, kwargs)

        schema_fields = spec.schema.fields
        if fields:
            schema_fields = [f for f in schema_fields if f.name in fields]

        field_names = [f.name for f in schema_fields]
        headers = ['#'] + [
            '{}\n({})'.format(f.name, f.type) for f in schema_fields
        ]
        toprint = []
        last = []
        x = 1

        for i, row in enumerate(rows):

            index = i + 1
            prow = [index] + [truncate_cell(row.get(f), max_cell_size) for f in field_names]
            yield row

            if index - x == (num_rows + 1):
                x *= num_rows

            if 0 <= index - x <= num_rows:
                last.clear()
                if toprint and toprint[-1][0] != index - 1:
                    toprint.append(['...'])
                toprint.append(prow)
            else:
                last.append(prow)
                if len(last) > (last_rows or num_rows):
                    last = last[1:]

        if toprint and last and toprint[-1][0] != last[0][0] - 1:
            toprint.append(['...'])

        toprint += last

        table_print(tabulate(toprint, headers=headers, **kwargs), kwargs)

    return func
