from dataflows import Flow, load, dump_to_path, dump_to_zip, printer, add_metadata
from dataflows import sort_rows, filter_rows, find_replace, delete_fields, set_type, validate, unpivot


{% if 'custom' in processing %}
def my_custom_processing(row):
    # Do some modifications to the row here:
    # ...
    return row
{% endif %}


def {{slug}}():
    flow = Flow(
        # Load inputs
        {% if input == 'file' %}
        load('{{input_url}}', format='{{format}}', {% if sheet %}sheet={{sheet}}{% endif %}),
        {% endif %}
        {% if input == 'remote' %}
        load('{{input_url}}', format='{{format}}', {% if sheet %}sheet={{sheet}}{% endif %}),
        {% endif %}
        {% if input == 'sql' %}
        load('{{input_url}}', table='{{input_db_table}}'),
        {% endif %}
        {% if input == 'other' %}
        {% endif %}
        # Process them (if necessary)
        {% if 'sort' in processing %}
        sort_rows('{field_name}'),  # Key is a Python format string or a list of field names
        {% endif %}
        {% if 'filter' in processing %}
        filter_rows(),
        {% endif %}
        {% if 'find_replace' in processing %}
        find_replace([
            dict(name='field_name',
                 patterns=[
                     dict(find='re-pattern-to-find', replace='re-pattern-to-replace-with'),                     
                 ])
        ]),
        {% endif %}
        {% if 'delete_fields' in processing %}
        delete_fields(['field_name']),  # Pass a list of field names to delete from the data
        {% endif %}
        {% if 'set_type' in processing %}
        set_type('field_name', type='number', constraints=dict(minimum=3)),  # There are quite a few options you can use here
                                                                             # Take a look at https://frictionlessdata.io/specs/table-schema/
        # Or you can simply use validate() here instead                                                                             
        {% endif %}
        {% if 'unpivot' in processing %}
        unpivot(unpivot_fields, extra_keys, extra_value),  # See documentation on the meaning of each of these parameters
        {% endif %}
        {% if 'custom' in processing %}
        my_custom_processing,
        {% endif %}
        # Save the results
        add_metadata(name='{{slug}}', title='''{{title}}'''),
        {% if output in ('print', 'print_n_pkg')  %}
        printer(),
        {% endif %}
        {% if output == 'list' %}
        {% endif %}
        {% if output in ('dp_csv', 'print_n_pkg') %}
        dump_to_path('{{slug}}'),
        {% endif %}
        {% if output == 'dp_csv_zip' %}
        dump_to_zip('{{slug}}.zip'),
        {% endif %}
        {% if output == 'dp_json' %}
        dump_to_path('{{slug}}', force_format=True, format='json'),
        {% endif %}
        {% if output == 'dp_json_zip' %}
        dump_to_zip('{{slug}}.zip', force_format=True, format='json'),
        {% endif %}
        {% if output == 'sql' %}
        dump_to_sql('{{output_url}}', table='{{output_db_table}}')
        {% endif %}
    )
    {% if output != 'list' %}
    flow.process()
    {% endif %}
    {% if output == 'list' %}
    data, *_ = flow.results()
    {% endif %}


if __name__ == '__main__':
    {{slug}}()
