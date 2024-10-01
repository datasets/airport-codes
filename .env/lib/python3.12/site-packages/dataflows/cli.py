import os
import sys
import subprocess

import slugify
import click
from jinja2 import Environment, PackageLoader
import inquirer
from inquirer import themes

# Some settings
FORMATS = ['csv', 'tsv', 'xls', 'xlsx', 'json', 'ndjson', 'ods', 'gsheet']
INPUTS = dict((
  ('File', 'file'),
  ('Remote URL', 'remote'),
  ('SQL Database', 'sql'),
  ('Other', 'other')
))
PROCESSING = dict((
  ('Sort all rows by key',                'sort'),
  ('Filter according to column values',   'filter'),
  ('Search & replace values in the data', 'find_replace'),
  ('Delete some columns',                 'delete_fields'),
  ('Normalize and validate numbers, dates and other types', 'set_type'),
  ('Un-pivot the data',                   'unpivot'),
  ('Custom row-by-row processing',        'custom'),
))
OUTPUTS = dict((
  ('Just print the data',                     'print'),
  ('As a Python list',                        'list'),
  ('A CSV file (in a data package)',          'dp_csv'),
  ('A CSV file (in a zipped data package)',   'dp_csv_zip'),
  ('A JSON file (in a data package)',         'dp_json'),
  ('A JSON file (in a zipped data package)',  'dp_json_pkg'),
  ('An SQL database',                         'sql'),
))


# Utility functions
def fall(*validators):
    def func(*args):
        return all(v(*args) for v in validators)
    return func


def fany(*validators):
    def func(*args):
        return any(v(*args) for v in validators)
    return func


# Validators
def valid_url(ctx, url):
    return url.startswith('http://') or url.startswith('https://')


def not_empty(ctx, x):
    return x


# Converters
def convert_processing(ctx, key):
    ctx['processing'] = [PROCESSING[x] for x in key]
    return True


def convert_input(ctx, key):
    ctx['input'] = INPUTS[key]
    return True


def convert_output(ctx, key):
    ctx['output'] = OUTPUTS[key]
    return True


def extract_format(ctx, url):
    if url:
        _, ext = os.path.splitext(url)
        if ext:
            ctx['format'] = ext[1:].lower()
            return True
    ctx['format'] = None
    return True


# Render
env = Environment(loader=PackageLoader('dataflows'),
                  trim_blocks=True, lstrip_blocks=True,
                  autoescape=False)


def render(parameters):
    tpl = env.get_template('main.tpl.py')
    return tpl.render(**parameters)


@click.group()
def cli():
    pass


# Main CLI routine
@cli.command()
@click.argument('arg', default='interactive')
def init(arg):
    """Bootstrap a processing pipeline script.
ARG is either a path or a URL for some data to read from, 'hello-world' for a full working code example,
or leave empty for an interactive walkthrough.
    """

    answers = {'a': 1}
    if arg == 'interactive':
        input("""Hi There!
    DataFlows will now bootstrap a data processing flow based on your needs.

    Press any key to start...
    """)

    elif arg == 'hello-world':
        raise NotImplementedError()
    else:
        url = arg
        answers = dict(
            input='remote',
            title=os.path.basename(url),
            input_url=url,
            processing=[],
            output='print_n_pkg'
        )
        extract_format(answers, url)

    questions = [
        # Input
        inquirer.List('input_str',
                    message='What is the source of your data?',
                    choices=INPUTS.keys(),
                    ignore=lambda ctx: ctx.get('input') is not None,
                    validate=convert_input),

        # Input Parameters
        inquirer.Text('input_url',
                    message='What is the path of that file',
                    ignore=fany(lambda ctx: ctx.get('input') != 'file',
                                lambda ctx: ctx.get('input_url') is not None),
                    validate=fall(not_empty, extract_format)),
        inquirer.List('format',
                    message='We couldn''t detect the file format - which is it?',
                    choices=FORMATS[:-1],
                    ignore=fany(lambda ctx: ctx.get('input') != 'file',
                                lambda ctx: ctx.get('format') in FORMATS)),

        inquirer.Text('input_url',
                    message='Where is that file located (URL)',
                    ignore=fany(lambda ctx: ctx.get('input') != 'remote',
                                lambda ctx: ctx.get('input_url') is not None),
                    validate=fall(extract_format, not_empty, valid_url)),
        inquirer.List('format',
                      message='We couldn''t detect the source format - which is it',
                      choices=FORMATS,
                      ignore=fany(lambda ctx: ctx['input'] != 'remote',
                                  lambda ctx: ctx.get('format') in FORMATS)
                      ),
        inquirer.Text('sheet',
                      message='Which sheet in the spreadsheet should be processed (name or index)',
                      validate=not_empty,
                      ignore=lambda ctx: ctx.get('format') not in ('xls', 'xlsx', 'ods'),
                      ),
        inquirer.Text('input_url',
                      message='What is the connection string to the database',
                      validate=not_empty,
                      ignore=fany(lambda ctx: ctx['input'] != 'sql',
                                  lambda ctx: ctx.get('input_url') is not None),
                      ),
        inquirer.Text('input_db_table',
                      message='...and the name of the database table to extract',
                      validate=not_empty,
                      ignore=fany(lambda ctx: ctx['input'] != 'sql',
                                  lambda ctx: ctx.get('input_db_table') is not None),
                      ),

        inquirer.Text('input_url',
                      message='Describe that other source (shortly)',
                      ignore=fany(lambda ctx: ctx['input'] != 'other',
                                  lambda ctx: ctx.get('input_url') is not None),
                      ),

        # Processing
        inquirer.Checkbox('processing_str',
                        message='What kind of processing would you like to run on the data',
                        choices=PROCESSING.keys(),
                        ignore=lambda ctx: ctx.get('processing') is not None,
                        validate=convert_processing),

        # Output
        inquirer.List('output_str',
                    message='Finally, where would you like the output data',
                    choices=OUTPUTS.keys(),
                    ignore=lambda ctx: ctx.get('output') is not None,
                    validate=convert_output),
        inquirer.Text('output_url',
                      message='What is the connection string to the database',
                      validate=not_empty,
                      ignore=fany(lambda ctx: ctx['output'] != 'sql',
                                  lambda ctx: ctx.get('output_url') is not None),
                      ),
        inquirer.Text('output_db_table',
                      message='...and the name of the database table to write to',
                      validate=not_empty,
                      ignore=fany(lambda ctx: ctx['output'] != 'sql',
                                  lambda ctx: ctx.get('output_db_table') is not None),
                      ),

        # # Finalize
        inquirer.Text('title',
                    message='That''s it! Now, just provide a title for your processing flow',
                    ignore=lambda ctx: ctx.get('title') is not None,
                    validate=not_empty),
    ]
    answers = inquirer.prompt(questions, answers=answers, theme=themes.GreenPassion())
    if answers is None:
        return
    answers['slug'] = slugify.slugify(answers['title'], separator='_')

    filename = '{slug}.py'.format(**answers)
    with open(filename, 'w') as out:
        print('Writing processing code into {}'.format(filename))
        out.write(render(answers))

    try:
        print('Running {}'.format(filename))
        ret = subprocess.check_output(sys.executable + ' ' + filename,
                                      stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        print(ret)
        print('Done!')
    except subprocess.CalledProcessError as e:
        print('Processing failed, here''s the error:')
        print(e.stderr)
        answers = inquirer.prompt([
            inquirer.Confirm('edit',
                            message='Would you like to open {} in the default editor?'.format(filename),
                            default=False)
        ])
        if answers['edit']:
            click.edit(filename=filename)


if __name__ == '__main__':
    sys.exit(cli())
