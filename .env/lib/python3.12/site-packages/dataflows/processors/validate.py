from inspect import isfunction

from .. import DataStreamProcessor, schema_validator, ResourceWrapper
from ..base.schema_validator import raise_exception, wrap_handler
from ..helpers import ResourceMatcher


class validate(DataStreamProcessor):

    def __init__(self, *args, resources=None, on_error=None):
        super(validate, self).__init__()
        if on_error is None:
            on_error = raise_exception
        self.on_error = wrap_handler(on_error)
        self.resources = resources
        if len(args) == 2:
            field, validator = args
            assert isinstance(field, str), 'Field name must be a string'
            assert isfunction(validator), 'Validator must be callable'
            validator = self.row_validator(field, validator)
            validator = self.rows_validator(validator)
        elif len(args) == 1:
            validator = args[0]
            assert isfunction(validator), 'Validator must be callable'
            validator = self.rows_validator(validator)
        elif len(args) == 0:
            validator = self.validate_with_schema()
        else:
            assert False, 'Unexpected number of arguments'
        assert validator is not None
        self.validator = validator

    def row_validator(self, field, field_validator):
        def func(row):
            return field_validator(row.get(field))
        return func

    def rows_validator(self, row_validator):
        def func(rows: ResourceWrapper):
            res_name = rows.res.name
            for i, row in enumerate(rows):
                if not row_validator(row):
                    if not self.on_error(res_name, row, i, None, None):
                        continue
                yield row
        return func

    def validate_with_schema(self):
        def func(res):
            yield from schema_validator(res.res, res, on_error=self.on_error)
        return func

    def process_resource(self, res):
        if self.resources.match(res.res.name):
            yield from self.validator(res)
        else:
            yield from super().process_resource()

    def process_datapackage(self, dp):
        self.resources = ResourceMatcher(self.resources, dp)
        return super().process_datapackage(dp)
