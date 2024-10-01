from .. import DataStreamProcessor


class rows_processor(DataStreamProcessor):

    def __init__(self, rows_processor_func):
        super(rows_processor, self).__init__()
        self.func = rows_processor_func

    def process_resource(self, resource):
        yield from self.func(resource)
