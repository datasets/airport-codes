from .. import DataStreamProcessor


class resources_processor(DataStreamProcessor):

    def __init__(self, rows_processor_func):
        super(resources_processor, self).__init__()
        self.func = rows_processor_func

    def process_resources(self, resources):
        yield from self.func(resources)
