from .. import DataStreamProcessor


class row_processor(DataStreamProcessor):

    def __init__(self, row_processor_func):
        super(row_processor, self).__init__()
        self.func = row_processor_func

    def process_row(self, row):
        ret = self.func(row)
        if ret is None:
            return row
        return ret
