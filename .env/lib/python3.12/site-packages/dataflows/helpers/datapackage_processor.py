from .. import DataStreamProcessor, PackageWrapper


class datapackage_processor(DataStreamProcessor):

    def __init__(self, dp_processor_func):
        super(datapackage_processor, self).__init__()
        self.func = dp_processor_func
        self.dp = None
        self.dp_processor = None

    def process_datapackage(self, dp):
        self.dp = PackageWrapper(dp)
        self.dp_processor = self.func(self.dp)
        ret = next(self.dp_processor)
        if ret is None:
            return dp
        return ret

    def process_resources(self, res_iter):
        self.dp.it = res_iter
        yield from self.dp_processor
