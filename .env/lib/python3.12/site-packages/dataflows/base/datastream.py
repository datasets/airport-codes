from datapackage import Package


class DataStream:
    def __init__(self, dp=None, res_iter=None, stats=None):
        self.dp = dp if dp is not None else Package()
        self.res_iter = res_iter if res_iter is not None else []
        self.stats = stats if stats is not None else []

    def merge_stats(self):
        ret = {}
        for s in self.stats:
            ret.update(s)
        return ret

    def _process(self):
        return self
