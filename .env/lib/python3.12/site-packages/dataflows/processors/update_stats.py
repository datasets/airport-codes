from .. import DataStreamProcessor


class update_stats(DataStreamProcessor):

    def __init__(self, stats):
        self.stats = stats
