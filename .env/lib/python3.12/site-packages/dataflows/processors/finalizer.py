from inspect import signature

from .. import DataStreamProcessor


class finalizer(DataStreamProcessor):

    def __init__(self, callback):
        super().__init__()
        self.callback = callback

    def get_iterator(self, datastream):
        base_func = super().get_iterator(datastream)

        def func():
            yield from base_func()
            if 'stats' in signature(self.callback).parameters:
                stats = datastream.merge_stats()
                self.callback(stats=stats)
            else:
                self.callback()
        return func
