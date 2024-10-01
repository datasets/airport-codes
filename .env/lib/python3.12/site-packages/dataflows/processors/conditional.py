from .. import DataStreamProcessor


class conditional(DataStreamProcessor):

    def __init__(self, predicate, flow):
        super().__init__()
        self.predicate = predicate
        self.flow = flow

    def _process(self):
        ds = self.source._process()
        if self.predicate(ds.dp):
            if callable(self.flow):
                flow = self.flow(ds.dp)
            else:
                flow = self.flow
            return flow.datastream(ds)
        else:
            return ds
