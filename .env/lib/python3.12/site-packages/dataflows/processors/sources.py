from datapackage import Package

from .. import DataStreamProcessor, Flow, DataStream


class sources(DataStreamProcessor):

    def __init__(self, *sources):
        super().__init__()
        self.sources: DataStream = [
            Flow(s).datastream()
            for s in sources
        ]

    def process_resources(self, resources):
        yield from super().process_resources(resources)
        source: DataStream
        for source in self.sources:
            for res in source.res_iter:
                yield res

    def process_datapackage(self, dp: Package):
        super().process_datapackage(dp)
        descriptor = dp.descriptor
        source: DataStream
        for source in self.sources:
            res1 = descriptor.pop('resources', [])
            res2 = source.dp.descriptor['resources']
            descriptor.update(source.dp.descriptor)
            descriptor['resources'] = res1 + res2
        dp.commit()
        return dp
