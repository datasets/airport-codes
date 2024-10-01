import logging
import itertools
import collections
import copy

from datapackage import Package
from tableschema.exceptions import CastError, UniqueKeyError

from . import exceptions
from .datastream import DataStream
from .resource_wrapper import ResourceWrapper
from .schema_validator import schema_validator


class LazyIterator:

    def __init__(self, get_iterator):
        self.get_iterator = get_iterator

    def __iter__(self):
        return self.get_iterator()


class DataStreamProcessor:

    def __init__(self):
        self.stats = {}
        self.source = None
        self.datapackage = None
        self.position = None

    def __call__(self, source=None, position=None):
        if source is None:
            source = DataStream()
        self.source = source
        self.position = position
        return self

    def process_resource(self, resource: ResourceWrapper):
        for row in resource:
            yield self.process_row(row)

    def process_resources(self, resources):
        for res in resources:
            yield self.process_resource(res)

    def process_row(self, row):
        return row

    def process_datapackage(self, dp: Package):
        return dp

    def get_res(self, current_dp, name):
        ret = self.datapackage.get_resource(name)
        if ret is None:
            ret = current_dp.get_resource(name)
        assert ret is not None
        return ret

    def get_iterator(self, datastream):
        current_dp = datastream.dp
        res_iter_ = datastream.res_iter

        def func():
            res_iter = (ResourceWrapper(self.get_res(current_dp, rw.res.name), rw.it)
                        for rw in res_iter_)
            res_iter = self.process_resources(res_iter)
            res_iter = (it if isinstance(it, ResourceWrapper) else ResourceWrapper(res, it)
                        for res, it
                        in itertools.zip_longest(self.datapackage.resources, res_iter))
            return res_iter
        return func

    def _process(self):
        datastream = self.source._process()

        try:
            self.datapackage = Package(descriptor=copy.deepcopy(datastream.dp.descriptor))
            self.datapackage = self.process_datapackage(self.datapackage)
            self.datapackage.commit()

            return DataStream(self.datapackage,
                            LazyIterator(self.get_iterator(datastream)),
                            datastream.stats + [self.stats])
        except Exception as exception:
            self.raise_exception(exception)

    def raise_exception(self, cause):
        if not isinstance(cause, exceptions.ProcessorError):
            error = exceptions.ProcessorError(
                cause,
                processor_name=self.__class__.__name__,
                processor_object=self,
                processor_position=self.position
            )
            raise error from cause
        raise cause

    def safe_process(self, return_results=False, on_error=None):
        results = []
        try:
            ds = self._process()
            for res in ds.res_iter:
                if return_results:
                    if on_error is not None:
                        results.append(list(
                            schema_validator(res.res, res, on_error=on_error)
                        ))
                    else:
                        results.append(list(res))
                else:
                    collections.deque(res, maxlen=0)
        except UniqueKeyError as e:
            self.raise_exception(e)
        except CastError as e:
            for err in e.errors:
                logging.error('%s', err)
        except Exception as exception:
            self.raise_exception(exception)
        return ds, results

    def process(self):
        ds, _ = self.safe_process()
        return ds.dp, ds.merge_stats()

    def results(self, on_error=None):
        ds, results = self.safe_process(return_results=True, on_error=on_error)
        return results, ds.dp, ds.merge_stats()
