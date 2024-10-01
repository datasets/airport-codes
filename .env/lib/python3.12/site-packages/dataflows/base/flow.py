from inspect import isfunction, signature
from collections.abc import Iterable

from .datastream_processor import DataStreamProcessor
from .schema_validator import raise_exception


class Flow:
    def __init__(self, *args):
        self.chain = args

    def results(self, on_error=raise_exception):
        return self._chain().results(on_error=on_error)

    def process(self):
        return self._chain().process()

    def datastream(self, ds=None):
        return self._chain(ds)._process()

    def _preprocess_chain(self):
        checkpoint_links = []
        for link in self.chain:
            if hasattr(link, 'handle_flow_checkpoint'):
                checkpoint_links = link.handle_flow_checkpoint(checkpoint_links)
            else:
                checkpoint_links.append(link)
        return checkpoint_links

    def _chain(self, ds=None):
        from ..helpers import datapackage_processor, rows_processor, row_processor, iterable_loader

        for position, link in enumerate(self._preprocess_chain(), start=1):
            if isinstance(link, Flow):
                ds = link._chain(ds)
            elif isinstance(link, DataStreamProcessor):
                ds = link(ds, position=position)
            elif isfunction(link):
                sig = signature(link)
                params = list(sig.parameters)
                if len(params) == 1:
                    if params[0] == 'row':
                        ds = row_processor(link)(ds, position=position)
                    elif params[0] == 'rows':
                        ds = rows_processor(link)(ds, position=position)
                    elif params[0] == 'package':
                        ds = datapackage_processor(link)(ds, position=position)
                    else:
                        assert False, 'Failed to parse function signature {!r}'.format(params)
                else:
                    assert False, 'Failed to parse function signature {!r}'.format(params)
            elif isinstance(link, Iterable):
                ds = iterable_loader(link)(ds, position=position)

        return ds
