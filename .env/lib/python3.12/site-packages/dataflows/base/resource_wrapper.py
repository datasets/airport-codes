from datapackage import Resource


class ResourceWrapper:
    def __init__(self, res: Resource, it):
        self.res = res
        self.it = it
        assert res is not None
        assert isinstance(res, Resource)

    def __iter__(self):
        return self.it
