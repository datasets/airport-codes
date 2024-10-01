from datapackage import Package


class PackageWrapper:
    def __init__(self, pkg: Package):
        self.pkg: Package = pkg
        self.it = iter([])
        assert isinstance(pkg, Package)

    def __iter__(self):
        return self.it
