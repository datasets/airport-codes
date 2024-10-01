import copy


def update_package(**metadata):

    metadata = copy.deepcopy(metadata)
    if 'resources' in metadata:
        del metadata['resources']

    def func(package):
        package.pkg.descriptor.update(metadata)
        yield package.pkg
        yield from package

    return func


add_metadata = update_package
