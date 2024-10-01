import copy
from kvfile import KVFile


def saver(resource, db, batch_size):
    gen = db.insert_generator(
        (('{:08x}'.format(idx), row)
         for idx, row
         in enumerate(resource)),
        batch_size=batch_size
    )
    for _, row in gen:
        yield row


def loader(db):
    for _, value in db.items():
        yield value
    db.close()


def duplicate(
    source=None,
    target_name=None,
    target_path=None,
    batch_size=1000,
    duplicate_to_end=False,
):
    def func(package):
        source_, target_name_, target_path_ = source, target_name, target_path
        if source_ is None:
            source_ = package.pkg.descriptor['resources'][0]['name']
        if target_name_ is None:
            target_name_ = source_ + '_copy'
        if target_path is None:
            target_path_ = target_name_ + '.csv'

        def traverse_resources(resources):
            new_res_list = []
            for res in resources:
                yield res
                if res['name'] == source_:
                    res = copy.deepcopy(res)
                    res['name'] = target_name_
                    res['path'] = target_path_
                    if duplicate_to_end:
                        new_res_list.append(res)
                    else:
                        yield res
            for res in new_res_list:
                yield res

        descriptor = package.pkg.descriptor
        descriptor['resources'] = list(traverse_resources(descriptor['resources']))
        yield package.pkg

        dbs = []
        for resource in package:
            if resource.res.name == source_:
                db = KVFile()
                yield saver(resource, db, batch_size)
                if duplicate_to_end:
                    dbs.append(db)
                else:
                    yield loader(db)
            else:
                yield resource
        for db in dbs:
            yield loader(db)

    return func
