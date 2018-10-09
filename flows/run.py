from dataflows import Flow, PackageWrapper, ResourceWrapper
from dataflows import load, add_computed_field, delete_fields, add_metadata
from dataflows import validate, dump_to_path, cache, printer

def rename(package: PackageWrapper):
    package.pkg.descriptor['resources'][0]['name'] = 'airport-codes'
    package.pkg.descriptor['resources'][0]['path'] = 'data/airport-codes.csv'
    yield package.pkg
    res_iter = iter(package)
    first: ResourceWrapper = next(res_iter)
    yield first.it
    yield from package


dialing_info_cldr = Flow(
    load('http://ourairports.com/data/airports.csv'),
    add_metadata(
        name= "airport-codes",
        title= "Airport Codes",
        homepage= "http://www.ourairports.com/",
        version= "0.2.0",
        license= "PDDL-1.0",
        sources= [
            {
              "name": "Our Airports",
              "path": "http://ourairports.com/data/",
              "title": "Our Airports"
            }
         ],
    ),
    add_computed_field(fields=[{
        "operation": "format",
        "target": "coordinates",
        "with": "{longitude_deg}, {latitude_deg}"
    }]),
    delete_fields(fields=[
        "id","longitude_deg","latitude_deg",
        "scheduled_service","home_link","wikipedia_link","keywords"
    ]),
    rename,
    validate(),
    printer(),
    dump_to_path(),
)


if __name__ == '__main__':
    dialing_info_cldr.process()
