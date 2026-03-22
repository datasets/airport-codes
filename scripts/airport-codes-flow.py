import os
import requests

from dataflows import Flow, load, add_computed_field, delete_fields
from dataflows import validate, update_resource, add_metadata, dump_to_path

def readme(fpath='README.md'):
    if os.path.exists(fpath):
        return open(fpath).read()


dialing_info_cldr = Flow(
    load('archive/data.csv', name='airport-codes'),
    add_metadata(
        name= "airport-codes",
        title= "Airport Codes",
        description = """Data contains the list of all airport codes,
the attributes are listed in the table schema. Some of the columns
contain attributes identifying airport locations,
other codes (IATA, local if exist) that are relevant to
identification of an airport.""",
        version= "0.2.0",
        has_premium= True,
        collection= "reference-data",
        has_solutions= ["global-country-region-reference-data"],
        homepage= "http://www.ourairports.com/",
        licenses=[
            {
              "id": "odc-pddl",
              "path": "http://opendatacommons.org/licenses/pddl/",
              "title": "Open Data Commons Public Domain Dedication and License v1.0",
              'name': "open_data_commons_public_domain_dedication_and_license_v1.0"
            }
        ],
        sources= [
            {
              "name": "Our Airports",
              "path": "http://ourairports.com/data/",
              "title": "Our Airports"
            }
        ],
        readme=readme(),
        views=[
            {
                "name": "airports-by-type",
                "title": "World Airports by Type",
                "description": "Distribution of the world's ~72,000 active airports by facility type. Small airports dominate with over 42,000 facilities — the infrastructure of general aviation. Heliports number over 22,000. Large commercial airports handling scheduled passenger services number just 1,194 worldwide, underscoring how concentrated commercial aviation is.",
                "resources": ["airport-codes"],
                "specType": "plot",
                "spec": {
                    "height": 340,
                    "marginLeft": 130,
                    "x": {"label": "Number of Airports", "grid": True},
                    "y": {"label": None},
                    "marks": [
                        {
                            "type": "barX",
                            "staticData": [
                                {"type": "Small Airport", "count": 42582},
                                {"type": "Heliport", "count": 22726},
                                {"type": "Medium Airport", "count": 4067},
                                {"type": "Seaplane Base", "count": 1255},
                                {"type": "Large Airport", "count": 1194},
                                {"type": "Balloonport", "count": 61},
                            ],
                            "x": "count",
                            "y": "type",
                            "fill": "#3b82f6",
                            "tip": True,
                            "sort": {"y": "-x"},
                        }
                    ],
                },
            }
        ],
    ),
    add_computed_field([{
        "operation": "format",
        "target": "coordinates",
        "with": "{latitude_deg}, {longitude_deg}"
    }]),
    delete_fields(fields=[
        "id","longitude_deg","latitude_deg",
        "scheduled_service","home_link","wikipedia_link","keywords"
    ]),
    update_resource('airport-codes', **{'path':'data/airport-codes.csv'}),
    validate(),
    dump_to_path()
)


def flow(parameters, datapackage, resources, stats):
    return dialing_info_cldr


if __name__ == '__main__':
    dialing_info_cldr.process()
