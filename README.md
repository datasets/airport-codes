<a className="gh-badge" href="https://datahub.io/core/airport-codes"><img src="https://badgen.net/badge/icon/View%20on%20datahub.io/orange?icon=https://datahub.io/datahub-cube-badge-icon.svg&label&scale=1.25" alt="badge" /></a>

## Description

The airport codes may refer to either [IATA](https://en.wikipedia.org/wiki/IATA_airport_code)
airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems, or the [ICAO](https://en.wikipedia.org/wiki/ICAO_airport_code) airport code
which is a four letter code used by ATC systems and for airports that do not have an IATA airport code (from wikipedia).

Airport codes from around the world. Downloaded from public domain source https://ourairports.com/data/ who compiled this data from multiple different sources. This data is updated nightly.

## Data

"data/airport-codes.csv" contains the list of all airport codes, the attributes are identified in datapackage description. Some of the columns contain attributes identifying airport locations, other codes (IATA, local if exist) that are relevant to identification of an airport.  
Original source url is https://ourairports.com/data/airports.csv  (an unchanged copy with all of its columns is stored in archive/data.csv)  

> Note: Currently the scripts is run automatically using Github Actions

## Preparation

You will need Python 3.12 or greater to run the script

To update the data run the process script locally:
```bash
# To run locally you should do this
# Install using requirements
pip install -r scripts/requirements.txt
python3 scripts/process.py

# Or use make
make
make clean
```

Several steps will be done to get the final data.

* merge columns "latitude_deg" and "longitude_deg" into "coordinates"
* remove columns: "id",  "scheduled_service", "home_link", "wikipedia_link", "keywords"

## Automation

Daily updated 'Airport codes' datapackage could be found on the [datahub.io](https://datahub.io/):  
https://datahub.io/core/airport-codes

## License

The data comes from OurAirports (https://ourairports.com/data/). OurAirports states: "All data is released to the Public Domain, and comes with no guarantee of accuracy or fitness for use." Credit is not required; OurAirports asks for it as a courtesy, and a credit line is in `ATTRIBUTION.txt`. This data package is offered under the Open Data Commons Public Domain Dedication and License (PDDL) 1.0. OurAirports says it compiles its data from many sources (https://ourairports.com/about.html).

Unclear: OurAirports' own sources (for example government airport lists and GeoNames) and any rights IATA or ICAO may hold in their code lists. We did not find terms that answer this, so this package does not claim more than OurAirports' statement above.
