The airport codes may refer to either [IATA](http://en.wikipedia.org/wiki/International_Air_Transport_Association_airport_code)
airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems, or the [ICAO](http://en.wikipedia.org/wiki/International_Civil_Aviation_Organization_airport_code) airport code
which is a four letter code used by ATC systems and for airports that do not have an IATA airport code (from wikipedia).

Airport codes from around the world. Downloaded from public domain source http://ourairports.com/data/ who compiled this data from multiple different sources. This data is updated nightly.

## Data

"data/airport-codes.csv" contains the list of all airport codes, the attributes are identified in datapackage description. Some of the columns contain attributes identifying airport locations, other codes (IATA, local if exist) that are relevant to identification of an airport.  
Original source url is http://ourairports.com/data/airports.csv  (stored in archive/data.csv)  

## Preparation

You will need Python 3.6 or greater and [dataflows](https://pypi.org/project/dataflows/) library to run the script

To update the data run the process script locally:
```bash
# Install dataflows
pip install dataflows

# Run the script
python airport_codes_flow.py
```

Several steps will be done to get the final data.

* merge columns "latitude_deg" and "longitude_deg" into "coordinates"
* remove columns: "id",  "scheduled_service", "home_link", "wikipedia_link", "keywords"

## Automation

Daily updated 'Airport codes' datapackage could be found on the [datahub.io](http://datahub.io/):  
https://datahub.io/core/airport-codes

## License

The source specifies that the data can be used as is without any warranty. Given size and factual nature of the data and its source from a US company would imagine this was public domain and as such have licensed the Data Package under the Public Domain Dedication and License (PDDL).
