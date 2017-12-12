The airport codes may refer to either [IATA](http://en.wikipedia.org/wiki/International_Air_Transport_Association_airport_code)
airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems, or the [ICAO](http://en.wikipedia.org/wiki/International_Civil_Aviation_Organization_airport_code) airport code 
which is a four letter code used by ATC systems and for airports that do not have an IATA airport code (from wikipedia).

Airport codes from around the world. Downloaded from public domain source http://ourairports.com/data/ who compiled this data from multiple different sources. This data is updated nightly.

## Data

There is one csv file ,"airport-codes" which contains the list of all airport codes, the attributes are identified in datapackage description. Some of the columns contain attributes identifying airport locations, other codes (IATA, local if exist) that are relevant to identification of an airport

## Preparation


Original source is downloaded in archive/data.csv 
Original source url http://ourairports.com/data/airports.csv

There are several steps have been done to get final data.

* We download original data into `archive/data.csv`
* We merge columns "latitude_deg" and "longitude_deg" into "coordinates" and remove columns "id",  "scheduled_service", "home_link", "wikipedia_link", "keywords"

Original source url http://ourairports.com/data/airports.csv

Process is recorded and automated in python script:

```
scripts/process.py
```


TODO: Add relationship to UNLOCODEs ?

## License

The source specifies that the data can be used as is without any warranty. Given size and factual nature of the data and its source from a US company would imagine this was public domain and as such have licensed the Data Package under the Public Domain Dedication and License (PDDL).
