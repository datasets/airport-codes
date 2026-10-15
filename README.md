<a className="gh-badge" href="https://datahub.io/core/airport-codes"><img src="https://badgen.net/badge/icon/View%20on%20datahub.io/orange?icon=https://datahub.io/datahub-cube-badge-icon.svg&label&scale=1.25" alt="badge" /></a>

# Airport Codes

Airports, airfields, heliports and other aviation facilities from around the world, with their [IATA](https://en.wikipedia.org/wiki/IATA_airport_code) codes (three letters, used in passenger reservation, ticketing and baggage handling), [ICAO](https://en.wikipedia.org/wiki/ICAO_airport_code) codes (four letters, used by air traffic control) and other identifiers, plus type, location and coordinates. The data comes from OurAirports.

Column definitions and types are in `datapackage.json`.

## Data

* `data/airport-codes.csv` has one row per facility.
* `archive/data.csv` is an unchanged copy of the file downloaded from OurAirports, with all of its columns.
* `data/type-counts.csv` is a small table of counts by facility type, including `Closed`. It is rebuilt from `airport-codes.csv` each time the data is updated.

Columns of `data/airport-codes.csv`:

| Column | Meaning |
|---|---|
| `ident` | OurAirports identifier of the facility, for example `KLAX` or `00A`. It is not always an ICAO code |
| `type` | `large_airport`, `medium_airport`, `small_airport`, `heliport`, `seaplane_base`, `balloonport` or `closed` |
| `name` | Name of the facility |
| `elevation_ft` | Elevation above sea level in feet. Empty when unknown |
| `continent` | Two-letter continent code: `AF`, `AN`, `AS`, `EU`, `NA` (North America), `OC`, `SA` |
| `iso_country` | Two-letter country code, for example `US` |
| `iso_region` | Country and region, for example `US-CA` |
| `municipality` | City or town served. Empty when unknown |
| `icao_code` | ICAO code, for example `KLAX`. Often empty |
| `iata_code` | IATA code, for example `LAX`. Often empty |
| `gps_code` | GPS code. Often the same as the ICAO code. May be empty |
| `local_code` | Code used inside the country, for example the FAA code in the US. May be empty |
| `coordinates` | Latitude and longitude in one text value: `"33.942501, -118.407997"` |

### Known limitations

* Many rows have no `iata_code` or no `icao_code`, because many facilities have no such code. Do not use either column as a key.
* The file includes closed facilities (`type` is `closed`). Filter on `type` if you only want facilities that are in use.
* `NA` is a real value in two columns: `continent` (North America) and `iso_country` (Namibia). By default pandas reads `NA` as missing. Read the file as text and turn that off: `pd.read_csv("data/airport-codes.csv", dtype=str, keep_default_na=False)`. Empty cells are then empty strings.
* `coordinates` is one text column, not two numbers. To split it: `lat_lon = df["coordinates"].str.split(", ", expand=True).astype(float)`. The original `latitude_deg` and `longitude_deg` columns are only in `archive/data.csv`.

## Data sources

* [OurAirports][oa] ([airports.csv][oacsv]): the list of airports, with all columns. OurAirports says it compiles its data from many sources, listed on its [About page][oaabout]. Licence: public domain, as stated by OurAirports (see [License](#license)).

Changes made to the source data: the file `airports.csv` is downloaded; `latitude_deg` and `longitude_deg` are combined into one `coordinates` column; the columns `id`, `scheduled_service`, `home_link`, `wikipedia_link` and `keywords` are removed. No rows are changed, added or removed.

Not endorsed by, or affiliated with, OurAirports.

[oa]: https://ourairports.com/data/
[oacsv]: https://ourairports.com/data/airports.csv
[oaabout]: https://ourairports.com/about.html

## Preparation

You need Python 3.12 or greater. To rebuild the data from the repository root:

```bash
# Install the requirements and run the script
pip install -r scripts/requirements.txt
python3 scripts/process.py

# Or use make
make
make clean
```

The script downloads `airports.csv` into `archive/data.csv`, then writes `data/airport-codes.csv`. It then counts the facilities by type to write `data/type-counts.csv` and to update the counts used by the chart in `datapackage.json`.

A GitHub workflow runs the script every night (at 00:00 UTC) and on every push to `main`, and commits `archive/`, `data/` and `datapackage.json` if they changed.

The data is also published on [datahub.io](https://datahub.io/core/airport-codes).

## License

The data comes from OurAirports (https://ourairports.com/data/). OurAirports states: "All data is released to the Public Domain, and comes with no guarantee of accuracy or fitness for use." Credit is not required; OurAirports asks for it as a courtesy, and a credit line is in [ATTRIBUTION.txt](ATTRIBUTION.txt).

Two things are unclear. We did not find terms that answer either, so this package does not claim more than OurAirports' statement above:

* OurAirports' own sources (for example government airport lists and GeoNames). OurAirports names them but we found no statement from them about reuse.
* Any rights IATA or ICAO may hold in their code lists. The IATA code search page carries "All rights reserved" and we did not read IATA's or ICAO's terms.

The maintainers offer their own work (the selection and formatting of the data) under the [Public Domain Dedication and License (PDDL)](https://opendatacommons.org/licenses/pddl/1.0/).
