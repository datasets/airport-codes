import sys
import csv
import copy
import requests

archive = "archive/data.csv"
source = "https://ourairports.com/data/airports.csv"


def download():
    r = requests.get(source, timeout=60)
    r.raise_for_status()
    if len(r.content.splitlines()) < 2:
        sys.exit("Download from {} has no data rows".format(source))
    with open(archive, "wb") as f:
        f.write(r.content)


def process():
    with open(archive, "r", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        with open("data/airport-codes.csv", "w", encoding="utf-8", newline="") as result:
            fieldnames = [
                "ident",
                "type",
                "name",
                "elevation_ft",
                "continent",
                "iso_country",
                "iso_region",
                "municipality",
                "icao_code",
                "iata_code",
                "gps_code",
                "local_code",
                "coordinates",
            ]
            writer = csv.DictWriter(
                result, fieldnames=fieldnames, extrasaction="ignore"
            )
            writer.writeheader()
            count = 0
            for row in reader:
                count += 1
                new_row = copy.deepcopy(row)
                new_row["coordinates"] = "{}, {}".format(
                    row["latitude_deg"], row["longitude_deg"]
                )
                writer.writerow(new_row)
    if count == 0:
        sys.exit("No data rows in {}".format(archive))


if __name__ == "__main__":
    try:
        download()
    except requests.RequestException as e:
        sys.exit("Download failed: {}".format(e))
    process()
