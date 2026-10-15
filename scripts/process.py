import sys
import csv
import copy
import json
import collections
import requests

archive = "archive/data.csv"
datapackage = "datapackage.json"
type_counts = "data/type-counts.csv"
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


def type_label(raw):
    # large_airport -> Large Airport; closed -> Closed
    return raw.replace("_", " ").title()


def count_types():
    """Count facilities by type in the file just written, most common first."""
    with open("data/airport-codes.csv", "r", encoding="utf-8", newline="") as f:
        counts = collections.Counter(row["type"] for row in csv.DictReader(f))
    return [
        {"count": n, "type": type_label(t)}
        for t, n in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def write_type_counts(rows):
    with open(type_counts, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["type", "count"], lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def update_chart(rows):
    """Put the same counts into the chart view in datapackage.json."""
    with open(datapackage, "r", encoding="utf-8", newline="") as f:
        package = json.load(f)
    for view in package["views"]:
        for mark in view["spec"]["marks"]:
            if "staticData" in mark:
                mark["staticData"] = rows
    with open(datapackage, "w", encoding="utf-8", newline="") as f:
        f.write(json.dumps(package, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    try:
        download()
    except requests.RequestException as e:
        sys.exit("Download failed: {}".format(e))
    process()
    rows = count_types()
    write_type_counts(rows)
    update_chart(rows)
