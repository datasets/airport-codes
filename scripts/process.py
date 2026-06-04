import csv
import copy
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

archive = "archive/data.csv"
source = "https://davidmegginson.github.io/ourairports-data/airports.csv"


def download():
    r = requests.get(source, timeout=60, verify=False)
    r.raise_for_status()
    with open(archive, "wb") as f:
        f.write(r.content)


def process():
    with open(archive, "r", encoding="utf-8") as src:
        reader = csv.DictReader(src)
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
        rows = []
        type_counts = {}
        with open("data/airport-codes.csv", "w", newline="\n", encoding="utf-8") as result:
            writer = csv.DictWriter(
                result, fieldnames=fieldnames, extrasaction="ignore"
            )
            writer.writeheader()
            for row in reader:
                new_row = copy.deepcopy(row)
                new_row["coordinates"] = "{}, {}".format(
                    row["latitude_deg"], row["longitude_deg"]
                )
                writer.writerow(new_row)
                t = row["type"]
                if t != "closed":
                    type_counts[t] = type_counts.get(t, 0) + 1

    type_label = {
        "large_airport": "Large Airport",
        "medium_airport": "Medium Airport",
        "small_airport": "Small Airport",
        "heliport": "Heliport",
        "seaplane_base": "Seaplane Base",
        "balloonport": "Balloonport",
    }
    with open("data/type-counts.csv", "w", newline="\n", encoding="utf-8") as tc:
        writer = csv.DictWriter(tc, fieldnames=["type", "count"])
        writer.writeheader()
        for key in ["large_airport", "medium_airport", "small_airport", "heliport", "seaplane_base", "balloonport"]:
            if key in type_counts:
                writer.writerow({"type": type_label[key], "count": type_counts[key]})


download()
process()
