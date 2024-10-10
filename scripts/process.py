import ssl
import csv
import copy
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

archive = 'archive/data.csv'
source = 'http://ourairports.com/data/airports.csv'

def download():
    r = requests.get(source, verify=False)  
    with open(archive, 'wb') as f:
        f.write(r.content)


def process():
    with open(archive,"r") as source:
        reader = csv.DictReader(source)
        with open("data/airport-codes.csv", "w") as result:
            fieldnames = ['ident', 'type', 'name', 'coordinates', 'elevation_ft',' continent', 'iso_country',
                          'iso_region', 'municipality','gps_code', 'iata_code', 'local_code']
            writer = csv.DictWriter(result, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for row in reader:
                new_row = copy.deepcopy(row)
                new_row['coordinates'] = "{}, {}".format(row['latitude_deg'], row['longitude_deg'])
                writer.writerow(new_row)


download()

process()
