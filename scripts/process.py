import csv
import urllib
import copy

archive = 'archive/data.csv'

def download():
    source = 'http://ourairports.com/data/airports.csv'
    urllib.urlretrieve(source, archive)

def process():
    with open(archive,"rb") as source:
        reader = csv.DictReader(source)
        with open("data/airport-codes.csv","wb") as result:
            fieldnames = ['ident','type','name','coordinates','elevation_ft','continent','iso_country','iso_region','municipality','gps_code','iata_code','local_code']
            writer = csv.DictWriter(result, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for row in reader:
                new_row = copy.deepcopy(row)
                new_row['coordinates'] = "{}, {}".format(row['longitude_deg'], row['latitude_deg'])
                writer.writerow(new_row)
download()     
process()