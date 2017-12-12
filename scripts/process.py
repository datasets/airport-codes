import csv
import urllib

archive = 'archive/data.csv'

def download():
    source = 'http://ourairports.com/data/airports.csv'
    urllib.urlretrieve(source, archive)

def process():
    with open(archive,"rb") as source:
        reader = csv.reader(source)
        next(reader, None)  # skip the headers
        with open("data/airport-codes.csv","wb") as result:
            writer = csv.writer(result)
            writer.writerow(('ident','type','name','coordinates','elevation_ft','continent','iso_country','iso_region','municipality','gps_code','iata_code','local_code'))
            for row in reader:
                result= row[1:4] + [ row[5]+", "+row[4]] + row[6:11] + row[12:15]
                writer.writerow(result)
download()     
process()