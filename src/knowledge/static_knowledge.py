import time
import configparser
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

config = configparser.ConfigParser()
config.read('configuration/config.ini')

TOKEN = config.get('influx_db', 'token')
ORG = config.get('influx_db', 'org')
BUCKET = config.get('influx_db', 'bucket')
URL = config.get('influx_db', 'url')

stations={}
bikes_history={}

time.sleep(5)

client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

static_knowledge = {}
for section in config.sections():
    if section.startswith('S'):
        static_knowledge[section] = {
            "lat": config.getfloat(section, 'lat'),
            "lon": config.getfloat(section, 'lon'),
            "address": config.get(section, 'address'),
            "total_power": config.getint(section, 'total_power')
        }

for station in static_knowledge:
    point = Point("station_knowledge") \
                .tag("station_id", station) \
                .field("lat", static_knowledge[station]["lat"]) \
                .field("lon", static_knowledge[station]["lon"]) \
                .field("address", static_knowledge[station]["address"]) \
                .field("total_power", static_knowledge[station]["total_power"])

    write_api.write(bucket=BUCKET, record=point)



