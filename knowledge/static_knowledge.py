import time
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- Configurazione ---
token = "9UAPy4qDu16TQSUe4G9EN88rzsnC1srqrhgwu4Kxg9asMCxLdkCq_NgZzUp2gpnAfSj5W-XTzjeIUEsA23CiIw=="
org = "GreenMoving"
bucket = "bike_monitoring"
url = "http://influxdb:8086"

stations={}
bikes_history={}

time.sleep(5)

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

static_knowledge = {
  "S1": {"lat": 42.3540, "lon": 13.3910, "address": "Piazza Duomo", "total_power":50},
  "S2": {"lat": 42.3512, "lon": 13.4012, "address": "Stazione Centrale", "total_power":50}
  #"S3": {"lat": 42.3600, "lon": 13.3850, "address": "Polo Universitario", "total_power":50}
}

for station in static_knowledge:

    point = Point("station_knowledge") \
                .tag("station_id", station) \
                .field("lat", static_knowledge[station]["lat"]) \
                .field("lon", static_knowledge[station]["lon"]) \
                .field("address", static_knowledge[station]["address"]) \
                .field("total_power", static_knowledge[station]["total_power"])

    write_api.write(bucket=bucket, record=point)



