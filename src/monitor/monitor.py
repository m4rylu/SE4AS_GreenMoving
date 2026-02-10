import configparser
import paho.mqtt.client as mqtt
import json
import time
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

config = configparser.ConfigParser()
config.read('configuration/config.ini')

UPDATE_RATE = config.getint('update_rate', 'monitor_update_rate')

TOKEN = config.get('influx_db', 'token')
ORG = config.get('influx_db', 'org')
BUCKET = config.get('influx_db', 'bucket')
URL = config.get('influx_db', 'url')

HOST = config.get('mqtt', 'host')
PORT = config.getint('mqtt', 'port')

BIKE_TOPIC = config.get('mqtt_topics', 'bike_topic')
BIKE_COMMAND_TOPIC = config.get('mqtt_topics', 'bike_command_topic')
STATION_TOPIC = config.get('mqtt_topics', 'station_topic')
STATION_COMMAND_TOPIC = config.get('mqtt_topics', 'station_command_topic')
STATION_EVENTS = config.get('mqtt_topics', 'station_events')

bikes = {}
stations = {}

def send_data_bikes():
    if bikes:
        for bike in bikes:
            print("Bike's ID :", bike)
            point = Point("bikes") \
                .tag("bike_id", bike) \
                .field("battery", bikes[bike]["battery"]) \
                .field("motor_locked", bikes[bike]["motor_locked"]) \
                .field("is_charging", bikes[bike]["is_charging"]) \
                .field("is_available", bikes[bike]["motor_locked"]) \
                .field("lat", bikes[bike]["lat"]) \
                .field("lon", bikes[bike]["lon"])

            write_api.write(bucket=BUCKET, record=point)


def send_station_data():
    if stations:
        for station in stations:
            print("Station's ID :", station)
            point = Point("station") \
            .tag("station_id", station) \
            .field("slot1", stations[station]["slot1"][0]) \
            .field("slot1_rate", stations[station]["slot1"][1] ) \
            .field("slot2", stations[station]["slot2"][0]) \
            .field("slot2_rate", stations[station]["slot2"][1] ) \
            .field("slot3", stations[station]["slot3"][0]) \
            .field("slot3_rate", stations[station]["slot3"][1] ) \
            .field("slot4", stations[station]["slot4"][0]) \
            .field("slot4_rate", stations[station]["slot4"][1] ) \
            .field("slot5", stations[station]["slot5"][0]) \
            .field("slot5_rate", stations[station]["slot5"][1] ) \

            write_api.write(bucket=BUCKET, record=point)

def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected with result code "+str(rc))
    client.subscribe(BIKE_TOPIC)
    client.subscribe(STATION_TOPIC)

def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    topic = msg.topic.split("/")
    if topic[1] == "bikes":
        bike_id = msg.topic.split("/")[2]
        bs = payload["telemetry"]
        bikes[bike_id] = bs
    elif topic[3] == "slots":
        station_id = msg.topic.split("/")[2]
        ds = payload["slot"]
        stations[station_id] = ds
        send_station_data()

if __name__ == "__main__":
    client = mqtt.Client(client_id="Monitor")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(HOST, PORT, 60)
    client.loop_start()

    client_db = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
    write_api = client_db.write_api(write_options=SYNCHRONOUS)

    while True:
        send_data_bikes()
        time.sleep(UPDATE_RATE)
