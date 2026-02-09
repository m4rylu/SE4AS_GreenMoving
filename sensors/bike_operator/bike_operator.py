import json
import time
import configparser
import paho.mqtt.client as mqtt

config = configparser.ConfigParser()
config.read('configuration/config.ini')

UPDATE_RATE = config.getint('system', 'operator_update_rate')

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
OPERATOR_TOPIC = config.get('mqtt_topics', 'operator_topic')

config_s = configparser.ConfigParser()
config_s.read('config.ini')

OPERATOR_ID = config_s.get('operator','id')

bikes = {}
stations = {}
station_loc = {}
for section in config.sections():
    if section.startswith('S'):
        station_loc[section] = {
            "lat": config.getfloat(section, 'lat'),
            "lon": config.getfloat(section, 'lon'),
            "address": config.get(section, 'address'),
            "total_power": config.getint(section, 'total_power')
        }

def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected with result code "+str(rc))
    client.subscribe(BIKE_TOPIC)
    client.subscribe(STATION_TOPIC)
    client.subscribe(OPERATOR_TOPIC)

def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    topic = msg.topic.split("/")
    if topic[1] == "bikes":
        bike_id = msg.topic.split("/")[2]
        bs = payload["telemetry"]
        bikes[bike_id] = bs
    elif topic[1] == "stations":
        station_id = msg.topic.split("/")[2]
        ds = payload["slot"]
        stations[station_id] = ds
    elif topic[1] == "operators":
        request = payload["request"]
        #operator take the bike to the station and connect it
        if request=="CHARGE":
            bike_id = payload["bike_id"]
            station_id = payload["station_id"]
            print(f"Ricevuta richiesta di ricaricare bici {bike_id}")
            payload1 = {
                "request": request,
                "station_id": station_id,
                "slot": payload["slot"],
                "lat": station_loc[station_id]["lat"],
                "lon": station_loc[station_id]["lon"],
            }
            client_mqtt.publish(f"ebike/bikes/{bike_id}/commands", json.dumps(payload1))
            print("Contatto stazione")
            payload1 = {
                "request": "CONNECT",
                "slot": payload["slot"],
                "bike_id": bike_id,
            }
            client_mqtt.publish(f"ebike/stations/{station_id}/request", json.dumps(payload1))

        # operator move bikes
        elif request=="MOVE":
            print("Ricevuta richiesta di spostare bici")
            # metto bici nella nuova stazione
            bike_id = payload["bike_id"]
            station_id_start = payload["station_id_start"]
            slot_start = payload["slot_start"]
            slot_end = payload["slot_end"]
            station_id_end = payload["station_id_end"]
            end_lat = station_loc[station_id_end]["lat"]
            end_lon = station_loc[station_id_end]["lon"]

            payload1 = {
                "request": "CONNECT",
                "slot": slot_end,
                "bike_id": bike_id
            }
            client_mqtt.publish(f"ebike/stations/{station_id_end}/request", json.dumps(payload1))

            # elimino bici vecchia stazione
            payload1 = {
                "request": "DISCONNECT",
                "slot": slot_start
            }
            client_mqtt.publish(f"ebike/stations/{station_id_start}/request", json.dumps(payload1))

            # aggiorna stato bici
            payload1 = {
                request: "CHARGE",
                "station_id": station_id_end,
                "slot": slot_end,
                "lat": end_lat,
                "lon": end_lon
            }
            client_mqtt.publish(f"ebike/bikes/{bike_id}/commands", json.dumps(payload1))





if __name__ == "__main__":
    client_mqtt = mqtt.Client(client_id="Operator1")
    client_mqtt.on_connect = on_connect
    client_mqtt.on_message = on_message
    client_mqtt.connect(HOST, PORT, 60)
    client_mqtt.loop_start()

    while True:
        time.sleep(10)

