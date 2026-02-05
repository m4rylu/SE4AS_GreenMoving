import paho.mqtt.client as mqtt
import json
import time

OP_ID = "OP1"

bike_topic = "ebike/bikes/+/telemetry"
bike_command_topic = "ebike/bikes/+/commands"
station_topic = "ebike/stations/+/slots"
station_command_topic = "ebike/stations/+/request"
operator_topic = f"ebike/operators/events"

bikes = {}
stations = {}
station_loc = {
  "S1": {"lat": 42.3540, "lon": 13.3910, "address": "Piazza Duomo"},
  "S2": {"lat": 42.3512, "lon": 13.4012, "address": "Stazione Centrale"},
  #"S3": {"lat": 42.3600, "lon": 13.3850, "address": "Polo Universitario"}
}



def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected with result code "+str(rc))
    client.subscribe(bike_topic)
    client.subscribe(station_topic)
    client.subscribe(operator_topic)

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







client_mqtt = mqtt.Client(client_id="Operator1")
client_mqtt.on_connect = on_connect
client_mqtt.on_message = on_message
client_mqtt.connect("mqtt-broker", 1883, 60)
client_mqtt.loop_start()

while True:
    time.sleep(10)

