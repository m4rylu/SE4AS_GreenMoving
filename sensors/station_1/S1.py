import paho.mqtt.client as mqtt
import json
import time
import configparser

config = configparser.ConfigParser()
config.read('configuration/config.ini')

HOST = config.get('mqtt', 'host')
PORT = config.getint('mqtt', 'port')
N_SLOT = config.getint('system', 'n_slot_x_station')

config_s = configparser.ConfigParser()
config_s.read('config.ini')

STATION_ID = config_s.get('station','id')
TOPIC_REQUEST = f"ebike/stations/{STATION_ID}/request"
TOPIC_STATUS = f"ebike/stations/{STATION_ID}/status"


class ChargingStation:
    def __init__(self, id):
        self.id = id
        self.slots = {}

        for i in range(1,N_SLOT+1):
            self.slots[f"slot{i}"] = ("empty",0)

        # Setup Client Unico
        self.client = mqtt.Client(client_id=self.id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(HOST, PORT, 60)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code "+str(rc))
        self.client.subscribe(TOPIC_REQUEST)

    def on_message(self,client, userdata, msg):
        payload = json.loads(msg.payload.decode())
        type_request = payload["request"]
        slot_id = payload["slot"]
        if type_request == "DISCONNECT":
            self.slots[slot_id] = ("empty", 0)
            self.send_slots()
        elif type_request == "CONNECT":
            bike_id = payload["bike_id"]
            slot = payload["slot"]
            self.slots[slot] = (bike_id,0)
            print(f"[STATION] Aggiornamento: slot {slot_id} -> {bike_id}")
            self.send_slots()
        elif type_request == "RESERVED":
            self.slots[slot_id] = ("RESERVED",0)
        elif type_request == "BALANCE":
            if self.slots[slot_id][0] == payload["bike_id"]:
                self.slots[slot_id] = (self.slots[slot_id][0], payload["rate"])
                self.send_slots()

    def send_slots(self):
        payload = {
            "slot": self.slots
        }
        print(f"Aggiornamento: Slot: {self.slots}")
        self.client.publish(f"ebike/stations/{STATION_ID}/slots", json.dumps(payload))

if __name__ == "__main__":
    s = ChargingStation(STATION_ID)
    time.sleep(10)
    s.send_slots()
    while True:
        # just for keeping code active
        print(s.slots)
        time.sleep(20)