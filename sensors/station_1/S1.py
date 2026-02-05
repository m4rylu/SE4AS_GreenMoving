import paho.mqtt.client as mqtt
import json
import time

# --- CONFIGURAZIONE ---
STATION_ID = "S1"
LAT, LON = 45.4642, 9.1900
BROKER = "mqtt-broker"
TOPIC_REQUEST = f"ebike/stations/{STATION_ID}/request"
TOPIC_STATUS = f"ebike/stations/{STATION_ID}/status"


class ChargingStation:
    def __init__(self, id):
        self.id = id
        self.slots = {
            "slot1": ("empty",0),
            "slot2": ("empty",0),
            "slot3": ("empty",0),
            "slot4": ("empty",0),
            "slot5": ("empty",0)
        }

        # Setup Client Unico
        self.client = mqtt.Client(client_id=self.id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(BROKER, 1883, 60)
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
        print(s.slots)
        time.sleep(20)