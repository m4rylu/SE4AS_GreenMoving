import time

import paho.mqtt.client as mqtt
import random
import json

# utente vede bici 1 15% e slot 1 libero ->
# - bici vai in carica in slot 1
# - slot 1 occupato da bici 1

# --- CONFIGURAZIONE ---
# Lista di stazioni: (ID, Lat, Lon)
BROKER = "mqtt-broker"


class Bike:
    def __init__(self, bike_id):
        self.id = bike_id
        self.lat = round(random.uniform(42.3273, 42.3866),4)
        self.lon = round(random.uniform(13.3306,13.4287),4)
        self.battery = random.randint(0, 10)
        self.motor_locked = True
        self.is_charging = False
        self.charge_rate = 0
        self.is_available = True if self.battery > 10 else False

        # Topic specifici
        self.telemetry_topic = f"ebike/bikes/{self.id}/telemetry"
        self.command_topic = f"ebike/bikes/{self.id}/commands"

        self.client = mqtt.Client(client_id=self.id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(BROKER, 1883, 60)
        self.client.loop_start()


    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc))
        client.subscribe(self.command_topic)

    def on_message(self, client, userdata, msg):
        payload = json.loads(msg.payload.decode())
        cmd = payload.get("request")
        print(f"RICEVUTO COMANDO: {cmd}")
        if cmd == "UNLOCK":
            self.motor_locked = False
            self.is_available = False
            self.is_charging = False
        elif cmd == "LOCK":
            self.motor_locked = True
            self.is_available = True
        elif cmd == "CHARGE":
            self.is_charging = True
            self.lat = payload.get("lat")
            self.lon = payload.get("lon")
        elif cmd == "BALANCE":
            self.charge_rate = payload.get("rate")

    def update_state(self):
        # 1. LOGICA RICARICA
        if self.is_charging:
            self.battery = min(100, self.battery + self.charge_rate)
            if self.battery >= 20:
                self.is_available = True
                print(f"[{self.id}] INFO: Batteria sopra soglia critica. Disponibile per prenotazione.")

        # 2. LOGICA USO
        elif not self.motor_locked:
            self.battery -= 5
            self.lat += random.uniform(-0.001, 0.001)
            self.lon += random.uniform(-0.001, 0.001)
            if self.battery <= 10:
                self.motor_locked = True
                self.is_available = False
                print("BATTERIA CRITICA: Blocco motore attivato")

        elif self.motor_locked:
            if self.battery != 0:
                self.battery -= 1
            if self.battery <= 10:
                self.is_available = False

        # 4. INVIO TELEMETRIA
        bd = {
            "battery": self.battery,
            "motor_locked": self.motor_locked,
            "is_charging": self.is_charging,
            "charge_rate": self.charge_rate,
            "is_available": self.is_available,
            "lat": round(self.lat, 4),
            "lon": round(self.lon, 4)
        }
        payload = {
            "telemetry":bd
        }
        print(f"Bici: {self.id}, Batteria: {self.battery}, Latitude: {self.lat}, Longitude: {self.lon}")
        self.client.publish(f"ebike/bikes/{self.id}/telemetry", json.dumps(payload))



if __name__ == "__main__":
    bike = Bike("B1")

    while True:
        bike.update_state()
        time.sleep(10)