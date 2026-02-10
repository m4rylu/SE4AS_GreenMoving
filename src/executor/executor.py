import paho.mqtt.client as mqtt
import json
import time
import configparser
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone

config = configparser.ConfigParser()
config.read('configuration/config.ini')

TOKEN = config.get('influx_db', 'token')
ORG = config.get('influx_db', 'org')
BUCKET = config.get('influx_db', 'bucket')
URL = config.get('influx_db', 'url')

HOST = config.get('mqtt', 'host')
PORT = config.getint('mqtt', 'port')

UPDATE_RATE = config.getint('update_rate', 'executor_update_rate')

STATION_COMMAND_TOPIC = config.get('mqtt_topics', 'station_command_topic')
OPERATOR_TOPIC = config.get('mqtt_topics', 'operator_topic')

bikes = {}
stations = {}
last_processed_time= datetime.fromtimestamp(0, timezone.utc)
last_processed_time_a= datetime.fromtimestamp(0, timezone.utc)
last_processed_time_e= datetime.fromtimestamp(0, timezone.utc)
last_processed_time_bal= datetime.fromtimestamp(0, timezone.utc)

def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected with result code "+str(rc))
    client.subscribe(STATION_COMMAND_TOPIC)

def reserve_slot(slot_end,bike_id,station_id_end):
    payload = {
        "request": "RESERVED",
        "slot": slot_end,
        "bike_id": bike_id
    }
    print(f"Prenotato slot {slot_end} in stazione {station_id_end}")
    client_mqtt.publish(f"ebike/stations/{station_id_end}/request", json.dumps(payload))


def exec_structural_balance():
    global last_processed_time_bal
    flux_query_bikes_to_recharge = f'''
    from(bucket: "{BUCKET}")
    |> range(start: -5m)
    |> filter(fn: (r) => r["_measurement"] == "plan_structural_balance")
    |> last()
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    tables = query_api.query(query=flux_query_bikes_to_recharge, org=ORG)
    if tables:
        record = tables[0].records[0]
        if record.get_time() > last_processed_time_bal:
            bike_id = record.values.get("bike_id")
            station_id_start = record.values.get("station_id_start")
            slot_start = record.values.get("slot_start")
            station_id_end = record.values.get("station_id_end")
            slot_end = record.values.get("slot_end")
            print("bike_id", bike_id)
            print("station_id_start", station_id_start)
            print("slot_start", slot_start)
            print("station_id_end", station_id_end)
            print("slot_end", slot_end)

            reserve_slot(slot_end,bike_id,station_id_end)

        # notifies operator
            event_description = f"Move bike {bike_id} from station {station_id_start} slot {slot_start} to station {station_id_end} slot {slot_end}"
            point = Point("event") \
                .field("description", event_description)
            write_api.write(bucket=BUCKET, record=point)

        # message for operator simulation
            payload = {
                "request": "MOVE",
                "station_id_start": station_id_start,
                "slot_start": slot_start,
                "bike_id": bike_id,
                "station_id_end": station_id_end,
                "slot_end": slot_end,
            }
            # avvisa l'operatore
            client_mqtt.publish(OPERATOR_TOPIC, json.dumps(payload))
            last_processed_time_bal = record.get_time()

def exec_bikes_recharging():
    global last_processed_time
    flux_query_bikes_to_recharge = f'''
        from(bucket: "{BUCKET}")
        |> range(start: -5m)
        |> filter(fn: (r) => r["_measurement"] == "plan_recharging")
        |> last()
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

    tables = query_api.query(query=flux_query_bikes_to_recharge, org=ORG)
    if tables:
        for table in tables:
            for record in table.records:
                if record.get_time() > last_processed_time:
                    bike_id = record.values.get("bike_id")
                    station_id = record.values.get("station_id")
                    slot = record.values.get("slot")

                    reserve_slot(slot, bike_id, station_id)

                    #notifies the operator
                    event_description = f"Put bike {bike_id} in charge at station {station_id} slot {slot}"
                    point = Point("event") \
                        .field("description", event_description)
                    write_api.write(bucket=BUCKET, record=point)

                    # avverte l'operatore
                    payload = {
                        "request": "CHARGE",
                        "slot": slot,
                        "bike_id": bike_id,
                        "station_id": station_id,
                    }
                    # avvisa l'operatore
                    client_mqtt.publish(OPERATOR_TOPIC, json.dumps(payload))
                    last_processed_time = record.get_time()

# execute the charge balance for each station
def exec_energy_waste():
    global last_processed_time_e
    flux_query_balance_charge = f'''
            from(bucket: "{BUCKET}")
            |> range(start: -5m)
            |> filter(fn: (r) => r["_measurement"] == "plan_energy_waste")
            |> last()
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''

    tables = query_api.query(query=flux_query_balance_charge, org=ORG)
    for table in tables:
        for record in table.records:
            if record.get_time() > last_processed_time_e:
                station_id = record.values.get("station_id")
                slot = record.values.get("slot")
                rate = record.values.get("rate")
                bike_id = record.values.get("bike_id")

                #manda segnale a stazione
                payload = {
                    "request": "BALANCE",
                    "slot": slot,
                    "rate": rate,
                    "bike_id": bike_id,
                }
                print(f"mando segnale a stazione {station_id} a {slot} di mettere {rate}")
                # avvisa l'operatore
                client_mqtt.publish(f"ebike/stations/{station_id}/request", json.dumps(payload))

                # avvisa bike di caricarsi così
                payload = {
                    "request": "BALANCE",
                    "rate": rate,
                }
                print(f"mando segnale a bici {bike_id} di caricare a {rate}")
                # avvisa l'operatore
                client_mqtt.publish(f"ebike/bikes/{bike_id}/commands", json.dumps(payload))

                last_processed_time_e = record.get_time()

def execute_bike_availability():
    global last_processed_time_a
    flux_query_available_bikes = f'''
                from(bucket: "{BUCKET}")
                |> range(start: -5m)
                |> filter(fn: (r) => r["_measurement"] == "bike_availability")
                |> last()
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                '''

    tables = query_api.query(query=flux_query_available_bikes, org=ORG)
    if tables:
        for table in tables:
            for record in table.records:
                if record.get_time() > last_processed_time_a:
                    bike_id = record.values.get("bike_id")
                    event = record.values.get("event")
                    if event == "AVAILABLE":
                        payload = {
                            "request": "AVAILABLE",
                        }
                        print(f"mando segnale a bici {bike_id} di essere disponibile")
                        client_mqtt.publish(f"ebike/bikes/{bike_id}/commands", json.dumps(payload))
                    elif event == "NOT_AVAILABLE":
                        payload = {
                            "request": "NOT_AVAILABLE",
                        }
                        print(f"mando segnale a bici {bike_id} di non essere disponibile")
                        client_mqtt.publish(f"ebike/bikes/{bike_id}/commands", json.dumps(payload))

                    last_processed_time_a = record.get_time()



def execute():
    exec_bikes_recharging()
    exec_energy_waste()
    exec_structural_balance()
    execute_bike_availability()

if __name__ == "__main__":
    time.sleep(15)

    client_db = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
    query_api = client_db.query_api()
    write_api = client_db.write_api(write_options=SYNCHRONOUS)

    client_mqtt = mqtt.Client(client_id="Executor")
    client_mqtt.connect(HOST, PORT, 60)
    client_mqtt.loop_start()
    while True:
        execute()
        time.sleep(UPDATE_RATE)
