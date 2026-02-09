from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import json
import time
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- Configurazione ---

token = "9UAPy4qDu16TQSUe4G9EN88rzsnC1srqrhgwu4Kxg9asMCxLdkCq_NgZzUp2gpnAfSj5W-XTzjeIUEsA23CiIw=="
org = "GreenMoving"
bucket = "bike_monitoring"
url = "http://influxdb:8086"

bike_topic = "ebike/bikes/+/telemetry"
bike_command_topic = "ebike/bikes/+/commands"
station_topic = "ebike/stations/+/slots"
station_command_topic = "ebike/stations/+/request"

bikes = {}
stations = {}
last_processed_time= datetime.fromtimestamp(0, timezone.utc)
last_processed_time_e= datetime.fromtimestamp(0, timezone.utc)
last_processed_time_bal= datetime.fromtimestamp(0, timezone.utc)

time.sleep(30)

client_db = InfluxDBClient(url=url, token=token, org=org)
query_api = client_db.query_api()
write_api = client_db.write_api(write_options=SYNCHRONOUS)

client_mqtt = mqtt.Client(client_id="Executor")
client_mqtt.connect("mqtt-broker", 1883, 60)
client_mqtt.loop_start()

# fare una lista di active tasks anche per executor

def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected with result code "+str(rc))
    client.subscribe(station_command_topic)

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
    from(bucket: "{bucket}")
    |> range(start: -5m)
    |> filter(fn: (r) => r["_measurement"] == "plan_structural_balance")
    |> last()
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    tables = query_api.query(query=flux_query_bikes_to_recharge, org=org)
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
            write_api.write(bucket=bucket, record=point)

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
            client_mqtt.publish(f"ebike/operators/events", json.dumps(payload))
            last_processed_time_bal = record.get_time()

def exec_bikes_availability():
    global last_processed_time
    flux_query_bikes_to_recharge = f'''
        from(bucket: "{bucket}")
        |> range(start: -5m)
        |> filter(fn: (r) => r["_measurement"] == "plan_availability")
        |> last()
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

    tables = query_api.query(query=flux_query_bikes_to_recharge, org=org)
    if tables:
        for table in tables:
            for record in table.records:
                if record.get_time() > last_processed_time:
                    bike_id = record.values.get("bike_id")
                    station_id = record.values.get("station_id")
                    slot = record.values.get("slot")

                    reserve_slot(slot, bike_id, station_id)

                    #notifyies the operator
                    event_description = f"Put bike {bike_id} in charge at station {station_id} slot {slot}"
                    point = Point("event") \
                        .field("description", event_description)
                    write_api.write(bucket=bucket, record=point)

                    # avverte l'operatore
                    payload = {
                        "request": "CHARGE",
                        "slot": slot,
                        "bike_id": bike_id,
                        "station_id": station_id,
                    }
                    # avvisa l'operatore
                    client_mqtt.publish(f"ebike/operators/events", json.dumps(payload))
                    last_processed_time = record.get_time()

# execute the charge balance for each station
def exec_energy_waste():
    global last_processed_time_e
    flux_query_balance_charge = f'''
            from(bucket: "{bucket}")
            |> range(start: -5m)
            |> filter(fn: (r) => r["_measurement"] == "plan_energy_waste")
            |> last()
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''

    tables = query_api.query(query=flux_query_balance_charge, org=org)
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

def execute():
    exec_bikes_availability()
    exec_energy_waste()
    exec_structural_balance()

while True:
    execute()
    time.sleep(10)
