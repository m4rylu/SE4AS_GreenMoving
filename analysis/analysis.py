import time
from datetime import timezone, datetime

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


#rendere l'analisi sia time based che vent based

# --- Configurazione ---
token = "9UAPy4qDu16TQSUe4G9EN88rzsnC1srqrhgwu4Kxg9asMCxLdkCq_NgZzUp2gpnAfSj5W-XTzjeIUEsA23CiIw=="
org = "GreenMoving"
bucket = "bike_monitoring"
url = "http://influxdb:8086"

SOGLIA_MOVIMENTO=0.0001

stations={}
bikes_history={}
last_processed_time_s = datetime.fromtimestamp(0, timezone.utc)

time.sleep(10)

client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()
write_api = client.write_api(write_options=SYNCHRONOUS)


def structural_balance_goal():
    global last_processed_time_s
    flux_query_stations = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r["_measurement"] == "station")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    # STATION ANALYSIS
    tables = query_api.query(query=flux_query_stations, org=org)
    for table in tables:
        for record in table.records:
            print("last_record", last_processed_time_s)
            print("new_time", record.get_time())
            if record.get_time() > last_processed_time_s:
                station_id = record.values.get("station_id")
                slots = {}
                for i in range(1, 6):
                    slots[f"slot{i}"] = (record.values.get(f"slot{i}"), record.values.get(f"slot{i}_rate"))

                # Logica degli stati
                is_full = all(s[0] != "empty" for s in slots.values())  # Tutte bici o RESERVED
                is_empty = all(s[0] == "empty" for s in slots.values())
                print(f"DEBUG ANALYSIS: record per {station_id} -> {slots}")# Nessuna bici, nessun impegno

                # --- FILTRO EVENTI ---
                # Scriviamo sul DB SOLO SE si verifica una delle due condizioni critiche
                if is_full or is_empty:
                    # Possiamo anche aggiungere un campo "status" per rendere Grafana più leggibile
                    event_msg = "FULL" if is_full else "EMPTY"
                    print("is_full", is_full)
                    print("is_empty", is_empty)

                    point = Point("structural_balance") \
                        .tag("station_id", station_id) \
                        .field("event_type", event_msg)
                    write_api.write(bucket=bucket, record=point)
                    print(f"[ANALYSIS] Rilevato stato {event_msg} per stazione {station_id}. Evento inviato.")

                point = Point("energy_waste") \
                    .tag("station_id", station_id) \
                    .field("event_type", "UPDATED")

                write_api.write(bucket=bucket, record=point)

                last_processed_time_s = record.get_time()

def bike_availability_goal():
    flux_query_bikes = f'''
    from(bucket: "{bucket}")
      |> range(start: -30d)
      |> filter(fn: (r) => r["_measurement"] == "bikes")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    # BIKES ANALYSIS
    tables = query_api.query(query=flux_query_bikes, org=org)
    for table in tables:
        for record in table.records:
            bike_id = record.values.get("bike_id")
            battery = record.values.get("battery")
            locked = record.values.get("motor_locked")
            is_charging = record.values.get("is_charging")
            is_available = record.values.get("is_available")
            lat = record.values.get("lat")
            lon = record.values.get("lon")


            # verifico carica
            if battery < 10 and not is_charging:
                point = Point("bike_availability") \
                    .tag("bike_id", bike_id) \
                    .field("event", "LOW_BATTERY")
                write_api.write(bucket=bucket, record=point)


            # verifico zona
            if not (42.3866 > lat > 42.3273) or not (13.4287 > lon > 13.3306):
                point = Point("bike_availability") \
                    .tag("bike_id", bike_id) \
                    .field("event", "OUT_OF_BOUNDS")
                write_api.write(bucket=bucket, record=point)

            # verifico bici disponibile per prenotazione
            #if battery > 50 and is_available:

            if battery == 100:
                point = Point("bike_availability") \
                    .tag("bike_id", bike_id) \
                    .field("event", "FULLY_CHARGED")
                write_api.write(bucket=bucket, record=point)

            # verifico furto
            if is_available and not is_charging and locked:
                if bike_id in bikes_history:
                    old_pos = bikes_history[bike_id]
                    # Calcoliamo quanto si è spostata
                    diff_lat = abs(lat - old_pos['lat'])
                    diff_lon = abs(lon - old_pos['lon'])

                    if locked and (diff_lat > SOGLIA_MOVIMENTO or diff_lon > SOGLIA_MOVIMENTO):
                        point = Point("bike_availability") \
                            .tag("bike_id", bike_id) \
                            .field("event", "THEFT_ALARM")
                        write_api.write(bucket=bucket, record=point)
                        print(f"!!! POSSIBILE FURTO BICI {bike_id} !!!")


            bikes_history[bike_id] = {'lat': lat, 'lon': lon}



def do_analysis():
    bike_availability_goal()
    structural_balance_goal()



while True:
    do_analysis()
    time.sleep(10)
