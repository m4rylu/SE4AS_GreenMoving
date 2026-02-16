import time
import configparser
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

config = configparser.ConfigParser()
config.read('configuration/config.ini')

TOKEN = config.get('influx_db', 'token')
ORG = config.get('influx_db', 'org')
BUCKET = config.get('influx_db', 'bucket')
URL = config.get('influx_db', 'url')

N_SLOT = config.getint('system', 'n_slot_x_station')
RESET_TASK_TIME = config.getint('system', 'reset_task_time')
UPDATE_RATE = config.getint('update_rate', 'planning_update_rate')

stations={}
station_knowledge={}
bikes_history={}
last_processed_time_b = datetime.fromtimestamp(0, timezone.utc)
last_processed_time_s = datetime.fromtimestamp(0, timezone.utc)
last_processed_time_w = datetime.fromtimestamp(0, timezone.utc)
last_processed_time_w1 = datetime.fromtimestamp(0, timezone.utc)

active_tasks = {}
bikes = {}

def plan_energy_waste(station_id):
    charging_list = []
    charged_list = []

    if station_id not in stations:
        return

    for slots in stations[station_id]:
        # 1. Identifichiamo le bici che hanno bisogno di carica (escludiamo empty e RESERVED)
        # Supponiamo: bike_id = slots[s][0], battery = slots[s][3] (come nel tuo esempio)

        bike_id=stations[station_id][slots][0]

        if bike_id in ["empty", "RESERVED"]:
            continue

        bike_info = {"slot": slots, "bike_id": bike_id, "battery": bikes[bike_id]}
        if bikes[bike_id] < 100:
            charging_list.append(bike_info)
        else:
            charged_list.append(bike_info)


    if not charging_list and not charged_list:
        return

    rates = {}
    total_p = station_knowledge[station_id]["total_power"]

    # 2. Troviamo la bici con la batteria più alta (Priority Bike)
    # Usiamo sorted per averle tutte in ordine di batteria decrescente
    if charging_list:
        charging_list.sort(key=lambda x: x["battery"], reverse=True)

    # 3. Distribuzione Energia (Planning)
    # Strategia: 60% della potenza alla prioritaria, il resto diviso tra le altre
        if len(charging_list) == 1:
            if stations[station_id][charging_list[0]["slot"]][1] != total_p:
                rates[charging_list[0]["slot"]] = (total_p, charging_list[0]["bike_id"])
        else:
            priority_bike = charging_list[0]
            other_bikes = charging_list[1:]

            p_power = int(total_p * 0.6)
            o_power = int(total_p * 0.4 / len(other_bikes))

            #priority bike
            if stations[station_id][priority_bike["slot"]][1] != p_power:
                rates[priority_bike["slot"]] = (p_power, priority_bike["bike_id"])

            for b in other_bikes:
                if stations[station_id][b["slot"]][1] != o_power:
                    rates[b["slot"]] = (o_power, b["bike_id"])


    if charged_list:
        for b in charged_list:
            if stations[station_id][b["slot"]][1] != 0:
                rates[b["slot"]] = (0,b["bike_id"])

    if rates:
        for rate in rates:
            point = Point("plan_energy_waste") \
                .tag("station_id", station_id) \
                .field("slot", rate) \
                .field("rate", rates[rate][0]) \
                .field("bike_id", rates[rate][1])

            write_api.write(bucket=BUCKET, record=point)


def clean_old_tasks():
    now = time.time()
    for sid, task in list(active_tasks.items()):
        # Se il task è più vecchio di 15 minuti, lo consideriamo scaduto o fallito
        if now - task["timestamp"] > RESET_TASK_TIME:
            del active_tasks[sid]

def retrieve_station_knowledge():
    flux_query_station_loc = f'''
    from(bucket: "{BUCKET}")
    |> range(start: -1d)
    |> filter(fn: (r) => r["_measurement"] == "station_knowledge")
    |> last()
    |> sort(columns: ["_time"], desc: false)
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    tables = query_api.query(query=flux_query_station_loc, org=ORG)
    for table in tables:
        for record in table.records:
            station_id = record.values.get("station_id")
            lat = record.values.get("lat")
            lon = record.values.get("lon")
            address = record.values.get("address")
            total_power = record.values.get("total_power")
            station_knowledge[station_id] = {"lat": lat, "lon": lon, "address": address, "total_power": total_power}


def find_best_station(station_id, empty):

    def count_empty(sid):
        return sum(1 for data in stations[sid].values() if data[0] == "empty")

    if empty:
        # FULL: Cerco stazioni con ALMENO metà dei posti liberi (safe)
        safe_destinations = [
            sid for sid in stations
            if sid != station_id and count_empty(sid) >= N_SLOT // 2
        ]
    else:
        # EMPTY: Cerco stazioni con ALMENO metà dei posti occupati (per rubare una bici)
        safe_destinations = [
            sid for sid in stations
            if sid != station_id and count_empty(sid) <= N_SLOT // 2
        ]

    if not safe_destinations:
        return None
    print("safe_destinations", safe_destinations)

    # ORDINE: Usiamo la funzione count_empty che abbiamo già definito
    # Se empty=True (FULL), vogliamo quella con PIÙ empty (reverse=True)
    # Se empty=False (EMPTY), vogliamo quella con MENO empty (reverse=False)
    target_station = sorted(
        safe_destinations,
        key=count_empty,
        reverse=empty
    )[0]

    return target_station



def find_recharge_slot(station_id):
    best_station_id = find_best_station(station_id, True)

    if best_station_id:
        for slots in stations[best_station_id]:
            if stations[best_station_id][slots][0] == "empty":
                print(f"best station {best_station_id} and slot {slots} found")
                return best_station_id, slots

    # caso urgente in cui la bici va riaricata anche se riempie uno slot
    else:
        for station in stations:
            for slot in stations[station]:
                if stations[station][slot][0] == "empty":
                    print(f"best station {station} and slot {slot} found")
                    return station, slot
    return None, None

def find_bike_location(bike_id):
    for station in stations:
        for slot in stations[station]:
            if stations[station][slot][0] == bike_id:
                return station, slot
    return None,None

def retrieve_data_station():
    flux_query_stations = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -1d)
      |> filter(fn: (r) => r["_measurement"] == "station")
      |> last()
      |> sort(columns: ["_time"], desc: false)
      |> pivot(rowKey:["_time","station_id"], columnKey: ["_field"], valueColumn: "_value")
    '''

    # STATION DATA RETRIEVING
    tables = query_api.query(query=flux_query_stations, org=ORG)
    for table in tables:
        for record in table.records:
            station_id = record.values.get("station_id")
            stations[station_id] = {}
            for i in range(1, N_SLOT+1):
                stations[station_id][f"slot{i}"] = (record.values.get(f"slot{i}"), record.values.get(f"slot{i}_rate"))


def retrieve_data_bike():
    flux_query_bikes = f'''
    from(bucket: "{BUCKET}")
    |> range(start: -1d)
    |> filter(fn: (r) => r["_measurement"] == "bikes")
    |> last()
    |> sort(columns: ["_time"], desc: false)
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
    tables = query_api.query(query=flux_query_bikes, org=ORG)
    for table in tables:
        for record in table.records:
            bike_id = record.values.get("bike_id")
            battery = record.values.get("battery")
            bikes[bike_id] = battery

def plan_bike_recharging():
    global last_processed_time_b

    flux_query_event_bikes = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -1d)
      |> filter(fn: (r) => r["_measurement"] == "bike_recharging")
      |> last()
      |> sort(columns: ["_time"], desc: false)
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    # BIKE PLANNING
    tables = query_api.query(query=flux_query_event_bikes, org=ORG)
    new_max_time = last_processed_time_b
    if tables:
        for table in tables:
            for record in table.records:
                if record.get_time() > last_processed_time_b:
                    bike_id = record.values.get("bike_id")
                    event = record.values.get("event")

                    if event == "LOW_BATTERY":
                        if bike_id in active_tasks and active_tasks[bike_id]["type"] == "RECHARGE_PENDING":
                            print(f"Bici {bike_id} già in fase di ricarica. Salto la prenotazione.")

                        else:
                            stat, sl = find_recharge_slot(None)
                            point = Point("plan_recharging") \
                                .tag("bike_id", bike_id) \
                                .field("station_id", stat) \
                                .field("slot", sl)
                            write_api.write(bucket=BUCKET, record=point)
                            print(f"Bici {bike_id} pronta per essere ricaricata a {stat} in slot {sl}")
                            stations[stat][sl]=("RESERVED",0)

                            active_tasks[bike_id] = {
                                "type": "RECHARGE_PENDING",
                                "timestamp": time.time()
                            }

                    if event == "FULLY_CHARGED":
                        if bike_id in active_tasks and active_tasks[bike_id]["type"] == "BIKE_FULLY_CHARGED":
                            print("Bici già disconnessa")

                        else:
                            print("Bike_fully_charged")
                            station_id, slot = find_bike_location(bike_id)
                            plan_energy_waste(station_id)

                            active_tasks[bike_id] = {
                                "type": "BIKE_FULLY_CHARGED",
                                "timestamp": time.time()
                            }

                    if record.get_time() > new_max_time:
                        new_max_time = record.get_time()

        last_processed_time_b = new_max_time

def plan_structural_balance():
    global last_processed_time_s

    flux_query_event_station = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -10d)
      |> filter(fn: (r) => r["_measurement"] == "structural_balance")
      |> last()
      |> sort(columns: ["_time"], desc: false)
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    # STATION PLANNING
    tables = query_api.query(query=flux_query_event_station, org=ORG)
    if tables:
        record = tables[0].records[0]
        if record.get_time() > last_processed_time_s:
            last_processed_time_s = record.get_time()
            station_id = record.values.get("station_id")
            event = record.values.get("event_type")

            if event=='FULL':
                print("Ricevuto FULL da analysis")
                print(f"DEBUG: Stazioni caricate: {list(stations.keys())}")
                if station_id in active_tasks and active_tasks[station_id]["type"] == "STATIONS_REBALANCING":
                    print(f"Sto già cercando di riempire la stazione {station_id}, non fare nulla.")
                    return

                for sid in stations:
                    print(f"DEBUG: {sid} ha {sum(1 for s, _ in stations[sid].items() if s == 'empty')} slot vuoti")

                # 1. Identifichiamo la bici da spostare (non prenotata)
                bike_to_move = None
                for s in stations[station_id]:
                    if stations[station_id][s][0] != "RESERVED" and stations[station_id][s][0] != "empty":
                        bike_to_move=(station_id, s, stations[station_id][s][0])
                        print("FULL, bike to move:", bike_to_move)
                        break

                # 2. Creiamo una lista di stazioni di destinazione ordinate per numero di slot liberi
                target_station=find_best_station(station_id, True)

                # 3. Distribuiamo la bici nelle stazioni più vuote
                if target_station and bike_to_move:
                    for target_slot, status in stations[target_station].items():
                        if status[0] == "empty":
                            print(f"[PLANNING] Rebalancing: {bike_to_move[2]} -> {target_station} (Slot più libero: {target_slot})")

                            point = Point("plan_structural_balance") \
                                .tag("bike_id", bike_to_move[2]) \
                                .field("station_id_start", bike_to_move[0]) \
                                .field("slot_start", bike_to_move[1]) \
                                .field("station_id_end", target_station) \
                                .field("slot_end", target_slot)
                            write_api.write(bucket=BUCKET, record=point)
                            active_tasks[station_id] = {
                                "type": "STATIONS_REBALANCING",
                                "timestamp": time.time()
                            }
                            print("EMPTY, bike to move:", bike_to_move)
                            break


            elif event == 'EMPTY':
                print(f"Ricevuto EMPTY da analysis per {station_id}")
                if station_id in active_tasks and active_tasks[station_id]["type"] == "STATIONS_REBALANCING":
                    print(f"Sto già cercando di riempire la stazione {station_id}, non fare nulla.")
                    return
                # Trovo la bici da spostare
                target_station=find_best_station(station_id, False)
                print("target station",target_station)

                bike_to_move = None
                if not target_station:
                    print(f"[PLANNING] Nessuna stazione trovata")
                    return

                for target_slot, status in stations[target_station].items():
                    if status[0] != "empty" and status[0] != "RESERVED":
                        bike_to_move = (target_station, target_slot, status[0])
                        print("EMPTY, bike to move:", bike_to_move)
                        break

                #trovo il primo slot libero
                if target_station and bike_to_move:
                    for slot in stations[station_id]:
                        if stations[station_id][slot][0] == "empty":
                            print(f"[PLANNING] Rebalancing: {bike_to_move[2]} -> {station_id} (Slot più libero: {slot})")
                            point = Point("plan_structural_balance") \
                                .tag("bike_id", bike_to_move[2]) \
                                .field("station_id_start", target_station) \
                                .field("slot_start", target_slot) \
                                .field("station_id_end", station_id) \
                                .field("slot_end", slot)
                            write_api.write(bucket=BUCKET, record=point)
                            active_tasks[station_id] = {
                                "type": "STATIONS_REBALANCING",
                                "timestamp": time.time()
                            }
                            break


def retrieve_energy_waste_data():
    global last_processed_time_w
    flux_query_energy_waste = f'''
        from(bucket: "{BUCKET}")
          |> range(start: -1d)
          |> filter(fn: (r) => r["_measurement"] == "energy_waste")
          |> last()
          |> sort(columns: ["_time"], desc: false)
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
    # STATION PLANNING
    tables = query_api.query(query=flux_query_energy_waste, org=ORG)
    new_max_time = last_processed_time_w
    if tables:
        for table in tables:
            for record in table.records:
                if record.get_time() > last_processed_time_w:
                    station_id = record.values.get("station_id")
                    plan_energy_waste(station_id)
                    print(f"RECEIVED UPDATE EVENT from {station_id}")
                if record.get_time() > new_max_time:
                    new_max_time = record.get_time()

        last_processed_time_w = new_max_time

def plan_bike_book():
    global last_processed_time_w1
    flux_query_book_bike = f'''
            from(bucket: "{BUCKET}")
              |> range(start: -1d)
              |> filter(fn: (r) => r["_measurement"] == "book_bike")
              |> last()
              |> sort(columns: ["_time"], desc: false)
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''

    tables = query_api.query(query=flux_query_book_bike, org=ORG)
    new_max_time = last_processed_time_w1

    if tables:
        for table in tables:
            for record in table.records:
                if record.get_time() > last_processed_time_w1:
                    bike_id = record.values.get("bike_id")
                    event = record.values.get("event")

                    if event == 'START':
                        found_in_station = False

                        # CICLO CORRETTO: stations[station_id][slot][0]
                        for st_id in stations:
                            for sl_id in stations[st_id]:
                                # Accediamo all'elemento [0] che contiene il bike_id
                                if stations[st_id][sl_id][0] == bike_id:
                                    # Salviamo i riferimenti
                                    actual_station = st_id
                                    actual_slot = sl_id

                                    # Liberiamo lo slot mettendo "empty" (o come lo gestisci tu)
                                    stations[st_id][sl_id] = ("empty", 0)

                                    # Scriviamo il log su Influx con i dati reali trovati
                                    point = Point("plan_book_bike") \
                                        .tag("bike_id", bike_id) \
                                        .field("station_start", actual_station) \
                                        .field("slot_start", actual_slot) \
                                        .field("event", event)
                                    write_api.write(bucket=BUCKET, record=point)

                                    found_in_station = True
                                    print(f"DEBUG: Bici {bike_id} rimossa da {actual_station} (slot {actual_slot})")
                                    break  # Esci dai cicli slot
                            if found_in_station: break  # Esci dai cicli stazioni

                        if not found_in_station:
                            # Caso in cui la bici non era in nessuna stazione registrata
                            point = Point("plan_book_bike") \
                                .tag("bike_id", bike_id) \
                                .field("station_start", "not_found") \
                                .field("slot_start", "none") \
                                .field("event", event)
                            write_api.write(bucket=BUCKET, record=point)
                            print(f"DEBUG: Bici {bike_id} non trovata nei rack.")

                    else:  # Caso EVENT == 'END'
                        point = Point("plan_book_bike") \
                            .tag("bike_id", bike_id) \
                            .field("event", "END_RECOGNIZED")
                        write_api.write(bucket=BUCKET, record=point)

                if record.get_time() > new_max_time:
                    new_max_time = record.get_time()

        last_processed_time_w1 = new_max_time



def do_planning():
    clean_old_tasks()
    retrieve_data_station()
    retrieve_data_bike()
    retrieve_energy_waste_data()
    plan_bike_book()
    plan_bike_recharging()
    plan_structural_balance()

if __name__ == "__main__":
    client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    time.sleep(15)

    retrieve_station_knowledge()
    while True:
        do_planning()
        time.sleep(UPDATE_RATE)
