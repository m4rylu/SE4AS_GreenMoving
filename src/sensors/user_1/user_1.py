import random
import time
import configparser
from datetime import timezone, datetime, timedelta  # Aggiunto timedelta
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

config = configparser.ConfigParser()
config.read('configuration/config.ini')

TOKEN = config.get('influx_db', 'token')
ORG = config.get('influx_db', 'org')
BUCKET = config.get('influx_db', 'bucket')
URL = config.get('influx_db', 'url')

# Inizializza con il tempo attuale meno un giorno per recuperare dati recenti
last_processed_time = datetime.now(timezone.utc) - timedelta(days=1)
booked_bikes = []


def book_random_bike():
    global last_processed_time

    flux_query_available_bikes = f'''
        from(bucket: "{BUCKET}")
          |> range(start: -30d)
          |> filter(fn: (r) => r["_measurement"] == "bike_availability")
          |> last()
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

    tables = query_api.query(query=flux_query_available_bikes, org=ORG)
    new_max_time = last_processed_time
    available_bikes = {}

    if tables:
        for table in tables:
            for record in table.records:
                # Recupero dati dal record
                bike_id = record.values.get("bike_id")
                minutes = record.values.get("minutes")
                event = record.values.get("event")# Default 0 se manca

                # Se la bici ha minuti disponibili (batteria/autonomia)
                if event == "AVAILABLE":
                    available_bikes[bike_id] = minutes

                if record.get_time() > new_max_time:
                    new_max_time = record.get_time()

        last_processed_time = new_max_time

        # Se ci sono bici disponibili e il caso (50%) lo permette
        if available_bikes and random.random() < 0.4:
            # Scegli una bike_id a caso dal dizionario
            bike_id = random.choice(list(available_bikes.keys()))
            max_minutes = available_bikes[bike_id]

            if bike_id in booked_bikes:
                return

            # random.randint per numeri interi
            minutes_book = random.randint(1, max(1, int(max_minutes)))

            # Usiamo il tempo REALE attuale
            time_start = datetime.now(timezone.utc)
            time_end = time_start + timedelta(minutes=minutes_book)

            point = Point("bookings") \
                .tag("bike_id", bike_id) \
                .field("user_id", "U1") \
                .field("time_start", time_start.isoformat()) \
                .field("time_end", time_end.isoformat()) \
                .field("event", "START")

            write_api.write(bucket=BUCKET, record=point)
            booked_bikes.append(bike_id)
            print(f"--- PRENOTATA BICI {bike_id}: {minutes_book} minuti ---")


if __name__ == "__main__":
    # Setup Client
    client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    print("User Simulator avviato... in attesa di bici disponibili.")
    time.sleep(20)

    while True:
        book_random_bike()
        time.sleep(15)