import csv
import io
import os
import zipfile
from datetime import datetime, timezone

import requests
from google.transit import gtfs_realtime_pb2

TARGET_LINES = {"74", "704", "708", "18", "50"}

GTFS_ZIP_URL = "https://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip"
TRIP_UPDATES_URL = "https://gtfs.ztp.krakow.pl/TripUpdates_T.pb"
SERVICE_ALERTS_URL = "https://gtfs.ztp.krakow.pl/ServiceAlerts_T.pb"

DATA_DIR = "data"
TRIP_UPDATES_CSV = os.path.join(DATA_DIR, "trip_updates.csv")
SERVICE_ALERTS_CSV = os.path.join(DATA_DIR, "service_alerts.csv")


def get_route_map():
    resp = requests.get(GTFS_ZIP_URL, timeout=30)
    resp.raise_for_status()
    route_map = {}
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        with z.open("routes.txt") as f:
            text = io.TextIOWrapper(f, encoding="utf-8-sig")
            reader = csv.DictReader(text)
            for row in reader:
                route_map[row["route_id"]] = row["route_short_name"]
    return route_map


def ensure_header(path, header):
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(header)


def collect_trip_updates(route_map):
    feed = gtfs_realtime_pb2.FeedMessage()
    resp = requests.get(TRIP_UPDATES_URL, timeout=30)
    resp.raise_for_status()
    feed.ParseFromString(resp.content)

    header = [
        "collected_at", "route_short_name", "route_id", "trip_id",
        "stop_id", "stop_sequence", "arrival_delay_s", "departure_delay_s",
    ]
    ensure_header(TRIP_UPDATES_CSV, header)

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        route_id = tu.trip.route_id
        short_name = route_map.get(route_id)
        if short_name not in TARGET_LINES:
            continue
        for stu in tu.stop_time_update:
            arrival_delay = stu.arrival.delay if stu.HasField("arrival") else ""
            departure_delay = stu.departure.delay if stu.HasField("departure") else ""
            rows.append([
                now, short_name, route_id, tu.trip.trip_id,
                stu.stop_id, stu.stop_sequence, arrival_delay, departure_delay,
            ])

    if rows:
        with open(TRIP_UPDATES_CSV, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)
    print(f"Trip updates: zapisano {len(rows)} wierszy")


def collect_service_alerts(route_map):
    feed = gtfs_realtime_pb2.FeedMessage()
    resp = requests.get(SERVICE_ALERTS_URL, timeout=30)
    resp.raise_for_status()
    feed.ParseFromString(resp.content)

    header = [
        "collected_at", "alert_id", "route_short_name", "route_id",
        "header_text", "description_text",
    ]
    ensure_header(SERVICE_ALERTS_CSV, header)

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue
        alert = entity.alert
        header_text = alert.header_text.translation[0].text if alert.header_text.translation else ""
        description_text = alert.description_text.translation[0].text if alert.description_text.translation else ""

        matched_routes = []
        for ie in alert.informed_entity:
            short_name = route_map.get(ie.route_id)
            if short_name in TARGET_LINES:
                matched_routes.append((short_name, ie.route_id))

        if not matched_routes:
            continue

        for short_name, route_id in matched_routes:
            rows.append([now, entity.id, short_name, route_id, header_text, description_text])

    if rows:
        with open(SERVICE_ALERTS_CSV, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)
    print(f"Service alerts: zapisano {len(rows)} wierszy")


def main():
    route_map = get_route_map()
    collect_trip_updates(route_map)
    collect_service_alerts(route_map)


if __name__ == "__main__":
    main()
