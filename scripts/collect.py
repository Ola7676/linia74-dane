import csv
import io
import os
import zipfile
from datetime import datetime, timezone

import requests
from google.transit import gtfs_realtime_pb2

TARGET_LINES = {"74", "704", "708"}

GTFS_ZIP_URL = "https://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip"
TRIP_UPDATES_URL = "https://gtfs.ztp.krakow.pl/TripUpdates_T.pb"
SERVICE_ALERTS_URL = "https://gtfs.ztp.krakow.pl/ServiceAlerts_T.pb"

DATA_DIR = "data"
TRIP_UPDATES_CSV = os.path.join(DATA_DIR, "trip_updates.csv")
SERVICE_ALERTS_CSV = os.path.join(DATA_DIR, "service_alerts.csv")
DEBUG_LOG = os.path.join(DATA_DIR, "debug_log.txt")


def get_trip_and_route_maps():
    resp = requests.get(GTFS_ZIP_URL, timeout=30)
    resp.raise_for_status()

    route_short_name_by_id = {}
    trip_to_route = {}

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        with z.open("routes.txt") as f:
            text = io.TextIOWrapper(f, encoding="utf-8-sig")
            for row in csv.DictReader(text):
                route_short_name_by_id[row["route_id"]] = row["route_short_name"]

        with z.open("trips.txt") as f:
            text = io.TextIOWrapper(f, encoding="utf-8-sig")
            for row in csv.DictReader(text):
                trip_to_route[row["trip_id"]] = row["route_id"]

    trip_to_short_name = {
        trip_id: route_short_name_by_id.get(route_id)
        for trip_id, route_id in trip_to_route.items()
    }
    return route_short_name_by_id, trip_to_short_name


def ensure_header(path, header):
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(header)


def resolve_short_name(route_short_name_by_id, trip_to_short_name, route_id, trip_id):
    if route_id and route_id in route_short_name_by_id:
        return route_short_name_by_id[route_id]
    return trip_to_short_name.get(trip_id)


def collect_trip_updates(route_short_name_by_id, trip_to_short_name):
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
    total_entities = 0
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        total_entities += 1
        tu = entity.trip_update
        route_id = tu.trip.route_id
        trip_id = tu.trip.trip_id
        short_name = resolve_short_name(route_short_name_by_id, trip_to_short_name, route_id, trip_id)
        if short_name not in TARGET_LINES:
            continue
        for stu in tu.stop_time_update:
            arrival_delay = stu.arrival.delay if stu.HasField("arrival") else ""
            departure_delay = stu.departure.delay if stu.HasField("departure") else ""
            rows.append([
                now, short_name, route_id, trip_id,
                stu.stop_id, stu.stop_sequence, arrival_delay, departure_delay,
            ])

    if rows:
        with open(TRIP_UPDATES_CSV, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)

    with open(DEBUG_LOG, "a", encoding="utf-8") as f:
        f.write(f"{now} | trip_updates: encji={total_entities}, dopasowanych_wierszy={len(rows)}\n")

    print(f"Trip updates: zapisano {len(rows)} wierszy (encji w feedzie: {total_entities})")


def collect_service_alerts(route_short_name_by_id, trip_to_short_name):
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
            short_name = resolve_short_name(
                route_short_name_by_id, trip_to_short_name, ie.route_id, ie.trip.trip_id
            )
            if short_name in TARGET_LINES:
                matched_routes.append((short_name, ie.route_id))

        for short_name, route_id in matched_routes:
            rows.append([now, entity.id, short_name, route_id, header_text, description_text])

    if rows:
        with open(SERVICE_ALERTS_CSV, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)
    print(f"Service alerts: zapisano {len(rows)} wierszy")


def main():
    route_short_name_by_id, trip_to_short_name = get_trip_and_route_maps()
    collect_trip_updates(route_short_name_by_id, trip_to_short_name)
    collect_service_alerts(route_short_name_by_id, trip_to_short_name)


if __name__ == "__main__":
    main()
