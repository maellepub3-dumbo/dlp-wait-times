import requests
import csv
import os
from datetime import datetime, timezone, timedelta

# IDs des deux parcs Disneyland Paris sur queue-times.com
PARKS = [
    {"id": 4,  "name": "Disneyland Park"},
    {"id": 28, "name": "Disney Adventure World"},
]

# Fuseau horaire de Paris
PARIS_TZ = timezone(timedelta(hours=2))  # UTC+2 en été, UTC+1 en hiver
# Pour gérer l'heure d'été automatiquement, on utilise la lib standard
try:
    from zoneinfo import ZoneInfo
    PARIS_TZ = ZoneInfo("Europe/Paris")
    now = datetime.now(PARIS_TZ)
except ImportError:
    now = datetime.now(timezone.utc) + timedelta(hours=2)

DATA_FILE = "data/wait_times.csv"
HEADERS = ["date", "time", "park", "land", "attraction", "wait_time", "is_open"]

def ensure_csv_exists():
    """Crée le fichier CSV avec les en-têtes s'il n'existe pas encore."""
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)
        print(f"Fichier créé : {DATA_FILE}")

def fetch_park(park_id):
    """Appelle l'API queue-times.com pour un parc donné."""
    url = f"https://queue-times.com/parks/{park_id}/queue_times.json"
    response = requests.get(url, timeout=15, headers={
        "User-Agent": "DLP-Collector/1.0 (GitHub Actions data collector)"
    })
    response.raise_for_status()
    return response.json()

def parse_rides(data, park_name):
    """Extrait toutes les attractions depuis la réponse JSON."""
    rides = []
    # Attractions regroupées par zone (land)
    for land in data.get("lands", []):
        land_name = land.get("name", "")
        for ride in land.get("rides", []):
            rides.append({
                "land": land_name,
                "name": ride.get("name", ""),
                "wait_time": ride.get("wait_time", 0),
                "is_open": ride.get("is_open", False),
            })
    # Attractions sans zone
    for ride in data.get("rides", []):
        rides.append({
            "land": "",
            "name": ride.get("name", ""),
            "wait_time": ride.get("wait_time", 0),
            "is_open": ride.get("is_open", False),
        })
    return rides

def collect():
    """Fonction principale : collecte et sauvegarde les données."""
    ensure_csv_exists()

    try:
        now = datetime.now(ZoneInfo("Europe/Paris"))
    except:
        now = datetime.utcnow() + timedelta(hours=2)

    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")

    total_rows = 0

    with open(DATA_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        for park in PARKS:
            try:
                data = fetch_park(park["id"])
                rides = parse_rides(data, park["name"])

                for ride in rides:
                    writer.writerow([
                        date_str,
                        time_str,
                        park["name"],
                        ride["land"],
                        ride["name"],
                        ride["wait_time"],
                        ride["is_open"],
                    ])
                    total_rows += 1

                print(f"✓ {park['name']} : {len(rides)} attractions collectées")

            except requests.RequestException as e:
                print(f"✗ Erreur pour {park['name']} : {e}")

    print(f"→ {total_rows} lignes ajoutées à {DATA_FILE} ({date_str} {time_str})")

if __name__ == "__main__":
    collect()
