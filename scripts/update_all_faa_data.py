import os
import sys
import hashlib
import requests
import certifi
import csv
import subprocess
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

FORCE_DOWNLOAD = "--force" in sys.argv

# Setup session with retries and certifi
session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)

# URLs for FAA and airport data
DATA_SOURCES = {
    "DOF.dat": {
        "url": "https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/dof/",  # Manual fallback
        "raw_path": "raw_dof/DOF.dat",
        "checksum_path": "raw_dof/DOF.checksum",
        "convert": True
    },
    "airports.csv": {
        "url": "https://davidmegginson.github.io/ourairports-data/airports.csv",
        "raw_path": "raw_dof/airports.csv",
        "checksum_path": "raw_dof/airports.checksum",
        "convert": False
    }
}

OUTPUT_DIR = "data"
os.makedirs("raw_dof", exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_checksum(content):
    return hashlib.md5(content).hexdigest()

def download_and_check(name, info):
    print(f"Checking {name}...")
    try:
        r = session.get(info["url"], verify=certifi.where(), timeout=10)
        if r.status_code != 200:
            print(f"Failed to download {name} (status {r.status_code})")
            return False

        new_checksum = get_checksum(r.content)

        if not FORCE_DOWNLOAD and os.path.exists(info["checksum_path"]):
            with open(info["checksum_path"], "r") as f:
                if f.read().strip() == new_checksum:
                    print(f"No update needed for {name}")
                    return False

        with open(info["raw_path"], "wb") as f:
            f.write(r.content)
        with open(info["checksum_path"], "w") as f:
            f.write(new_checksum)

        print(f"Downloaded new {name}")
        return True

    except Exception as e:
        print(f"Error downloading {name}: {e}")
        return False

def download_airspace_geojson():
    print("Downloading FAA SUA airspace.geojson from ArcGIS API...")
    try:
        url = "https://opendata.arcgis.com/api/v3/datasets/dd0d1b726e504137ab3c41b21835d05b_0/downloads/data?format=geojson&spatialRefId=4326"
        r = session.get(url, verify=certifi.where(), timeout=15)
        if r.status_code != 200:
            print(f"Failed to download airspace.geojson (status {r.status_code})")
            return False

        new_checksum = get_checksum(r.content)
        checksum_path = "raw_dof/airspace.checksum"
        raw_path = "raw_dof/airspace.geojson"

        if not FORCE_DOWNLOAD and os.path.exists(checksum_path):
            with open(checksum_path, "r") as f:
                if f.read().strip() == new_checksum:
                    print("No update needed for airspace.geojson")
                    return False

        with open(raw_path, "wb") as f:
            f.write(r.content)
        with open(checksum_path, "w") as f:
            f.write(new_checksum)

        copy_file(raw_path, "airspace.geojson")
        print("Downloaded new airspace.geojson")
        return True

    except Exception as e:
        print(f"Error downloading airspace.geojson: {e}")
        return False

def convert_dof_to_csv():
    print("Converting DOF.dat to obstacles.csv...")
    columns = [
        ("state", 0, 2),
        ("obstacle_number", 3, 12),
        ("latitude", 13, 21),
        ("longitude", 22, 31),
        ("elevation_agl", 32, 37),
        ("elevation_amsl", 38, 43),
        ("lighting", 44, 45),
        ("marking", 46, 47),
        ("type", 48, 53),
        ("date", 54, 62)
    ]

    def parse_line(line):
        return {name: line[start:end].strip() for name, start, end in columns}

    with open(DATA_SOURCES["DOF.dat"]["raw_path"], "r") as infile:
        with open(os.path.join(OUTPUT_DIR, "obstacles.csv"), "w", newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=[col[0] for col in columns])
            writer.writeheader()
            for line in infile:
                writer.writerow(parse_line(line))

def copy_file(src, dest_name):
    dest_path = os.path.join(OUTPUT_DIR, dest_name)
    with open(src, "rb") as fsrc:
        with open(dest_path, "wb") as fdst:
            fdst.write(fsrc.read())

def push_to_git():
    subprocess.run(["git", "config", "--global", "user.name", "FAA Bot"])
    subprocess.run(["git", "config", "--global", "user.email", "faa-bot@example.com"])
    subprocess.run(["git", "add", "."], cwd=".")
    subprocess.run(["git", "diff", "--cached", "--quiet"])  # Check if there are changes
    subprocess.run(["git", "commit", "-m", "Auto-update FAA data"], cwd=".")
    subprocess.run(["git", "push", "origin", "main"], cwd=".")

# Main pipeline
updated = False
for name, info in DATA_SOURCES.items():
    if download_and_check(name, info):
        updated = True
        if info["convert"] and name == "DOF.dat":
            convert_dof_to_csv()
        else:
            copy_file(info["raw_path"], name)

if download_airspace_geojson():
    updated = True

if updated:
    push_to_git()
else:
    print("All data is up to date. No commit needed.")
