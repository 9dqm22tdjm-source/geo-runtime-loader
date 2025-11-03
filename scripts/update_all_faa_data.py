import os
import hashlib
import requests
import certifi
import csv
import subprocess
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Setup session with retries and certifi
session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)

# URLs for FAA and airport data
DATA_SOURCES = {
    "DOF.dat": {
        "url": "https://www.faa.gov/sites/faa.gov/files/2023-11/DOF.dat",
        "raw_path": "raw_dof/DOF.dat",
        "checksum_path": "raw_dof/DOF.checksum",
        "convert": True
    },
    "airports.csv": {
        "url": "https://ourairports.com/data/airports.csv",
        "raw_path": "raw_dof/airports.csv",
        "checksum_path": "raw_dof/airports.checksum",
        "convert": False
    },
    "airspace.geojson": {
        "url": "https://io-aero.github.io/io-olympus/io-data-sources/data_sources/io-xpa-core/FAA/sua.geojson",
        "raw_path": "raw_dof/airspace.geojson",
        "checksum_path": "raw_dof/airspace.checksum",
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

        if os.path.exists(info["checksum_path"]):
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

if updated:
    push_to_git()
else:
    print("All data is up to date. No commit needed.")
