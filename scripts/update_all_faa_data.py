import os, hashlib, requests, certifi, subprocess
from convert_dof import convert

DATA_SOURCES = {
    "DOF_251026.zip": {
        "url": "https://aeronav.faa.gov/Obst_Data/DOF_251026.zip",
        "raw_path": "raw_dof/DOF_251026.zip",
        "checksum_path": "raw_dof/DOF_251026.checksum",
        "convert": "dof"
    },
    "airports.csv": {
        "url": "https://ourairports.com/data/airports.csv",
        "raw_path": "raw_dof/airports.csv",
        "checksum_path": "raw_dof/airports.checksum",
        "convert": False
    },
    "airspace.geojson": {
        "url": "https://opendata.arcgis.com/api/v3/datasets/dd0d1b726e504137ab3c41b21835d05b_0/downloads/data?format=geojson&spatialRefId=4326",
        "raw_path": "raw_dof/airspace.geojson",
        "checksum_path": "raw_dof/airspace.checksum",
        "convert": False
    },
    "victor_airways.geojson": {
        "url": "https://opendata.arcgis.com/api/v3/datasets/1e3a3f8f2c7d4e3c9b1a8b9c9e2f3a7f_0/downloads/data?format=geojson&spatialRefId=4326",
        "raw_path": "raw_dof/victor_airways.geojson",
        "checksum_path": "raw_dof/victor_airways.checksum",
        "convert": False
    },
    "jet_routes.geojson": {
        "url": "https://opendata.arcgis.com/api/v3/datasets/2f4b6d7e8a9c4f3b8d2e9c7a1b3f4d6e_0/downloads/data?format=geojson&spatialRefId=4326",
        "raw_path": "raw_dof/jet_routes.geojson",
        "checksum_path": "raw_dof/jet_routes.checksum",
        "convert": False
    },
    "rnav_routes.geojson": {
        "url": "https://opendata.arcgis.com/api/v3/datasets/3c5d7e8f9a1b4c2d9e3f7a6b2c4d5e7f_0/downloads/data?format=geojson&spatialRefId=4326",
        "raw_path": "raw_dof/rnav_routes.geojson",
        "checksum_path": "raw_dof/rnav_routes.checksum",
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
        r = requests.get(info["url"], verify=certifi.where(), timeout=15)
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

def copy_file(src, dest_name):
    dest_path = os.path.join(OUTPUT_DIR, dest_name)
    if os.path.exists(src):
        with open(src, "rb") as fsrc, open(dest_path, "wb") as fdst:
            fdst.write(fsrc.read())

def push_to_git():
    subprocess.run(["git", "config", "--global", "user.name", "FAA Bot"])
    subprocess.run(["git", "config", "--global", "user.email", "faa-bot@example.com"])
    subprocess.run(["git", "add", "."], cwd=".")
    subprocess.run(["git", "diff", "--cached", "--quiet"])
    subprocess.run(["git", "commit", "-m", "Auto-update FAA data"], cwd=".")
    subprocess.run(["git", "push", "origin", "main"], cwd=".")

updated = False
for name, info in DATA_SOURCES.items():
    if download_and_check(name, info):
        updated = True
        if info["convert"] == "dof":
            convert()
        else:
            copy_file(info["raw_path"], name)

if updated:
    push_to_git()
else:
    print("All data is up to date. No commit needed.")
