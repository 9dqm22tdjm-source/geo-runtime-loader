import os, csv, zipfile

ZIP_PATH = "raw_dof/DOF_251026.zip"
EXTRACT_TO = "raw_dof"
DAT_FILENAME = "DOF.dat"
OUTPUT_CSV = "data/obstacles.csv"

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

def convert():
    # Step 1: Extract ZIP
    if not os.path.exists(ZIP_PATH):
        raise FileNotFoundError(f"Missing ZIP file: {ZIP_PATH}")
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(EXTRACT_TO)
        print(f"Extracted {ZIP_PATH} to {EXTRACT_TO}")

    # Step 2: Parse DOF.dat
 def find_dof_dat(root):
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            if f.lower() == "dof.dat":
                return os.path.join(dirpath, f)
    return None

dat_path = find_dof_dat(EXTRACT_TO)
if not dat_path:
    raise FileNotFoundError("DOF.dat not found after extraction.")

    with open(dat_path, "r") as infile, open(OUTPUT_CSV, "w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=[col[0] for col in columns])
        writer.writeheader()
        for line in infile:
            writer.writerow(parse_line(line))

    print(f"Converted DOF.dat to {OUTPUT_CSV}")

if __name__ == "__main__":
    convert()

