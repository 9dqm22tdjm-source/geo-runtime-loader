import os, csv, zipfile

ZIP_PATH = "raw_dof/DOF_251026.zip"
EXTRACT_TO = "raw_dof"
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
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(EXTRACT_TO)

    dat_path = os.path.join(EXTRACT_TO, "DOF.dat")
    with open(dat_path, "r") as infile, open(OUTPUT_CSV, "w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=[col[0] for col in columns])
        writer.writeheader()
        for line in infile:
            writer.writerow(parse_line(line))

if __name__ == "__main__":
    convert()
