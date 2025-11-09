import os, csv

# Folder where your .dat files are stored
input_folder = "scripts/raw_dof"
output_file = "obstacles.csv"

# Define fixed-width columns from FAA DOF format
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
    return { name: line[start: end].strip() for name, start, end in columns}

# Write to CSV
with open(output_file, "w", newline= "") as outfile:
    writer = csv.DictWriter(outfile, fieldnames =[col[0] for col in columns])
    writer.writeheader()
    for filename in os.listdir(input_folder):
        if filename.endswith(".dat"):
            with open(os.path.join(input_folder, filename), "r") as infile:
                for line in infile:

                    writer.writerow(parse_line(line))
