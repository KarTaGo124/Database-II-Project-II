import csv
import os

input_file = "data/datasets/styles.csv"
output_dir = "data/datasets/styles"

os.makedirs(output_dir, exist_ok=True)

sizes = [1000, 2000, 4000, 8000, 16000, 32000]

with open(input_file, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    header = next(reader)
    all_rows = list(reader)

total_rows = len(all_rows)
print(f"Total rows: {total_rows}")

for size in sizes:
    output_file = os.path.join(output_dir, f"styles_{size}.csv")
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(all_rows[:size])
    print(f"Created {output_file} with {size} rows")

full_output = os.path.join(output_dir, f"styles_{total_rows}.csv")
with open(full_output, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(all_rows)
print(f"Created {full_output} with {total_rows} rows")

print("Done!")
