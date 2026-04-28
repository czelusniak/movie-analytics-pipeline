from pathlib import Path
import csv

# 1. Define relative paths
script_dir = Path(__file__).parent.parent  # go up 1 level from ingestion/ to root
raw_dir = script_dir / "data" / "raw"
sample_dir = script_dir / "data" / "sample"

print(f"Raw CSVs directory: {raw_dir}")
print("-" * 50)

csv_files = ['movies.csv', 'user_rating_history.csv', 'ratings_for_additional_users.csv']

for file in csv_files:
    print(f"\n Processing: {file}")

    with open(raw_dir / file, 'r') as f_in:
        reader = csv.reader(f_in)
        header = next(reader)                       # read header
        rows = [next(reader) for _ in range(100)]   # read 100 rows

    output_file = file.replace('.csv', '_sample.csv')
    output_path = sample_dir / output_file

    with open(output_path, 'w', newline='') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(header)   # write header
        writer.writerows(rows)    # write rows

    print(f"   Saved to : {output_path}")

print("\n" + "-" * 50)
print(f"Done! {len(csv_files)} sample files created in: {sample_dir}")