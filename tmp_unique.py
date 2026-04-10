import csv

input_file = "hit_random_peers_all_nodes.csv"
output_file = "hit_random_peers_all_nodes_unique.csv"

seen = set()

with open(input_file, "r", encoding="utf-8", newline="") as infile, \
     open(output_file, "w", encoding="utf-8", newline="") as outfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    for row in reader:
        if not row:
            continue

        key = row[0].strip()  # 第一列作为去重键

        if key not in seen:
            seen.add(key)
            writer.writerow(row)

print(f"去重完成，结果已保存到 {output_file}")