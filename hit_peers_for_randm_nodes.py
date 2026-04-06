import csv
import sys


def safe_percentage(hit_peers: str, responded_peers: str) -> str:
    try:
        hit = int(hit_peers)
        responded = int(responded_peers)
        if responded == 0:
            return "0.00%"
        return f"{hit / responded * 100:.2f}%"
    except (ValueError, TypeError):
        return "NA"


def main():
    if len(sys.argv) < 3:
        print("Usage: python3 process_findnode_summary.py input.csv output.csv")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    with open(input_file, "r", encoding="utf-8", newline="") as f_in:
        reader = csv.reader(f_in)
        rows = list(reader)

    if not rows:
        print("Input CSV is empty.")
        sys.exit(1)

    header = rows[0]

    if len(header) < 4:
        print("Input CSV format is invalid. Need at least 4 columns.")
        sys.exit(1)

    # 去掉最后一列，并追加 hit_rate
    new_header = header[:-1] + ["hit_rate"]
    output_rows = [new_header]

    for row in rows[1:]:
        if not row:
            continue

        # 行长度不足时补空
        while len(row) < len(header):
            row.append("")

        target_id = row[0]
        total_peers = row[1]
        responded_peers = row[2]
        hit_peers = row[3]

        hit_rate = safe_percentage(hit_peers, responded_peers)

        # 去掉最后一列
        new_row = row[:-1] + [hit_rate]
        output_rows.append(new_row)

    with open(output_file, "w", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out)
        writer.writerows(output_rows)

    print(f"Done. Output written to: {output_file}")


if __name__ == "__main__":
    main()