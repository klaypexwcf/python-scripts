#!/usr/bin/env python3
import re
import sys

def extract_peer_ips_from_bottom(log_path, out_path):
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    header_pattern = re.compile(r"^=+\s*Peer stats: all \d+, active \d+, passive \d+")
    peer_pattern = re.compile(r"^Peer\s+/(\d+\.\d+\.\d+\.\d+):\d+")

    header_idx = -1

    # 从底部向上找，找到第一次匹配的 Peer stats
    for i in range(len(lines) - 1, -1, -1):
        if header_pattern.match(lines[i].strip()):
            header_idx = i
            break

    if header_idx == -1:
        print("未找到目标 Peer stats 块")
        return

    ips = []
    seen = set()

    # 从该 header 往下扫描，提取 Peer /IP:PORT
    for i in range(header_idx + 1, len(lines)):
        line = lines[i].strip()

        # 如果遇到下一段统计块或新的分隔标题，就停止
        if i > header_idx + 1 and header_pattern.match(line):
            break
        if line.startswith("====") and not header_pattern.match(line):
            break

        m = peer_pattern.match(line)
        if m:
            ip = m.group(1)
            if ip not in seen:
                seen.add(ip)
                ips.append(ip)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(",".join(ips))

    print(f"提取完成，共 {len(ips)} 个 IP，已写入: {out_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"用法: {sys.argv[0]} <日志文件> <输出文件>")
        sys.exit(1)

    extract_peer_ips_from_bottom(sys.argv[1], sys.argv[2])