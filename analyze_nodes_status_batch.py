#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import csv
import math
import argparse
import statistics
from datetime import datetime, date, time, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import matplotlib.pyplot as plt


IP_PATTERN = re.compile(
    r'\b(?:25[0-5]|2[0-4]\d|1?\d?\d)'
    r'(?:\.(?:25[0-5]|2[0-4]\d|1?\d?\d)){3}\b'
)


def parse_line(line: str) -> Optional[Tuple[str, str]]:
    """
    支持两种格式：
    1) timestamp<TAB>neighbors
    2) timestamp,neighbors   （只在第一个逗号处分割）
    """
    line = line.strip()
    if not line:
        return None

    if "\t" in line:
        ts, neighbors = line.split("\t", 1)
    elif "," in line:
        ts, neighbors = line.split(",", 1)
    else:
        return None

    ts = ts.strip()
    neighbors = neighbors.strip()

    if ts.lower() == "timestamp":
        return None

    return ts, neighbors


def parse_timestamp_series(ts_list: List[str]) -> List[datetime]:
    """
    支持：
    1) YYYY-MM-DD HH:MM:SS.mmm
    2) HH:MM:SS.mmm

    对于只有时分秒的情况：
    - 默认从一个虚拟日期开始
    - 如果后一个时间小于前一个时间，视为跨天，日期 +1
    """
    results: List[datetime] = []

    has_full_date = any(len(ts) >= 10 and "-" in ts[:10] for ts in ts_list)

    if has_full_date:
        for ts in ts_list:
            ts = ts.strip()
            try:
                results.append(datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f"))
            except ValueError:
                results.append(datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"))
        return results

    base_day = date(2000, 1, 1)
    day_offset = 0
    prev_t: Optional[time] = None

    for ts in ts_list:
        ts = ts.strip()
        try:
            t = datetime.strptime(ts, "%H:%M:%S.%f").time()
        except ValueError:
            t = datetime.strptime(ts, "%H:%M:%S").time()

        if prev_t is not None and t < prev_t:
            day_offset += 1

        dt = datetime.combine(base_day + timedelta(days=day_offset), t)
        results.append(dt)
        prev_t = t

    return results


def midpoint(t1: datetime, t2: datetime) -> datetime:
    return t1 + (t2 - t1) / 2


def load_snapshots(file_path: Path) -> List[Dict]:
    raw_rows = []

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line_no, line in enumerate(f, 1):
            parsed = parse_line(line)
            if parsed is None:
                continue
            ts_str, neighbors_str = parsed
            neighbors = set(IP_PATTERN.findall(neighbors_str))
            raw_rows.append({
                "line_no": line_no,
                "ts_str": ts_str,
                "neighbors": neighbors,
            })

    if not raw_rows:
        return []

    dt_list = parse_timestamp_series([row["ts_str"] for row in raw_rows])

    snapshots = []
    for row, dt in zip(raw_rows, dt_list):
        snapshots.append({
            "line_no": row["line_no"],
            "ts_str": row["ts_str"],
            "dt": dt,
            "neighbors": row["neighbors"],
        })

    return snapshots


def analyze_ip(snapshots: List[Dict], target_ip: str):
    """
    保持和你现有 analyze_nodes_status.py 一致的核心逻辑：
    - 忽略开头没有前文的连接段
    - 忽略结尾没有后文的连接段
    - 用相邻快照的中点估计连接开始/结束时间
    """
    if not snapshots:
        return [], []

    present = [target_ip in snap["neighbors"] for snap in snapshots]
    n = len(snapshots)

    sessions = []
    i = 0
    while i < n:
        if not present[i]:
            i += 1
            continue

        start_idx = i
        while i + 1 < n and present[i + 1]:
            i += 1
        end_idx = i

        if start_idx == 0 or end_idx == n - 1:
            i += 1
            continue

        prev_absent_idx = start_idx - 1
        next_absent_idx = end_idx + 1

        start_est = midpoint(snapshots[prev_absent_idx]["dt"], snapshots[start_idx]["dt"])
        end_est = midpoint(snapshots[end_idx]["dt"], snapshots[next_absent_idx]["dt"])
        duration = (end_est - start_est).total_seconds()

        sessions.append({
            "ip": target_ip,
            "start_idx": start_idx,
            "end_idx": end_idx,
            "first_seen": snapshots[start_idx]["ts_str"],
            "last_seen": snapshots[end_idx]["ts_str"],
            "start_est": start_est,
            "end_est": end_est,
            "duration_sec": duration,
        })

        i += 1

    downtimes = []
    for k in range(len(sessions) - 1):
        cur = sessions[k]
        nxt = sessions[k + 1]

        gap = (nxt["start_est"] - cur["end_est"]).total_seconds()
        downtimes.append({
            "ip": target_ip,
            "from_disconnect": cur["end_est"],
            "to_reconnect": nxt["start_est"],
            "duration_sec": gap,
            "prev_last_seen": cur["last_seen"],
            "next_first_seen": nxt["first_seen"],
        })

    return sessions, downtimes


def load_ip_list(ip_list_file: Path) -> List[str]:
    """
    支持：
    - 每行一个 IP
    - 一行多个 IP（逗号/空格/制表符分隔）
    - 忽略空行和 # 注释行
    - 自动去重并保持原顺序
    """
    seen = set()
    result = []

    with open(ip_list_file, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            ips = IP_PATTERN.findall(line)
            for ip in ips:
                if ip not in seen:
                    seen.add(ip)
                    result.append(ip)

    return result


def format_seconds(seconds: float) -> str:
    seconds = float(seconds)
    sign = "-" if seconds < 0 else ""
    seconds = abs(seconds)

    ms = int(round((seconds - int(seconds)) * 1000))
    total = int(seconds)

    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60

    if ms == 1000:
        s += 1
        ms = 0
    if s == 60:
        m += 1
        s = 0
    if m == 60:
        h += 1
        m = 0

    return f"{sign}{h:02d}:{m:02d}:{s:02d}.{ms:03d}"


def percentile(sorted_values: List[float], p: float) -> float:
    """
    线性插值分位数，p in [0, 1]
    """
    if not sorted_values:
        return float("nan")
    if len(sorted_values) == 1:
        return sorted_values[0]

    pos = (len(sorted_values) - 1) * p
    lo = math.floor(pos)
    hi = math.ceil(pos)

    if lo == hi:
        return sorted_values[lo]

    frac = pos - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def calc_stats(values: List[float]) -> Dict[str, float]:
    if not values:
        return {
            "count": 0,
            "total": 0.0,
            "mean": float("nan"),
            "median": float("nan"),
            "min": float("nan"),
            "max": float("nan"),
            "std": float("nan"),
            "p25": float("nan"),
            "p75": float("nan"),
        }

    sorted_values = sorted(values)
    count = len(values)
    total = sum(values)
    mean = statistics.mean(values)
    median = statistics.median(values)
    min_v = sorted_values[0]
    max_v = sorted_values[-1]
    std = statistics.pstdev(values) if count > 1 else 0.0
    p25 = percentile(sorted_values, 0.25)
    p75 = percentile(sorted_values, 0.75)

    return {
        "count": count,
        "total": total,
        "mean": mean,
        "median": median,
        "min": min_v,
        "max": max_v,
        "std": std,
        "p25": p25,
        "p75": p75,
    }


def write_stats_txt(output_path: Path, conn_stats: Dict[str, float], down_stats: Dict[str, float]):
    def block(name: str, s: Dict[str, float]) -> str:
        lines = [
            f"=== {name} ===",
            f"样本数: {s['count']}",
            f"总时长(秒): {s['total']:.6f}",
            f"总时长(格式化): {format_seconds(s['total'])}",
        ]
        if s["count"] > 0:
            lines.extend([
                f"平均值(秒): {s['mean']:.6f}",
                f"中位数(秒): {s['median']:.6f}",
                f"最小值(秒): {s['min']:.6f}",
                f"最大值(秒): {s['max']:.6f}",
                f"标准差(秒): {s['std']:.6f}",
                f"P25(秒): {s['p25']:.6f}",
                f"P75(秒): {s['p75']:.6f}",
            ])
        return "\n".join(lines)

    content = (
        block("连接持续时间统计", conn_stats)
        + "\n\n"
        + block("断连持续时间统计", down_stats)
        + "\n"
    )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)


def write_samples_csv(output_path: Path, rows: List[Dict], row_type: str):
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)

        if row_type == "connection":
            writer.writerow([
                "ip", "segment_index", "first_seen", "last_seen",
                "start_est", "end_est", "duration_sec"
            ])
            for idx, row in enumerate(rows, 1):
                writer.writerow([
                    row["ip"], idx, row["first_seen"], row["last_seen"],
                    row["start_est"].isoformat(sep=" "),
                    row["end_est"].isoformat(sep=" "),
                    f"{row['duration_sec']:.6f}",
                ])
        elif row_type == "downtime":
            writer.writerow([
                "ip", "segment_index", "prev_last_seen", "next_first_seen",
                "from_disconnect", "to_reconnect", "duration_sec"
            ])
            for idx, row in enumerate(rows, 1):
                writer.writerow([
                    row["ip"], idx, row["prev_last_seen"], row["next_first_seen"],
                    row["from_disconnect"].isoformat(sep=" "),
                    row["to_reconnect"].isoformat(sep=" "),
                    f"{row['duration_sec']:.6f}",
                ])
        else:
            raise ValueError(f"未知 row_type: {row_type}")


def write_per_ip_summary(output_path: Path, per_ip_summary: List[Dict]):
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "ip",
            "connection_segment_count",
            "connection_total_sec",
            "connection_mean_sec",
            "downtime_segment_count",
            "downtime_total_sec",
            "downtime_mean_sec",
        ])
        for row in per_ip_summary:
            writer.writerow([
                row["ip"],
                row["connection_segment_count"],
                f"{row['connection_total_sec']:.6f}",
                f"{row['connection_mean_sec']:.6f}" if row["connection_segment_count"] > 0 else "",
                row["downtime_segment_count"],
                f"{row['downtime_total_sec']:.6f}",
                f"{row['downtime_mean_sec']:.6f}" if row["downtime_segment_count"] > 0 else "",
            ])


def percentile(sorted_values, p):
    if not sorted_values:
        return float("nan")
    if len(sorted_values) == 1:
        return sorted_values[0]

    pos = (len(sorted_values) - 1) * p
    lo = math.floor(pos)
    hi = math.ceil(pos)

    if lo == hi:
        return sorted_values[lo]

    frac = pos - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def plot_distribution(
    values,
    xlabel,
    output_path,
    upper_percentile=0.95,
    max_x_seconds=None,
    unit="minute",
    bin_width=None,          # 新增：柱宽
    x_tick_step=None         # 新增：横轴刻度间隔
):
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["font.sans-serif"] = [
        "Noto Sans CJK SC", "SimHei", "Microsoft YaHei",
        "Arial Unicode MS", "DejaVu Sans"
    ]

    plt.figure(figsize=(12, 7))

    if not values:
        plt.text(0.5, 0.5, "无样本", ha="center", va="center", transform=plt.gca().transAxes)
        plt.xlabel(xlabel)
        plt.ylabel("样本数量（个）")
        plt.tight_layout()
        plt.savefig(output_path, dpi=200)
        plt.close()
        return

    sorted_values = sorted(values)
    percentile_cap = percentile(sorted_values, upper_percentile)
    upper_bound = min(percentile_cap, max_x_seconds) if max_x_seconds is not None else percentile_cap
    filtered_values = [v for v in values if v <= upper_bound]

    if not filtered_values:
        plt.text(0.5, 0.5, "无样本", ha="center", va="center", transform=plt.gca().transAxes)
        plt.xlabel(xlabel)
        plt.ylabel("样本数量（个）")
        plt.tight_layout()
        plt.savefig(output_path, dpi=200)
        plt.close()
        return

    mean_v = statistics.mean(filtered_values)
    median_v = statistics.median(filtered_values)

    if unit == "minute":
        plot_values = [v / 60.0 for v in filtered_values]
        mean_plot = mean_v / 60.0
        median_plot = median_v / 60.0
        mean_text = f"{mean_plot:.1f}分钟"
        median_text = f"{median_plot:.1f}分钟"
    else:
        plot_values = filtered_values
        mean_plot = mean_v
        median_plot = median_v
        mean_text = f"{mean_plot:.1f}秒"
        median_text = f"{median_plot:.1f}秒"

    x_max = max(plot_values)

    # 关键：更细的柱子
    if bin_width is not None and bin_width > 0:
        bins = np.arange(0, x_max + bin_width, bin_width)
        if len(bins) < 2:
            bins = 10
    else:
        bins = 40

    counts, _, _ = plt.hist(
        plot_values,
        bins=bins,
        edgecolor="black",
        linewidth=1.2
    )

    plt.axvline(mean_plot, linestyle="--", linewidth=2.5)
    plt.axvline(median_plot, linestyle="-.", linewidth=2.5)

    y_max = max(counts) if len(counts) > 0 else 1
    plt.text(mean_plot, y_max * 0.88, f"平均值\n{mean_text}", ha="left", va="center", fontsize=16)
    plt.text(median_plot, y_max * 0.70, f"中位数\n{median_text}", ha="left", va="center", fontsize=16)

    plt.xlabel(xlabel, fontsize=18)
    plt.ylabel("样本数量（个）", fontsize=18)
    plt.xlim(0, x_max * 1.05)

    # 关键：更细的横轴刻度
    if x_tick_step is not None and x_tick_step > 0:
        plt.xticks(np.arange(0, x_max * 1.05 + x_tick_step, x_tick_step), fontsize=13)
    else:
        plt.xticks(fontsize=13)

    plt.yticks(fontsize=13)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="批量分析 output_nodes.csv 中多个 IP 的连接持续时间与断连持续时间，并汇总统计与画图"
    )
    parser.add_argument("input_file", help="例如 output_nodes.csv")
    parser.add_argument("ip_list_file", help="IP 列表文件，支持一行一个或一行多个")
    parser.add_argument(
        "--output-dir",
        default="batch_analysis_output",
        help="输出目录，默认 batch_analysis_output"
    )
    args = parser.parse_args()

    input_path = Path(args.input_file)
    ip_list_path = Path(args.ip_list_file)
    output_dir = Path(args.output_dir)

    if not input_path.is_file():
        raise FileNotFoundError(f"输入文件不存在: {input_path}")
    if not ip_list_path.is_file():
        raise FileNotFoundError(f"IP 列表文件不存在: {ip_list_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    snapshots = load_snapshots(input_path)
    if not snapshots:
        raise RuntimeError("output_nodes.csv 中没有读取到有效快照数据")

    ip_list = load_ip_list(ip_list_path)
    if not ip_list:
        raise RuntimeError("IP 列表文件中没有读取到有效 IP")

    all_sessions: List[Dict] = []
    all_downtimes: List[Dict] = []
    per_ip_summary: List[Dict] = []

    print(f"[INFO] 快照数: {len(snapshots)}")
    print(f"[INFO] IP 数量: {len(ip_list)}")

    for ip in ip_list:
        sessions, downtimes = analyze_ip(snapshots, ip)

        conn_total = sum(x["duration_sec"] for x in sessions)
        down_total = sum(x["duration_sec"] for x in downtimes)

        per_ip_summary.append({
            "ip": ip,
            "connection_segment_count": len(sessions),
            "connection_total_sec": conn_total,
            "connection_mean_sec": (conn_total / len(sessions)) if sessions else 0.0,
            "downtime_segment_count": len(downtimes),
            "downtime_total_sec": down_total,
            "downtime_mean_sec": (down_total / len(downtimes)) if downtimes else 0.0,
        })

        all_sessions.extend(sessions)
        all_downtimes.extend(downtimes)

        print(
            f"[OK] {ip}: "
            f"连接段 {len(sessions)} 个, "
            f"断连段 {len(downtimes)} 个"
        )

    connection_values = [x["duration_sec"] for x in all_sessions]
    downtime_values = [x["duration_sec"] for x in all_downtimes]

    conn_stats = calc_stats(connection_values)
    down_stats = calc_stats(downtime_values)

    write_samples_csv(output_dir / "all_connection_samples.csv", all_sessions, "connection")
    write_samples_csv(output_dir / "all_downtime_samples.csv", all_downtimes, "downtime")
    write_per_ip_summary(output_dir / "per_ip_summary.csv", per_ip_summary)
    write_stats_txt(output_dir / "summary_stats.txt", conn_stats, down_stats)

    plot_distribution(
        connection_values,
        xlabel="连接持续时间",
        output_path=output_dir / "connection_duration_distribution.png",
    )
    plot_distribution(
        downtime_values,
        xlabel="断连持续时间",
        output_path=output_dir / "downtime_duration_distribution.png",
    )

    print()
    print("[DONE] 输出完成")
    print(f"[OUT] {output_dir / 'all_connection_samples.csv'}")
    print(f"[OUT] {output_dir / 'all_downtime_samples.csv'}")
    print(f"[OUT] {output_dir / 'per_ip_summary.csv'}")
    print(f"[OUT] {output_dir / 'summary_stats.txt'}")
    print(f"[OUT] {output_dir / 'connection_duration_distribution.png'}")
    print(f"[OUT] {output_dir / 'downtime_duration_distribution.png'}")


if __name__ == "__main__":
    main()