import os
import sys
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def load_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    required_cols = {"targetId", "totalPeers", "respondedPeers", "hitPeers"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"CSV 缺少必要列: {sorted(missing)}")

    df["totalPeers"] = pd.to_numeric(df["totalPeers"], errors="coerce")
    df["respondedPeers"] = pd.to_numeric(df["respondedPeers"], errors="coerce")
    df["hitPeers"] = pd.to_numeric(df["hitPeers"], errors="coerce")

    df = df.dropna(subset=["totalPeers", "respondedPeers", "hitPeers"]).copy()

    # 重新计算数值型 hit_rate，避免字符串百分比带来的排序/分析问题
    df["hit_rate_num"] = np.where(
        df["respondedPeers"] > 0,
        df["hitPeers"] / df["respondedPeers"] * 100.0,
        np.nan
    )

    return df


def ensure_outdir(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)


def save_summary(df: pd.DataFrame, out_dir: str) -> None:
    summary_path = os.path.join(out_dir, "summary.txt")

    s = df["hit_rate_num"].dropna()
    h = df["hitPeers"].dropna()
    r = df["respondedPeers"].dropna()

    lines = [
        f"样本数: {len(df)}",
        f"totalPeers 唯一值: {sorted(df['totalPeers'].dropna().unique().tolist())}",
        "",
        "[hitPeers]",
        f"均值: {h.mean():.4f}",
        f"中位数: {h.median():.4f}",
        f"最小值: {h.min():.4f}",
        f"最大值: {h.max():.4f}",
        f"标准差: {h.std(ddof=1):.4f}" if len(h) > 1 else "标准差: NA",
        "",
        "[respondedPeers]",
        f"均值: {r.mean():.4f}",
        f"中位数: {r.median():.4f}",
        f"最小值: {r.min():.4f}",
        f"最大值: {r.max():.4f}",
        f"标准差: {r.std(ddof=1):.4f}" if len(r) > 1 else "标准差: NA",
        "",
        "[hit_rate_num (%)]",
        f"均值: {s.mean():.4f}",
        f"中位数: {s.median():.4f}",
        f"最小值: {s.min():.4f}",
        f"最大值: {s.max():.4f}",
        f"Q1: {s.quantile(0.25):.4f}",
        f"Q3: {s.quantile(0.75):.4f}",
        f"标准差: {s.std(ddof=1):.4f}" if len(s) > 1 else "标准差: NA",
    ]

    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def plot_hist_hit_peers(df: pd.DataFrame, out_dir: str) -> None:
    data = df["hitPeers"].dropna()
    if len(data) == 0:
        return

    bin_width = 20
    data_min = data.min()
    data_max = data.max()

    left = math.floor(data_min / bin_width) * bin_width
    right = math.ceil(data_max / bin_width) * bin_width + bin_width
    bins = np.arange(left, right + bin_width, bin_width)

    plt.figure(figsize=(8, 5))
    plt.hist(data, bins=bins)
    plt.xlabel("hitPeers")
    plt.ylabel("Count")
    plt.title("Distribution of hitPeers")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "hist_hitPeers.png"), dpi=200)
    plt.close()


def plot_hist_hit_rate(df: pd.DataFrame, out_dir: str) -> None:
    data = df["hit_rate_num"].dropna()
    if len(data) == 0:
        return

    bin_width = 20
    data_min = data.min()
    data_max = data.max()

    left = math.floor(data_min / bin_width) * bin_width
    right = math.ceil(data_max / bin_width) * bin_width + bin_width
    bins = np.arange(left, right + bin_width, bin_width)

    plt.figure(figsize=(8, 5))
    plt.hist(data, bins=bins)
    plt.xlabel("hit rate (%)")
    plt.ylabel("Count")
    plt.title("Distribution of hit rate")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "hist_hit_rate.png"), dpi=200)
    plt.close()


def plot_ecdf_hit_rate(df: pd.DataFrame, out_dir: str) -> None:
    data = np.sort(df["hit_rate_num"].dropna().to_numpy())
    if len(data) == 0:
        return
    y = np.arange(1, len(data) + 1) / len(data)

    plt.figure(figsize=(8, 5))
    plt.plot(data, y)
    plt.xlabel("hit rate (%)")
    plt.ylabel("ECDF")
    plt.title("ECDF of hit rate")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "ecdf_hit_rate.png"), dpi=200)
    plt.close()


def plot_scatter_responded_vs_hit(df: pd.DataFrame, out_dir: str) -> None:
    plt.figure(figsize=(8, 5))
    plt.scatter(df["respondedPeers"], df["hitPeers"])
    plt.xlabel("respondedPeers")
    plt.ylabel("hitPeers")
    plt.title("respondedPeers vs hitPeers")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "scatter_responded_vs_hit.png"), dpi=200)
    plt.close()


def plot_box_hit_rate(df: pd.DataFrame, out_dir: str) -> None:
    data = df["hit_rate_num"].dropna()
    if len(data) == 0:
        return
    plt.figure(figsize=(6, 5))
    plt.boxplot(data, vert=True)
    plt.ylabel("hit rate (%)")
    plt.title("Boxplot of hit rate")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "box_hit_rate.png"), dpi=200)
    plt.close()

def plot_ecdf_hit_peers(df: pd.DataFrame, out_dir: str) -> None:
    data = np.sort(df["hitPeers"].dropna().to_numpy())
    if len(data) == 0:
        return

    y = np.arange(1, len(data) + 1) / len(data)

    # 90% 分位点
    p = 0.90
    x_p = np.quantile(data, p)

    # 指定参考点
    x_ref = 82
    y_ref = np.searchsorted(data, x_ref, side="right") / len(data)

    plt.figure(figsize=(8, 5))
    plt.plot(data, y, label="ECDF")

    # 90% 参考线
    plt.axhline(p, linestyle="--")
    plt.axvline(x_p, linestyle="--")
    plt.scatter([x_p], [p])
    plt.annotate(
        f"90%: {x_p:.2f}",
        xy=(x_p, p),
        xytext=(10, -15),
        textcoords="offset points"
    )

    # x=82 的分界线和点
    plt.axvline(x_ref, linestyle="--")
    plt.scatter([x_ref], [y_ref])
    plt.annotate(
        f"{y_ref:.2%}: {x_ref:.2f}",
        xy=(x_ref, y_ref),
        xytext=(10, 10),
        textcoords="offset points"
    )

    plt.xlabel("hitPeers")
    plt.ylabel("ECDF")
    plt.title("ECDF of hitPeers")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "ecdf_hitPeers.png"), dpi=200)
    plt.close()

def main():
    if len(sys.argv) < 2:
        print("用法: python analyze_hit_peers.py input.csv [output_dir]")
        sys.exit(1)

    csv_path = sys.argv[1]
    out_dir = sys.argv[2] if len(sys.argv) >= 3 else "./hit_peers_analysis"

    ensure_outdir(out_dir)
    df = load_data(csv_path)

    # 保存清洗后的数据
    df.to_csv(os.path.join(out_dir, "cleaned_data.csv"), index=False, encoding="utf-8-sig")

    save_summary(df, out_dir)
    plot_hist_hit_peers(df, out_dir)
    plot_hist_hit_rate(df, out_dir)
    plot_ecdf_hit_rate(df, out_dir)
    plot_scatter_responded_vs_hit(df, out_dir)
    plot_box_hit_rate(df, out_dir)
    plot_ecdf_hit_peers(df, out_dir)

    print(f"分析完成，输出目录: {out_dir}")


if __name__ == "__main__":
    main()