#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
高性能版本：解析本地与远程 java-tron stdout 日志，按交易哈希聚合输出。

相比上一版的主要增强：
1. 解析阶段仍按 tx_hash 前缀分桶落盘（bucket）。
2. 排序+导出改成“按 bucket 顺序逐个处理”。
3. 每个 bucket 导出成功后，立即删除该 bucket 的 parsed/sorted 中间产物，自动回收空间。
4. 为了支持重启恢复，先为每个 bucket 生成 bucket_blocks 文件，再统一打包成最终 part 文件。
5. parse 阶段支持文件级断点续跑；bucket 导出阶段支持按 bucket 断点续跑。

目录结构：
  workdir/
    state/
      local/progress.json
      remote/progress.json
      bucket_done/bucket_xx.done
      gc_done/bucket_xx.done
    parsed/local/*.tsv
    parsed/remote/*.tsv
    sorted/bucket_xx.sorted.tsv
    bucket_blocks/bucket_xx.blocks.txt
    tmp/...

注意：
- 自动 GC 只能在某个 bucket 已成功导出为 bucket_blocks 之后进行。
- 如果 parse 阶段本身就会把磁盘打满，这个版本也无能为力；此时必须把 workdir 放到更大的盘。
- 最终输出 part 文件在 pack 阶段从 bucket_blocks 重建；因此 pack 阶段失败后可直接重跑，无需 reset。
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding=encoding) as f:
        f.write(text)
    os.replace(tmp, path)


def human_size(num: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(num)
    for u in units:
        if n < 1024 or u == units[-1]:
            return f"{n:.2f}{u}"
        n /= 1024
    return f"{num}B"


def path_size(path: Path) -> int:
    try:
        if path.is_file():
            return path.stat().st_size
        if path.is_dir():
            total = 0
            for p in path.rglob("*"):
                try:
                    if p.is_file():
                        total += p.stat().st_size
                except FileNotFoundError:
                    pass
            return total
    except FileNotFoundError:
        return 0
    return 0


def disk_free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


@dataclass(frozen=True)
class FileMeta:
    source: str
    path: Path
    date_str: str
    order_idx: int
    is_current: bool
    size: int
    mtime_ns: int
    file_key: str


def make_file_key(path: Path, size: int, mtime_ns: int) -> str:
    raw = f"{path.resolve()}|{size}|{mtime_ns}".encode("utf-8", errors="replace")
    return hashlib.sha1(raw).hexdigest()[:24]


def safe_stem(path: Path) -> str:
    name = path.name.replace(os.sep, "_")
    return name.replace(" ", "_")


# -----------------------------
# 文件发现与排序
# -----------------------------

def get_file_date_and_order(path: Path) -> Tuple[Optional[str], Optional[int], Optional[bool]]:
    name = path.name

    if name.startswith("stdout-") and ".log" in name:
        try:
            rest = name[len("stdout-"):]
            first_dot = rest.find(".")
            if first_dot == -1:
                return None, None, None
            date_str = rest[:first_dot]
            remain = rest[first_dot + 1:]
            second_dot = remain.find(".")
            if second_dot == -1:
                return None, None, None
            order_idx = int(remain[:second_dot])
            if len(date_str) != 10 or date_str[4] != "-" or date_str[7] != "-":
                return None, None, None
            return date_str, order_idx, False
        except Exception:
            return None, None, None

    if name == "stdout.log" or name == "stdout.log.gz":
        dt = time.localtime(path.stat().st_mtime)
        date_str = time.strftime("%Y-%m-%d", dt)
        return date_str, 10**9, True

    return None, None, None


def discover_log_files(folder: Path, source: str) -> List[FileMeta]:
    metas: List[FileMeta] = []
    for p in folder.iterdir():
        if not p.is_file():
            continue
        date_str, order_idx, is_current = get_file_date_and_order(p)
        if date_str is None:
            continue
        st = p.stat()
        metas.append(
            FileMeta(
                source=source,
                path=p,
                date_str=date_str,
                order_idx=order_idx,
                is_current=is_current,
                size=st.st_size,
                mtime_ns=st.st_mtime_ns,
                file_key=make_file_key(p, st.st_size, st.st_mtime_ns),
            )
        )
    metas.sort(key=lambda m: (m.date_str, m.order_idx, m.path.name))
    return metas


# -----------------------------
# 快速解析辅助
# -----------------------------

@lru_cache(maxsize=65536)
def is_ipv4(text: str) -> bool:
    parts = text.split('.')
    if len(parts) != 4:
        return False
    for p in parts:
        if not p or not p.isdigit():
            return False
        try:
            v = int(p)
        except ValueError:
            return False
        if v < 0 or v > 255:
            return False
    return True


_HEX_CHARS = set("0123456789abcdefABCDEF")


@lru_cache(maxsize=65536)
def is_hex_hash(text: str) -> bool:
    n = len(text)
    if n < 32 or n > 128:
        return False
    for ch in text:
        if ch not in _HEX_CHARS:
            return False
    return True


@lru_cache(maxsize=65536)
def bucket_of_tx(tx_hash: str, bucket_digits: int) -> str:
    return tx_hash[:bucket_digits].lower()


def open_text_maybe_gz(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "rt", encoding="utf-8", errors="replace")


# -----------------------------
# 断点状态
# -----------------------------

def load_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, payload: dict) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, sort_keys=False))


def validate_resume_prefix(current_files: List[FileMeta], completed_keys: List[str], source: str) -> int:
    if len(completed_keys) > len(current_files):
        raise RuntimeError(f"[{source}] 当前文件数量少于已完成前缀，无法安全续跑")
    current_prefix = [m.file_key for m in current_files[:len(completed_keys)]]
    if current_prefix != completed_keys:
        raise RuntimeError(
            f"[{source}] 已完成文件前缀与当前目录内容不一致，无法安全续跑。\n"
            f"请检查日志文件是否被替换/删除/重命名，或使用 --reset 重新开始。"
        )
    return len(completed_keys)


# -----------------------------
# parse 阶段
# -----------------------------

def parse_one_source(
    source: str,
    folder: str,
    workdir: str,
    bucket_digits: int,
    flush_every: int,
    progress_lines: int,
    resume: bool,
) -> dict:
    t0 = time.time()
    folder_p = Path(folder)
    workdir_p = Path(workdir)

    files = discover_log_files(folder_p, source)
    if not files:
        eprint(f"[WARN] [{source}] 未发现可解析日志: {folder_p}")
        return {"source": source, "files": 0, "matched": 0, "unmatched": 0, "inv": 0}

    eprint(f"[INFO] [{source}] 开始处理目录: {folder_p}")
    eprint(f"[INFO] [{source}] 共发现 {len(files)} 个日志文件")

    state_dir = workdir_p / "state" / source
    parsed_dir = workdir_p / "parsed" / source
    tmp_dir = workdir_p / "tmp" / source
    state_dir.mkdir(parents=True, exist_ok=True)
    parsed_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    progress_path = state_dir / "progress.json"

    completed_keys: List[str] = []
    last_inv_by_peer: Dict[str, int] = {}
    start_idx = 0

    if resume and progress_path.exists():
        progress = load_json(progress_path)
        if progress is not None:
            completed_keys = list(progress.get("completed_keys", []))
            start_idx = validate_resume_prefix(files, completed_keys, source)
            last_inv_by_peer = {k: int(v) for k, v in progress.get("last_inv_by_peer", {}).items()}
            eprint(f"[INFO] [{source}] 从断点续跑，已完成文件数={start_idx}")
    else:
        eprint(f"[INFO] [{source}] 从头开始处理")

    total_stats = {
        "files": len(files),
        "processed_files": start_idx,
        "inv": 0,
        "matched": 0,
        "unmatched": 0,
        "ignored_iplist": 0,
        "ignored_other": 0,
        "bad": 0,
        "lines": 0,
    }

    for idx in range(start_idx, len(files)):
        meta = files[idx]
        file_start = time.time()
        eprint(f"[INFO] [{source}] 开始处理文件 {idx + 1}/{len(files)}: {meta.path.name}")
        if meta.is_current:
            eprint(f"[INFO] [{source}] 当前文件使用 mtime 推断日期: {meta.path.name} -> {meta.date_str}")

        file_stats = {
            "lines": 0,
            "inv": 0,
            "matched": 0,
            "unmatched": 0,
            "ignored_iplist": 0,
            "ignored_other": 0,
            "bad": 0,
        }

        file_tag = f"{idx:06d}__{meta.file_key}__{safe_stem(meta.path)}"
        local_tmp_dir = tmp_dir / file_tag
        local_tmp_dir.mkdir(parents=True, exist_ok=True)

        handles: Dict[str, object] = {}
        buffers: Dict[str, List[str]] = {}

        def flush_bucket(bucket: str) -> None:
            buf = buffers.get(bucket)
            if not buf:
                return
            fp = handles[bucket]
            fp.write(''.join(buf))
            buf.clear()

        def write_record(tx_hash: str, full_ts: str, peer_ip: str, inv_size: int) -> None:
            bucket = bucket_of_tx(tx_hash, bucket_digits)
            if bucket not in handles:
                tmp_file = local_tmp_dir / f"{file_tag}.{bucket}.tsv.tmp"
                handles[bucket] = open(tmp_file, "w", encoding="utf-8", newline="")
                buffers[bucket] = []
            buffers[bucket].append(f"{tx_hash}\t{full_ts}\t{peer_ip}\t{inv_size}\n")
            if len(buffers[bucket]) >= flush_every:
                flush_bucket(bucket)

        try:
            with open_text_maybe_gz(meta.path) as f:
                for raw in f:
                    file_stats["lines"] += 1
                    total_stats["lines"] += 1

                    line = raw.rstrip("\n")
                    sp = line.find(' ')
                    if sp <= 0:
                        file_stats["bad"] += 1
                        total_stats["bad"] += 1
                        continue

                    time_str = line[:sp]
                    if len(time_str) != 12 or time_str[2] != ':' or time_str[5] != ':' or time_str[8] != '.':
                        file_stats["bad"] += 1
                        total_stats["bad"] += 1
                        continue

                    rest = line[sp + 1:].strip()
                    if not rest:
                        file_stats["bad"] += 1
                        total_stats["bad"] += 1
                        continue

                    if rest.startswith("INV "):
                        parts = rest.split()
                        if len(parts) >= 3 and parts[0] == "INV" and is_ipv4(parts[1]) and parts[2].isdigit():
                            last_inv_by_peer[parts[1]] = int(parts[2])
                            file_stats["inv"] += 1
                            total_stats["inv"] += 1
                        else:
                            file_stats["ignored_other"] += 1
                            total_stats["ignored_other"] += 1
                        continue

                    first_space = rest.find(' ')
                    first_token = rest if first_space == -1 else rest[:first_space]
                    if first_token and first_token[0].isdigit():
                        if ',' in first_token:
                            file_stats["ignored_iplist"] += 1
                            total_stats["ignored_iplist"] += 1
                            continue

                        parts = rest.split()
                        if len(parts) >= 2 and is_ipv4(parts[0]) and is_hex_hash(parts[1]):
                            peer_ip = parts[0]
                            tx_hash = parts[1].lower()
                            inv_size = last_inv_by_peer.get(peer_ip)
                            if inv_size is None:
                                file_stats["unmatched"] += 1
                                total_stats["unmatched"] += 1
                            else:
                                full_ts = f"{meta.date_str} {time_str}"
                                write_record(tx_hash, full_ts, peer_ip, inv_size)
                                file_stats["matched"] += 1
                                total_stats["matched"] += 1
                        else:
                            file_stats["ignored_other"] += 1
                            total_stats["ignored_other"] += 1
                    else:
                        file_stats["ignored_other"] += 1
                        total_stats["ignored_other"] += 1

                    if progress_lines > 0 and file_stats["lines"] % progress_lines == 0:
                        eprint(
                            f"[INFO] [{source}] 文件进行中: {meta.path.name} | "
                            f"lines={file_stats['lines']} | INV={file_stats['inv']} | "
                            f"TX匹配={file_stats['matched']} | TX未匹配={file_stats['unmatched']}"
                        )

            for b in list(buffers.keys()):
                flush_bucket(b)
            for fp in handles.values():
                fp.close()

            created_shards = 0
            for tmp_file in sorted(local_tmp_dir.glob("*.tsv.tmp")):
                final_name = tmp_file.name[:-4]
                final_path = parsed_dir / final_name
                os.replace(tmp_file, final_path)
                created_shards += 1

            shutil.rmtree(local_tmp_dir, ignore_errors=True)

            completed_keys.append(meta.file_key)
            progress = {
                "version": 1,
                "source": source,
                "folder": str(folder_p.resolve()),
                "completed_keys": completed_keys,
                "last_inv_by_peer": last_inv_by_peer,
                "updated_at": time.time(),
            }
            save_json(progress_path, progress)

            total_stats["processed_files"] += 1
            elapsed = time.time() - file_start
            eprint(
                f"[INFO] [{source}] 文件完成 {idx + 1}/{len(files)}: {meta.path.name} | "
                f"耗时={elapsed:.2f}s | shards={created_shards} | 行数={file_stats['lines']} | "
                f"INV={file_stats['inv']} | TX匹配={file_stats['matched']} | TX未匹配={file_stats['unmatched']} | "
                f"忽略IP列表={file_stats['ignored_iplist']} | 其他忽略={file_stats['ignored_other']} | 坏行={file_stats['bad']}"
            )

        except Exception:
            try:
                for fp in handles.values():
                    fp.close()
            except Exception:
                pass
            shutil.rmtree(local_tmp_dir, ignore_errors=True)
            raise

    elapsed_all = time.time() - t0
    eprint(
        f"[INFO] [{source}] 全部完成 | 文件数={total_stats['files']} | 已处理到={total_stats['processed_files']} | "
        f"总行数={total_stats['lines']} | INV={total_stats['inv']} | TX匹配={total_stats['matched']} | "
        f"TX未匹配={total_stats['unmatched']} | 忽略IP列表={total_stats['ignored_iplist']} | "
        f"其他忽略={total_stats['ignored_other']} | 坏行={total_stats['bad']} | 总耗时={elapsed_all:.2f}s"
    )
    return {"source": source, **total_stats, "elapsed": elapsed_all}


# -----------------------------
# bucket 工具
# -----------------------------

def iter_bucket_names(bucket_digits: int) -> Iterable[str]:
    total = 16 ** bucket_digits
    for i in range(total):
        yield f"{i:0{bucket_digits}x}"


def list_bucket_shards(workdir: Path, bucket: str) -> List[Path]:
    files: List[Path] = []
    for source in ("local", "remote"):
        p = workdir / "parsed" / source
        if p.exists():
            files.extend(sorted(p.glob(f"*.{bucket}.tsv")))
    return files


def bucket_done_marker(workdir: Path, bucket: str) -> Path:
    return workdir / "state" / "bucket_done" / f"bucket_{bucket}.done"


def gc_done_marker(workdir: Path, bucket: str) -> Path:
    return workdir / "state" / "gc_done" / f"bucket_{bucket}.done"


def bucket_block_path(workdir: Path, bucket: str) -> Path:
    return workdir / "bucket_blocks" / f"bucket_{bucket}.blocks.txt"


def sorted_bucket_path(workdir: Path, bucket: str) -> Path:
    return workdir / "sorted" / f"bucket_{bucket}.sorted.tsv"


def needs_resort(sorted_path: Path, inputs: List[Path]) -> bool:
    if not sorted_path.exists():
        return True
    out_mtime = sorted_path.stat().st_mtime_ns
    for p in inputs:
        if p.stat().st_mtime_ns > out_mtime:
            return True
    return False


def sort_one_bucket(bucket: str, workdir: Path, sort_mem: str, sort_parallel: int, force_sort: bool) -> Tuple[int, str]:
    out_path = sorted_bucket_path(workdir, bucket)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    inputs = list_bucket_shards(workdir, bucket)
    if not inputs:
        return 0, "empty"

    if (not force_sort) and (not needs_resort(out_path, inputs)):
        return len(inputs), "skip"

    tmp_sort_dir = workdir / "tmp_sort"
    tmp_sort_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "sort",
        "-t", "\t",
        "-k1,1",
        "-k2,2",
        "--stable",
        "--parallel", str(max(1, sort_parallel)),
        "-S", sort_mem,
        "-T", str(tmp_sort_dir),
        "-o", str(out_path),
        *[str(p) for p in inputs],
    ]
    env = os.environ.copy()
    env["LC_ALL"] = "C"
    subprocess.run(cmd, check=True, env=env)
    return len(inputs), "sorted"


def export_one_bucket(workdir: Path, bucket: str) -> Tuple[int, int, int]:
    """
    从 sorted/bucket_xx.sorted.tsv 生成 bucket_blocks/bucket_xx.blocks.txt
    返回: (blocks, rows, bytes)
    """
    sorted_path = sorted_bucket_path(workdir, bucket)
    out_path = bucket_block_path(workdir, bucket)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not sorted_path.exists():
        if out_path.exists():
            return 0, 0, out_path.stat().st_size
        return 0, 0, 0

    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    blocks = 0
    rows = 0
    current_tx: Optional[str] = None
    current_lines: List[str] = []

    with open(sorted_path, "r", encoding="utf-8", errors="replace") as fin, \
         open(tmp_path, "w", encoding="utf-8", newline="") as fout:
        for raw in fin:
            rows += 1
            parts = raw.rstrip("\n").split("\t")
            if len(parts) != 4:
                continue
            tx_hash, full_ts, peer_ip, inv_size = parts

            if current_tx is None:
                current_tx = tx_hash

            if tx_hash != current_tx:
                fout.write(current_tx + "\n")
                fout.write(''.join(current_lines))
                fout.write(".\n")
                blocks += 1
                current_tx = tx_hash
                current_lines = []

            current_lines.append(f"{full_ts} {peer_ip} {inv_size}\n")

        if current_tx is not None:
            fout.write(current_tx + "\n")
            fout.write(''.join(current_lines))
            fout.write(".\n")
            blocks += 1

    os.replace(tmp_path, out_path)
    return blocks, rows, out_path.stat().st_size


def mark_bucket_done(workdir: Path, bucket: str, payload: dict) -> None:
    marker = bucket_done_marker(workdir, bucket)
    marker.parent.mkdir(parents=True, exist_ok=True)
    save_json(marker, payload)


def cleanup_bucket_artifacts(workdir: Path, bucket: str) -> int:
    """删除已导出 bucket 的 parsed/sorted 中间文件。返回释放字节数。"""
    freed = 0
    patterns = [
        (workdir / "parsed" / "local", f"*.{bucket}.tsv"),
        (workdir / "parsed" / "remote", f"*.{bucket}.tsv"),
        (workdir / "sorted", f"bucket_{bucket}.sorted.tsv"),
    ]
    for base, pat in patterns:
        if not base.exists():
            continue
        for p in base.glob(pat):
            try:
                size = p.stat().st_size
            except FileNotFoundError:
                size = 0
            try:
                p.unlink()
                freed += size
            except FileNotFoundError:
                pass
    save_json(gc_done_marker(workdir, bucket), {"bucket": bucket, "freed": freed, "updated_at": time.time()})
    return freed


def process_buckets_with_gc(
    workdir: Path,
    bucket_digits: int,
    sort_mem: str,
    sort_parallel: int,
    force_sort: bool,
    min_free_gb_warn: float,
) -> Tuple[int, int, int]:
    """
    对每个 bucket：sort -> export bucket_blocks -> mark done -> gc parsed/sorted
    返回: (done_buckets, total_blocks, total_rows)
    """
    done_buckets = 0
    total_blocks = 0
    total_rows = 0

    for idx, bucket in enumerate(iter_bucket_names(bucket_digits), start=1):
        marker = bucket_done_marker(workdir, bucket)
        if marker.exists() and bucket_block_path(workdir, bucket).exists():
            meta = load_json(marker) or {}
            done_buckets += 1
            total_blocks += int(meta.get("blocks", 0))
            total_rows += int(meta.get("rows", 0))
            # 补做 GC（如果上次导出后尚未清理）
            if not gc_done_marker(workdir, bucket).exists():
                freed = cleanup_bucket_artifacts(workdir, bucket)
                eprint(f"[INFO] [gc] bucket={bucket} | 补做GC | freed={human_size(freed)}")
            eprint(f"[INFO] [bucket] skip done bucket={bucket} | progress={idx}/{16 ** bucket_digits}")
            continue

        free_now = disk_free_bytes(workdir)
        if min_free_gb_warn > 0 and free_now < int(min_free_gb_warn * 1024 ** 3):
            eprint(f"[WARN] [disk] 当前剩余空间较低: {human_size(free_now)} | workdir={workdir}")

        inputs = list_bucket_shards(workdir, bucket)
        if not inputs:
            eprint(f"[INFO] [bucket] bucket={bucket} 无输入 | progress={idx}/{16 ** bucket_digits}")
            continue

        eprint(f"[INFO] [bucket] 开始 bucket={bucket} | inputs={len(inputs)} | progress={idx}/{16 ** bucket_digits}")

        sort_inputs, sort_status = sort_one_bucket(bucket, workdir, sort_mem, sort_parallel, force_sort)
        eprint(f"[INFO] [sort] bucket={bucket} | inputs={sort_inputs} | status={sort_status}")

        blocks, rows, out_bytes = export_one_bucket(workdir, bucket)
        total_blocks += blocks
        total_rows += rows

        mark_bucket_done(workdir, bucket, {
            "bucket": bucket,
            "inputs": len(inputs),
            "blocks": blocks,
            "rows": rows,
            "out_bytes": out_bytes,
            "updated_at": time.time(),
        })

        freed = cleanup_bucket_artifacts(workdir, bucket)
        done_buckets += 1
        eprint(
            f"[INFO] [bucket] 完成 bucket={bucket} | blocks={blocks} | rows={rows} | "
            f"bucket_blocks={human_size(out_bytes)} | gc_freed={human_size(freed)}"
        )

    return done_buckets, total_blocks, total_rows


# -----------------------------
# 最终 pack 阶段
# -----------------------------

class SplitWriter:
    def __init__(self, output_dir: Path, limit_bytes: int, prefix: str):
        self.output_dir = output_dir
        self.limit_bytes = limit_bytes
        self.prefix = prefix
        self.part_no = 0
        self.fp = None
        self.cur_path: Optional[Path] = None
        self.cur_size = 0
        self.generated: List[Path] = []

    def _open_new(self):
        self.part_no += 1
        self.cur_path = self.output_dir / f"{self.prefix}.part{self.part_no:04d}.txt"
        self.fp = open(self.cur_path, "w", encoding="utf-8", newline="")
        self.cur_size = 0
        self.generated.append(self.cur_path)

    def write_text(self, text: str):
        data = text.encode("utf-8")
        if self.fp is None:
            self._open_new()
        if self.cur_size > 0 and self.cur_size + len(data) > self.limit_bytes:
            self.fp.close()
            self._open_new()
        self.fp.write(text)
        self.cur_size += len(data)

    def close(self):
        if self.fp is not None:
            self.fp.close()
            self.fp = None


def pack_bucket_blocks_to_parts(
    workdir: Path,
    output_dir: Path,
    bucket_digits: int,
    part_size_mb: int,
    prefix: str,
    cleanup_bucket_blocks_after_pack: bool,
) -> List[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    for old in output_dir.glob(f"{prefix}.part*.txt"):
        old.unlink()

    writer = SplitWriter(output_dir, part_size_mb * 1024 * 1024, prefix)
    total_bytes_in = 0

    for bucket in iter_bucket_names(bucket_digits):
        block_file = bucket_block_path(workdir, bucket)
        if not block_file.exists():
            continue
        eprint(f"[INFO] [pack] 打包 bucket={bucket}: {block_file.name}")
        with open(block_file, "r", encoding="utf-8", errors="replace") as f:
            while True:
                chunk = f.read(8 * 1024 * 1024)
                if not chunk:
                    break
                writer.write_text(chunk)
                total_bytes_in += len(chunk.encode("utf-8", errors="replace"))
        if cleanup_bucket_blocks_after_pack:
            try:
                size = block_file.stat().st_size
            except FileNotFoundError:
                size = 0
            try:
                block_file.unlink()
                eprint(f"[INFO] [pack] 已删除 bucket_blocks: {block_file} ({size} bytes)")
            except FileNotFoundError:
                pass

    writer.close()
    pack_state = {
        "updated_at": time.time(),
        "parts": [str(p) for p in writer.generated],
        "part_count": len(writer.generated),
        "total_bytes_in": total_bytes_in,
        "cleanup_bucket_blocks_after_pack": cleanup_bucket_blocks_after_pack,
    }
    save_json(workdir / "state" / "pack_done.json", pack_state)

    eprint(f"[INFO] [pack] 打包完成 | parts={len(writer.generated)} | total_bytes_in={total_bytes_in}")
    for p in writer.generated:
        eprint(f"[INFO] [pack] 输出文件: {p} ({p.stat().st_size} bytes)")
    return writer.generated


# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    cpu = os.cpu_count() or 4
    parser = argparse.ArgumentParser(
        description="高性能合并 java-tron 本地/远程 stdout 日志，按交易哈希输出块文本（内建按 bucket 自动 GC）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--local-dir", required=True, help="本地日志目录")
    parser.add_argument("--remote-dir", required=True, help="远程同步下来的日志目录")
    parser.add_argument("--workdir", required=True, help="中间工作目录（分桶文件、状态文件、排序结果）")
    parser.add_argument("--output-dir", required=True, help="最终输出目录")
    parser.add_argument("--part-size-mb", type=int, default=100, help="最终输出切分大小")
    parser.add_argument("--bucket-digits", type=int, default=2, choices=[1, 2, 3], help="按 tx_hash 前几位分桶；2=256 桶")
    parser.add_argument("--flush-every", type=int, default=5000, help="每个 bucket 缓冲多少行后落盘")
    parser.add_argument("--progress-lines", type=int, default=200000, help="单文件内每处理多少行输出一次进度；0 表示关闭")
    parser.add_argument("--sort-parallel", type=int, default=max(1, min(4, cpu // 2 or 1)), help="传给单个 sort 命令的 --parallel")
    parser.add_argument("--sort-mem", default="1G", help="传给 sort 的 -S 内存参数，例如 512M / 1G / 2G")
    parser.add_argument("--force-sort", action="store_true", help="即便 sorted 文件已存在且较新，也强制重新排序")
    parser.add_argument("--reset", action="store_true", help="删除 workdir 中的中间结果和断点状态，从头开始")
    parser.add_argument("--no-resume", action="store_true", help="不读取断点，直接从头处理 source 解析阶段")
    parser.add_argument("--output-prefix", default="merged_tx_blocks", help="输出文件名前缀")
    parser.add_argument("--min-free-gb-warn", type=float, default=10.0, help="剩余空间低于该值时输出警告；0 表示关闭")
    parser.add_argument("--cleanup-bucket-blocks-after-pack", action="store_true", help="最终 part 文件打包成功后，删除 bucket_blocks 中间文件")
    return parser.parse_args()


def maybe_reset_workdir(workdir: Path, do_reset: bool) -> None:
    if do_reset and workdir.exists():
        eprint(f"[INFO] 删除旧 workdir: {workdir}")
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True, exist_ok=True)


def main() -> int:
    args = parse_args()

    local_dir = Path(args.local_dir)
    remote_dir = Path(args.remote_dir)
    workdir = Path(args.workdir)
    output_dir = Path(args.output_dir)

    if not local_dir.is_dir():
        raise SystemExit(f"本地日志目录不存在: {local_dir}")
    if not remote_dir.is_dir():
        raise SystemExit(f"远程日志目录不存在: {remote_dir}")

    maybe_reset_workdir(workdir, args.reset)
    output_dir.mkdir(parents=True, exist_ok=True)

    overall_start = time.time()

    # 1) parse 阶段：local / remote 两个 source 并行，各自内部严格顺序。
    parse_start = time.time()
    with cf.ProcessPoolExecutor(max_workers=2) as ex:
        futs = [
            ex.submit(
                parse_one_source,
                "local",
                str(local_dir),
                str(workdir),
                args.bucket_digits,
                args.flush_every,
                args.progress_lines,
                not args.no_resume,
            ),
            ex.submit(
                parse_one_source,
                "remote",
                str(remote_dir),
                str(workdir),
                args.bucket_digits,
                args.flush_every,
                args.progress_lines,
                not args.no_resume,
            ),
        ]
        parse_results = [f.result() for f in futs]
    eprint(f"[INFO] [main] 解析阶段完成 | 耗时={time.time() - parse_start:.2f}s")
    for r in parse_results:
        eprint(f"[INFO] [main] parse_result={r}")

    # 2) bucket 处理阶段：sort -> export bucket_blocks -> GC
    bucket_start = time.time()
    done_buckets, total_blocks, total_rows = process_buckets_with_gc(
        workdir=workdir,
        bucket_digits=args.bucket_digits,
        sort_mem=args.sort_mem,
        sort_parallel=args.sort_parallel,
        force_sort=args.force_sort,
        min_free_gb_warn=args.min_free_gb_warn,
    )
    eprint(
        f"[INFO] [main] bucket阶段完成 | done_buckets={done_buckets} | blocks={total_blocks} | rows={total_rows} | "
        f"耗时={time.time() - bucket_start:.2f}s"
    )

    # 3) pack 阶段：从 bucket_blocks 重建最终输出
    pack_start = time.time()
    generated = pack_bucket_blocks_to_parts(
        workdir=workdir,
        output_dir=output_dir,
        bucket_digits=args.bucket_digits,
        part_size_mb=args.part_size_mb,
        prefix=args.output_prefix,
        cleanup_bucket_blocks_after_pack=args.cleanup_bucket_blocks_after_pack,
    )
    eprint(f"[INFO] [main] pack阶段完成 | 耗时={time.time() - pack_start:.2f}s")

    eprint(
        f"[INFO] [main] 全部完成 | 总耗时={time.time() - overall_start:.2f}s | 输出文件数={len(generated)} | "
        f"workdir_free={human_size(disk_free_bytes(workdir))}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
