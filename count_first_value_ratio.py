#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
from pathlib import Path

# 纯哈希行：64位十六进制
HASH_RE = re.compile(r"^[0-9a-fA-F]{64}$")

# 记录行格式：
# 2026-04-04 20:42:05.250 34.92.45.131 5
RECORD_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}\s+"
    r"\d{2}:\d{2}:\d{2}(?:\.\d+)?\s+"
    r"(\S+)\s+"
    r"(\d+)\s*$"
)


def process_file(file_path: Path):
    """
    统计单个文件中：
    1. 总块数
    2. 每个块第一条记录行的最后一个数字为1的块数
    """
    total_blocks = 0
    first_value_is_1_blocks = 0

    in_block = False
    waiting_first_record = False

    with file_path.open("r", encoding="utf-8", errors="ignore") as f:
        for lineno, raw_line in enumerate(f, 1):
            line = raw_line.strip()
            if not line:
                continue

            # 如果是哈希行，说明新块开始
            if HASH_RE.fullmatch(line):
                total_blocks += 1
                in_block = True
                waiting_first_record = True
                continue

            # 如果当前在块中，且还在等这个块的第一条记录
            if in_block and waiting_first_record:
                m = RECORD_RE.fullmatch(line)
                if m:
                    last_num = int(m.group(2))
                    if last_num == 1:
                        first_value_is_1_blocks += 1
                    waiting_first_record = False
                else:
                    # 遇到异常行，不终止，继续往下找第一条合法记录
                    # 也可以改成报错提示
                    pass

    return total_blocks, first_value_is_1_blocks


def main():
    if len(sys.argv) != 2:
        print(f"用法: python {Path(sys.argv[0]).name} <输入目录>")
        sys.exit(1)

    input_dir = Path(sys.argv[1])
    if not input_dir.exists() or not input_dir.is_dir():
        print(f"错误：输入目录不存在或不是目录: {input_dir}")
        sys.exit(1)

    total_blocks_all = 0
    first_value_is_1_blocks_all = 0
    file_count = 0

    # 遍历目录下所有普通文件
    for file_path in sorted(input_dir.iterdir()):
        if not file_path.is_file():
            continue

        file_count += 1
        total_blocks, first_value_is_1_blocks = process_file(file_path)

        total_blocks_all += total_blocks
        first_value_is_1_blocks_all += first_value_is_1_blocks

        print(
            f"[文件] {file_path.name} | 块总数: {total_blocks} | "
            f"首记录最后数字=1的块数: {first_value_is_1_blocks}"
        )

    ratio = (
        first_value_is_1_blocks_all / total_blocks_all
        if total_blocks_all > 0 else 0.0
    )

    print("\n===== 汇总结果 =====")
    print(f"文件总数: {file_count}")
    print(f"总块数: {total_blocks_all}")
    print(f"最后数字为1的块数: {first_value_is_1_blocks_all}")
    print(f"比例: {ratio:.6f} ({ratio * 100:.4f}%)")


if __name__ == "__main__":
    main()