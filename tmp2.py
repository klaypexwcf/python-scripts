#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import pandas as pd


def read_table(path: str) -> pd.DataFrame:
    """
    根据扩展名读取表格文件，支持:
    - .csv
    - .tsv / .txt
    - .xlsx / .xls
    """
    ext = os.path.splitext(path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    elif ext in [".tsv", ".txt"]:
        return pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(path, dtype=str, keep_default_na=False)
    else:
        # 尝试自动分隔符识别
        return pd.read_csv(path, sep=None, engine="python", dtype=str, keep_default_na=False)


def write_table(df: pd.DataFrame, path: str):
    """
    根据输出扩展名写文件，支持:
    - .csv
    - .tsv / .txt
    - .xlsx
    """
    ext = os.path.splitext(path)[1].lower()

    if ext == ".csv":
        df.to_csv(path, index=False, encoding="utf-8-sig")
    elif ext in [".tsv", ".txt"]:
        df.to_csv(path, sep="\t", index=False, encoding="utf-8-sig")
    elif ext in [".xlsx", ".xls"]:
        df.to_excel(path, index=False)
    else:
        raise ValueError(f"不支持的输出文件格式: {ext}")


def main():
    parser = argparse.ArgumentParser(
        description="根据第二个文件中的 targetId -> source 映射，为第一个文件补充 source 列"
    )
    parser.add_argument("input1", help="第一个输入文件（需要补 source 的文件）")
    parser.add_argument("input2", help="第二个输入文件（包含 targetId 和 source）")
    parser.add_argument("output", help="输出文件路径")
    parser.add_argument(
        "--id-col",
        default="targetId",
        help="关联字段名，默认 targetId"
    )
    parser.add_argument(
        "--source-col",
        default="source",
        help="来源字段名，默认 source"
    )
    parser.add_argument(
        "--keep-old-source",
        action="store_true",
        help="如果第一个文件本来就有 source 列，则保留原值；默认会覆盖/重建 source 列"
    )

    args = parser.parse_args()

    df1 = read_table(args.input1)
    df2 = read_table(args.input2)

    id_col = args.id_col
    source_col = args.source_col

    if id_col not in df1.columns:
        print(f"错误：第一个文件中找不到列 {id_col}", file=sys.stderr)
        sys.exit(1)

    if id_col not in df2.columns:
        print(f"错误：第二个文件中找不到列 {id_col}", file=sys.stderr)
        sys.exit(1)

    if source_col not in df2.columns:
        print(f"错误：第二个文件中找不到列 {source_col}", file=sys.stderr)
        sys.exit(1)

    # 清理字符串首尾空白
    df1[id_col] = df1[id_col].astype(str).str.strip()
    df2[id_col] = df2[id_col].astype(str).str.strip()
    df2[source_col] = df2[source_col].astype(str).str.strip()

    # 如果第二个文件中 targetId 重复，保留最后一个
    df2_map = df2[[id_col, source_col]].drop_duplicates(subset=[id_col], keep="last")

    # 建立映射
    source_map = dict(zip(df2_map[id_col], df2_map[source_col]))

    # 给第一个文件添加/更新 source
    new_source = df1[id_col].map(source_map).fillna("")

    if args.keep_old_source and source_col in df1.columns:
        # 仅当原 source 为空时，用映射补充
        df1[source_col] = df1[source_col].astype(str).fillna("")
        df1[source_col] = df1[source_col].where(df1[source_col] != "", new_source)
    else:
        df1[source_col] = new_source

    write_table(df1, args.output)

    matched = (df1[source_col] != "").sum()
    total = len(df1)
    print(f"处理完成：{args.output}")
    print(f"总行数: {total}")
    print(f"成功补充非空 source 的行数: {matched}")
    print(f"空 source 的行数: {total - matched}")


if __name__ == "__main__":
    main()