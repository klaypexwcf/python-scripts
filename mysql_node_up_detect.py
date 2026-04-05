import os
import json
import time
import logging
from datetime import datetime
from typing import Optional

import mysql.connector
from mysql.connector import Error

# =========================
# 配置区
# =========================
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "your_password",
    "database": "tron",
    "autocommit": False,
}

SOURCE_TABLE = "public_nodes_test"
POLL_INTERVAL_SECONDS = 300  # 5 分钟

# 需要监控的 3 个 ipv4
MONITORED_IPV4S = [
    "1.1.1.1",
    "2.2.2.2",
    "3.3.3.3",
]

# 输出目录
OUTPUT_DIR = "./monitor_output"
STATE_FILE = os.path.join(OUTPUT_DIR, "state.json")

TIME_FMT = "%Y-%m-%d %H:%M:%S.%f"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict) -> None:
    tmp_file = STATE_FILE + ".tmp"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp_file, STATE_FILE)


def get_last_delete_time(state: dict, ipv4: str) -> Optional[datetime]:
    value = state.get(ipv4, {}).get("last_delete_time")
    if not value:
        return None
    return datetime.strptime(value, TIME_FMT)


def update_last_delete_time(state: dict, ipv4: str, delete_time: datetime) -> None:
    state[ipv4] = {
        "last_delete_time": delete_time.strftime(TIME_FMT)
    }


def ip_output_file(ipv4: str) -> str:
    # ipv4 本身可直接作文件名，这里保留一个简单清洗
    safe_name = ipv4.replace("/", "_").replace("\\", "_")
    return os.path.join(OUTPUT_DIR, f"{safe_name}.txt")


def append_result_to_ip_file(
    ipv4: str,
    record_create_time: datetime,
    base_time: datetime,
    interval_seconds: float,
    round_start_time: datetime,
    deleted_at: datetime,
    deleted_count: int,
    note: str,
) -> None:
    out_file = ip_output_file(ipv4)

    with open(out_file, "a", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write(f"ipv4: {ipv4}\n")
        f.write(f"record_create_time: {record_create_time.strftime(TIME_FMT)}\n")
        f.write(f"base_time: {base_time.strftime(TIME_FMT)}\n")
        f.write(f"interval_seconds: {interval_seconds:.6f}\n")
        f.write(f"round_start_time: {round_start_time.strftime(TIME_FMT)}\n")
        f.write(f"deleted_at: {deleted_at.strftime(TIME_FMT)}\n")
        f.write(f"deleted_count: {deleted_count}\n")
        f.write(f"note: {note}\n")
        f.write("\n")


def process_one_ip(ipv4: str) -> None:
    round_start_time = datetime.now()
    state = load_state()

    conn = None
    try:
        conn = get_connection()
        conn.start_transaction()

        # 先取最早一条记录，用于计算
        select_sql = f"""
        SELECT create_time
        FROM {SOURCE_TABLE}
        WHERE ipv4 = %s
        ORDER BY create_time ASC
        LIMIT 1
        FOR UPDATE
        """

        with conn.cursor(dictionary=True) as cursor:
            cursor.execute(select_sql, (ipv4,))
            row = cursor.fetchone()

        if not row:
            conn.rollback()
            logging.info("本轮未找到 ipv4=%s 的记录。", ipv4)
            return

        record_create_time = row["create_time"]

        last_delete_time = get_last_delete_time(state, ipv4)
        if last_delete_time is None:
            base_time = round_start_time
            note = "first_round_use_query_start"
        else:
            base_time = last_delete_time
            note = "use_last_delete_time"

        # 按你之前的要求：
        # create_time - 上次删除时间（或本轮查询开始时间）
        interval_seconds = (record_create_time - base_time).total_seconds()

        # 删除该 ipv4 的所有匹配记录
        delete_sql = f"""
        DELETE FROM {SOURCE_TABLE}
        WHERE ipv4 = %s
        """

        with conn.cursor() as cursor:
            cursor.execute(delete_sql, (ipv4,))
            deleted_count = cursor.rowcount

        if deleted_count < 1:
            conn.rollback()
            logging.warning("删除 0 条记录，已回滚。ipv4=%s", ipv4)
            return

        deleted_at = datetime.now()
        conn.commit()

        # 本地写入 ip 对应文本文件
        append_result_to_ip_file(
            ipv4=ipv4,
            record_create_time=record_create_time,
            base_time=base_time,
            interval_seconds=interval_seconds,
            round_start_time=round_start_time,
            deleted_at=deleted_at,
            deleted_count=deleted_count,
            note=note,
        )

        # 更新本地状态
        update_last_delete_time(state, ipv4, deleted_at)
        save_state(state)

        logging.info(
            "处理成功: ipv4=%s, create_time=%s, base_time=%s, interval_seconds=%.6f, deleted_count=%d, deleted_at=%s",
            ipv4,
            record_create_time.strftime(TIME_FMT),
            base_time.strftime(TIME_FMT),
            interval_seconds,
            deleted_count,
            deleted_at.strftime(TIME_FMT),
        )

    except Error as e:
        if conn is not None:
            conn.rollback()
        logging.exception("数据库操作失败: ipv4=%s, error=%s", ipv4, e)
    except Exception as e:
        if conn is not None:
            conn.rollback()
        logging.exception("处理失败: ipv4=%s, error=%s", ipv4, e)
    finally:
        if conn is not None and conn.is_connected():
            conn.close()


def main() -> None:
    ensure_output_dir()
    logging.info("开始监控，共 %d 个 ipv4，每 %d 秒执行一次。", len(MONITORED_IPV4S), POLL_INTERVAL_SECONDS)

    while True:
        for ipv4 in MONITORED_IPV4S:
            process_one_ip(ipv4)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()