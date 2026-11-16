import os
import glob
import math
import time
import sqlite3
from datetime import datetime, timedelta

class LocalDatabaseManager:
    def __init__(self, db_dir="/home/edge_device/sdcard_data"):
        self.db_dir = db_dir
        os.makedirs(self.db_dir, exist_ok=True)
        self._prune_old_partitions()

    def _get_current_db_path(self) -> str:
        month_str = datetime.now().strftime("%Y_%m")
        return os.path.join(self.db_dir, f"telemetry_{month_str}.sqlite3")

    def _get_connection(self, db_path: str = None) -> sqlite3.Connection:
        if db_path is None:
            db_path = self._get_current_db_path()
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def init_db(self):
        db_path = self._get_current_db_path()
        with self._get_connection(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_telemetry (
                    logged_at INTEGER,
                    device_id TEXT,
                    metric TEXT,
                    value REAL,
                    PRIMARY KEY (device_id, logged_at, metric)
                );
            """)

    def _flatten_dict(self, d, parent_key=''):
        """Recursively flattens nested dicts into key-value pairs."""
        items = []
        if isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, dict):
                    items.extend(self._flatten_dict(v, str(k)).items())
                elif isinstance(v, (int, float)) and not isinstance(v, bool):
                    items.append((str(k), float(v)))
        return dict(items)

    def insert_raw_dict_batch(self, raw_samples: list):
        """Flushes 60s in-RAM buffer samples to local SQLite database partition."""
        if not raw_samples:
            return

        db_path = self._get_current_db_path()
        self.init_db()

        query = "INSERT OR REPLACE INTO raw_telemetry (logged_at, device_id, metric, value) VALUES (?, ?, ?, ?);"
        records = []

        for ts, dev_id, metric, val in raw_samples:
            # Ensure timestamps are stored in Epoch Milliseconds (13 digits)
            ts_ms = int(ts * 1000) if ts < 10000000000 else int(ts)

            if isinstance(val, dict):
                flat_metrics = self._flatten_dict(val)
                for sub_k, sub_v in flat_metrics.items():
                    records.append((ts_ms, str(dev_id), str(sub_k), float(sub_v)))
            elif isinstance(val, (int, float)) and not isinstance(val, bool):
                records.append((ts_ms, str(dev_id), str(metric), float(val)))

        if records:
            with self._get_connection(db_path) as conn:
                conn.executemany(query, records)

    def get_5min_aggregated_metrics(self, start_ts_ms: int, end_ts_ms: int) -> dict:
        """Queries 2-min / 5-min averages and latest values using Epoch Milliseconds."""
        db_path = self._get_current_db_path()
        if not os.path.exists(db_path):
            return {}

        results = {}

        with self._get_connection(db_path) as conn:
            cursor = conn.cursor()

            # 1. Standard Arithmetic AVERAGE for all instantaneous measurements
            cursor.execute("""
                SELECT device_id, metric, AVG(value)
                FROM raw_telemetry
                WHERE logged_at >= ? AND logged_at <= ?
                GROUP BY device_id, metric;
            """, (start_ts_ms, end_ts_ms))
            for dev_id, metric, avg_val in cursor.fetchall():
                if dev_id not in results: results[dev_id] = {}
                results[dev_id][metric] = avg_val

            # 2. Monotonic Accumulators -> LATEST
            cursor.execute("""
                SELECT device_id, metric, value
                FROM raw_telemetry
                WHERE logged_at >= ? AND logged_at <= ?
                AND metric IN ('total_energy', 'import_energy', 'export_energy', 'today_energy','charged_energy', 'discharged_energy')
                GROUP BY device_id, metric HAVING MAX(logged_at);
            """, (start_ts_ms, end_ts_ms))
            for dev_id, metric, latest_val in cursor.fetchall():
                if dev_id not in results: results[dev_id] = {}
                results[dev_id][metric] = latest_val

        return results

    def _prune_old_partitions(self, retention_months=6):
        cutoff_date = datetime.now() - timedelta(days=retention_months * 30)
        pattern = os.path.join(self.db_dir, "telemetry_*.sqlite3")
        for db_file in glob.glob(pattern):
            filename = os.path.basename(db_file)
            try:
                date_part = filename.replace("telemetry_", "").replace(".sqlite3", "")
                file_date = datetime.strptime(date_part, "%Y_%m")
                if file_date < cutoff_date:
                    os.remove(db_file)
                    if os.path.exists(db_file + "-wal"): os.remove(db_file + "-wal")
                    if os.path.exists(db_file + "-shm"): os.remove(db_file + "-shm")
                    print(f"[STORAGE PRUNE] Removed 6-month-old partition: {filename}")
            except Exception as e:
                print(f"[PRUNE ERROR] Failed pruning {filename}: {e}")
