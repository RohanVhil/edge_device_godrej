import os
import glob
import time
import sqlite3
from datetime import datetime, timedelta

class LocalDatabaseManager:
    def __init__(self, db_dir="/home/edge_device/sdcard_data"):
        self.db_dir = db_dir
        os.makedirs(self.db_dir, exist_ok=True)
        
        # In-memory caches for ultra-fast ID lookups
        self._dev_cache = {}
        self._metric_cache = {}
        self._cache_db_path = None
        
        self.init_db()
        self._prune_old_partitions(retention_months=6)

    def _get_current_db_path(self) -> str:
        month_str = datetime.now().strftime("%Y_%m")
        # Creating a v3 database for the new normalized schema
        return os.path.join(self.db_dir, f"telemetry_v3_{month_str}.sqlite3")

    def _get_connection(self, db_path: str = None) -> sqlite3.Connection:
        if db_path is None:
            db_path = self._get_current_db_path()
        conn = sqlite3.connect(db_path, check_same_thread=False, timeout=15.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def init_db(self):
        db_path = self._get_current_db_path()

        # Numeric lookup IDs belong to one monthly database only.  Clear the
        # caches when the month changes so IDs from the previous partition are
        # never written into the new partition.
        if self._cache_db_path != db_path:
            self._dev_cache.clear()
            self._metric_cache.clear()
            self._cache_db_path = db_path

        with self._get_connection(db_path) as conn:
            # Lookup tables for string deduplication
            conn.execute("""
                CREATE TABLE IF NOT EXISTS devices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE
                );
            """)
            # Highly compressed telemetry table using Integer Foreign Keys
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_telemetry (
                    logged_at INTEGER,
                    dev_id INTEGER,
                    metric_id INTEGER,
                    value REAL,
                    PRIMARY KEY (dev_id, logged_at, metric_id)
                ) WITHOUT ROWID;
            """)
            # Index for fast cloud aggregation
            conn.execute("CREATE INDEX IF NOT EXISTS idx_logged_at ON raw_telemetry(logged_at);")

    def _get_id(self, conn, table, name_val, cache_dict):
        """Fetches the integer ID for a string, inserting it if it doesn't exist."""
        if name_val in cache_dict:
            return cache_dict[name_val]
        
        conn.execute(f"INSERT OR IGNORE INTO {table} (name) VALUES (?)", (name_val,))
        cursor = conn.execute(f"SELECT id FROM {table} WHERE name = ?", (name_val,))
        row_id = cursor.fetchone()[0]
        cache_dict[name_val] = row_id
        return row_id

    def _flatten_dict(self, d, parent_key=''):
        items = []
        if isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, dict):
                    items.extend(self._flatten_dict(v, str(k)).items())
                elif isinstance(v, (int, float)) and not isinstance(v, bool):
                    items.append((str(k), float(v)))
        return dict(items)

    def insert_raw_dict_batch(self, raw_samples: list):
        if not raw_samples:
            return

        db_path = self._get_current_db_path()
        self.init_db()

        query = "INSERT OR REPLACE INTO raw_telemetry (logged_at, dev_id, metric_id, value) VALUES (?, ?, ?, ?);"
        records = []

        with self._get_connection(db_path) as conn:
            for ts, dev_str, metric_str, val in raw_samples:
                ts_ms = int(ts * 1000) if ts < 10000000000 else int(ts)
                
                # Convert the massive string into a tiny integer instantly
                dev_id = self._get_id(conn, 'devices', str(dev_str), self._dev_cache)

                if isinstance(val, dict):
                    flat_metrics = self._flatten_dict(val)
                    for sub_k, sub_v in flat_metrics.items():
                        metric_id = self._get_id(conn, 'metrics', str(sub_k), self._metric_cache)
                        records.append((ts_ms, dev_id, metric_id, float(sub_v)))
                elif isinstance(val, (int, float)) and not isinstance(val, bool):
                    metric_id = self._get_id(conn, 'metrics', str(metric_str), self._metric_cache)
                    records.append((ts_ms, dev_id, metric_id, float(val)))

            if records:
                conn.executemany(query, records)

    def get_5min_aggregated_metrics(
        self,
        start_ts_ms: int,
        end_ts_ms: int,
        db_path: str = None
    ) -> dict:
        if db_path is None:
            db_path = self._get_current_db_path()
        if not os.path.exists(db_path):
            return {}

        results = {}

        with self._get_connection(db_path) as conn:
            cursor = conn.cursor()

            # Join to dynamically restore string names for the dataBank class
            cursor.execute("""
                SELECT d.name, m.name, AVG(t.value)
                FROM raw_telemetry t
                JOIN devices d ON t.dev_id = d.id
                JOIN metrics m ON t.metric_id = m.id
                WHERE t.logged_at >= ? AND t.logged_at < ?
                GROUP BY t.dev_id, t.metric_id;
            """, (start_ts_ms, end_ts_ms))
            
            for dev_str, metric_str, avg_val in cursor.fetchall():
                if dev_str not in results: results[dev_str] = {}
                results[dev_str][metric_str] = avg_val

            cursor.execute("""
                SELECT d.name, m.name, t.value
                FROM raw_telemetry t
                JOIN devices d ON t.dev_id = d.id
                JOIN metrics m ON t.metric_id = m.id
                WHERE t.logged_at >= ? AND t.logged_at < ?
                AND m.name IN ('total_energy', 'import_energy', 'export_energy', 'today_energy')
                AND t.logged_at = (
                    SELECT MAX(t2.logged_at)
                    FROM raw_telemetry t2
                    WHERE t2.dev_id = t.dev_id
                    AND t2.metric_id = t.metric_id
                    AND t2.logged_at >= ?
                    AND t2.logged_at < ?
                );
            """, (
                start_ts_ms,
                end_ts_ms,
                start_ts_ms,
                end_ts_ms
            ))
            
            for dev_str, metric_str, latest_val in cursor.fetchall():
                if dev_str not in results: results[dev_str] = {}
                results[dev_str][metric_str] = latest_val

        return results

    def _prune_old_partitions(self, retention_months=6):
        cutoff_date = datetime.now() - timedelta(days=retention_months * 30)
        pattern = os.path.join(self.db_dir, "telemetry_v3_*.sqlite3")
        for db_file in glob.glob(pattern):
            filename = os.path.basename(db_file)
            try:
                date_part = filename.replace("telemetry_v3_", "").replace(".sqlite3", "")
                file_date = datetime.strptime(date_part, "%Y_%m")
                if file_date < cutoff_date:
                    os.remove(db_file)
                    if os.path.exists(db_file + "-wal"): os.remove(db_file + "-wal")
                    if os.path.exists(db_file + "-shm"): os.remove(db_file + "-shm")
                    print(f"[STORAGE PRUNE] Removed 6-month-old partition: {filename}")
            except Exception as e:
                print(f"[PRUNE ERROR] Failed pruning {filename}: {e}")
