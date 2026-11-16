import os
import json
import sys
import time
from datetime import datetime
import pytz
import enum
import re
import zlib
import math

sys.path.insert(0, "../")
from control import control_base as ctrl
import path_config
from database.local_db import LocalDatabaseManager
from google.protobuf.wrappers_pb2 import FloatValue, DoubleValue, Int64Value, Int32Value

class SubcomponentType(enum.IntEnum):
    STRING = 1
    MPPT = 2
    CELL = 3
    RACK = 4

UNASSIGNED_JSON_PATH = os.path.join(path_config.path_cfg.base_path, "reports_handling", "unassigned_devices.json")
UNASSIGNED_BIN_PATH = os.path.join(path_config.path_cfg.base_path, "reports_handling", "unassigned_data.bin")
UNSENT_TELEMETRY_BIN_PATH = os.path.join(path_config.path_cfg.base_path, "reports_handling", "unsent_telemetry.bin")

def set_localdate():
    epoch_timestamp = int(time.time())
    utc_date = datetime.fromtimestamp(epoch_timestamp, tz=pytz.utc)
    local_timezone = pytz.timezone('Asia/Kolkata')
    return utc_date.astimezone(local_timezone).strftime('%Y-%m-%d')

def _get_temp_negative_id(raw_device_id: str) -> int:
    crc_hash = zlib.crc32(str(raw_device_id).encode('utf-8')) & 0x7FFFFFFF
    return -crc_hash if crc_hash != 0 else -1

class dataBank:
    report_period: int = 120
    publisher = None
    last_known_statuses: dict = {}
    is_first_status_run: bool = True

    _cached_id_map: dict = {}
    _id_map_mtime: float = 0

    def __init__(self) -> None:
        self.db_mgr = LocalDatabaseManager()
        self.db_mgr.init_db()

    def _load_id_map(self):
        map_path = os.path.join(path_config.path_cfg.base_path, "reports_handling", "device_id_map.json")
        if os.path.exists(map_path):
            try:
                current_mtime = os.path.getmtime(map_path)
                if current_mtime != self._id_map_mtime:
                    with open(map_path, 'r') as map_file:
                        self._cached_id_map = json.load(map_file)
                        self._id_map_mtime = current_mtime
            except Exception as e:
                print(f"[ID MAPPING ERROR] Failed loading device_id_map.json: {e}")
        return self._cached_id_map

    def _get_numeric_device_id(self, raw_device_id, id_map):
        raw_id_str = str(raw_device_id).strip()

        if raw_id_str in id_map:
            return int(id_map[raw_id_str])

        if raw_id_str.isdigit():
            return int(raw_id_str)

        parts = raw_id_str.split(":")
        if len(parts) >= 3:
            composite = f"{parts[1]}:{parts[2]}"
            if composite in id_map:
                return int(id_map[composite])
            if parts[2] in id_map:
                return int(id_map[parts[2]])

        return None

    def _set_float(self, field, val):
        if val is None:
            return
        try:
            f_type = type(field)
            if f_type == DoubleValue:
                field.CopyFrom(DoubleValue(value=float(val)))
            elif f_type == FloatValue:
                field.CopyFrom(FloatValue(value=float(val)))
            else:
                field.value = float(val)
        except Exception:
            try:
                field.value = float(val)
            except Exception:
                pass

    def _set_int64(self, field, val):
        if val is None:
            return
        try:
            field.CopyFrom(Int64Value(value=int(val)))
        except Exception:
            try:
                field.value = int(val)
            except Exception:
                pass

    def _set_int32(self, field, val):
        if val is None:
            return
        try:
            field.CopyFrom(Int32Value(value=int(val)))
        except Exception:
            try:
                field.value = int(val)
            except Exception:
                pass

    def _check_and_dispatch_status_on_change(self, data_source, timestamp):
        try:
            import reports_handling.enercog_status_pb2 as enercog_status_pb2

            id_map = self._load_id_map()
            epoch_ms = int(timestamp * 1000) if timestamp < 10000000000 else int(timestamp)
            current_statuses = {}

            for raw_device_id, val in data_source.items():
                if not isinstance(val, dict):
                    continue
                raw_id_str = str(raw_device_id)

                flat = {}
                for k, v in val.items():
                    if isinstance(v, dict):
                        for sub_k, sub_v in v.items():
                            if isinstance(sub_v, dict):
                                for s_k, s_v in sub_v.items():
                                    flat[s_k] = s_v
                            else:
                                flat[sub_k] = sub_v
                    else:
                        flat[k] = v

                is_online = False
                if any(flat.get(k) is not None for k in ("SoC", "soc", "bms_status", "grid_charging_status", "voltage", "L1_voltage", "total_power", "acfreq", "ambient_temperature", "module_temperature", "wind_speed", "gti", "ghi")):
                    is_online = True
                elif any(isinstance(v, (int, float)) for v in flat.values()):
                    is_online = True

                current_statuses[raw_id_str] = 2 if is_online else 1

            changed_statuses = {}
            for dev_id_str, status_val in current_statuses.items():
                if self.is_first_status_run or self.last_known_statuses.get(dev_id_str) != status_val:
                    changed_statuses[dev_id_str] = status_val

            if changed_statuses:
                status_batch = enercog_status_pb2.DeviceStatusBatch()
                sorted_devs = sorted(changed_statuses.items(), key=lambda x: self._get_numeric_device_id(x[0], id_map) or 0)

                for raw_device_id, status_val in sorted_devs:
                    numeric_device_id = self._get_numeric_device_id(raw_device_id, id_map)
                    if numeric_device_id is None:
                        continue

                    dev_status = status_batch.device_statuses.add()
                    dev_status.device_id = numeric_device_id
                    dev_status.device_status = status_val
                    dev_status.last_updated_at = epoch_ms

                if self.publisher is not None and len(status_batch.device_statuses) > 0:
                    self.publisher.queue.append(status_batch)
                    self.last_known_statuses.update(current_statuses)
                    log_reason = "Initial Startup" if self.is_first_status_run else f"State Change ({len(changed_statuses)} dev)"
                    print(f"[STATUS DISPATCH] Dispatched DeviceStatusBatch ({log_reason}).")

                self.is_first_status_run = False

        except Exception as e:
            print(f"[STATUS DISPATCH ERROR] {e}")

    def _build_telemetry_batch_from_aggregated(self, agg_results: dict, timestamp: int, allow_unassigned: bool = False):
        import reports_handling.enercog_telemetry_pb2 as enercog_telemetry_pb2

        id_map = self._load_id_map()
        batch = enercog_telemetry_pb2.TelemetryBatch()
        epoch_ms = int(timestamp * 1000) if timestamp < 10000000000 else int(timestamp)

        sorted_devices = sorted(agg_results.items(), key=lambda x: self._get_numeric_device_id(x[0], id_map) or 0)

        for raw_device_id, raw_val in sorted_devices:
            if not isinstance(raw_val, dict):
                continue

            numeric_device_id = self._get_numeric_device_id(raw_device_id, id_map)
            if numeric_device_id is None:
                if allow_unassigned:
                    numeric_device_id = _get_temp_negative_id(str(raw_device_id))
                else:
                    continue

            flat_val = {}
            for k, v in raw_val.items():
                if isinstance(v, dict):
                    for sub_k, sub_v in v.items():
                        if isinstance(sub_v, dict):
                            for s_k, s_v in sub_v.items():
                                flat_val[s_k] = s_v
                        else:
                            flat_val[sub_k] = sub_v
                else:
                    flat_val[k] = v

            device_type = str(flat_val.get("type", "")).lower()

            is_wms = ("wms" in device_type or "weather" in device_type or 
                      "wms" in str(raw_device_id).lower() or 
                      "ambient_temperature" in flat_val or "global_horizontal_irradiance" in flat_val)

            if is_wms:
                weather = batch.weather_readings.add()
                weather.device_id = numeric_device_id
                weather.logged_at = epoch_ms

                self._set_float(weather.ambient_temperature_c, flat_val.get("ambient_temperature"))
                self._set_float(weather.module_temperature_c, flat_val.get("module_temperature"))
                self._set_float(weather.ghi_irradiance_wm2, flat_val.get("ghi") or flat_val.get("global_horizontal_irradiance"))
                self._set_float(weather.gti_irradiance_wm2, flat_val.get("gti") or flat_val.get("global_tilt_irradiance"))
                self._set_float(weather.wind_speed_ms, flat_val.get("wind_speed"))
                self._set_float(weather.wind_direction_deg, flat_val.get("wind_direction"))
                self._set_float(weather.humidity_percent, flat_val.get("humidity"))
                self._set_float(weather.dni_irradiance_wm2, flat_val.get("dni") or flat_val.get("direct_normal_irradiance"))
                self._set_float(weather.dhi_irradiance_wm2, flat_val.get("dhi") or flat_val.get("diffuse_horizontal_irradiance"))
                self._set_float(weather.barometric_pressure_hpa, flat_val.get("pressure") or flat_val.get("barometric_pressure"))
                self._set_float(weather.rain_gauge_mm, flat_val.get("rain_gauge") or flat_val.get("rain"))
                self._set_float(weather.soiling_ratio, flat_val.get("soiling_ratio") or flat_val.get("soiling"))
                self._set_float(weather.cloud_cover_percent, flat_val.get("cloud_cover"))
                self._set_float(weather.max_wind_speed, flat_val.get("max_wind_speed") or flat_val.get("gust_speed"))
                self._set_int64(weather.max_wind_speed_ts, flat_val.get("max_wind_speed_ts"))
            else:
                comp = batch.component_readings.add()
                comp.device_id = numeric_device_id
                comp.logged_at = epoch_ms

                p_active = flat_val.get("total_power")
                q_reactive = flat_val.get("reactive_power") or flat_val.get("Q_total")
                s_apparent = flat_val.get("apparent_power") or flat_val.get("S_total")
                pf_factor = flat_val.get("Pf") or flat_val.get("power_factor")

                if s_apparent is None and p_active is not None and p_active > 0:
                    l1_v, l2_v, l3_v = flat_val.get("L1_voltage"), flat_val.get("L2_voltage"), flat_val.get("L3_voltage")
                    l1_i, l2_i, l3_i = flat_val.get("L1_current"), flat_val.get("L2_current"), flat_val.get("L3_current")
                    if l1_v and l2_v and l3_v and l1_i and l2_i and l3_i:
                        v_avg = (l1_v + l2_v + l3_v) / 3.0
                        i_avg = (l1_i + l2_i + l3_i) / 3.0
                        s_apparent = 1.73205 * v_avg * i_avg

                if pf_factor is None and p_active is not None and s_apparent is not None and s_apparent > 0:
                    pf_factor = min(1.0, max(0.0, float(p_active) / float(s_apparent)))

                if q_reactive is None and s_apparent is not None and p_active is not None and s_apparent >= p_active:
                    q_reactive = math.sqrt(max(0.0, s_apparent**2 - p_active**2))

                self._set_float(comp.active_power_w, p_active)
                self._set_float(comp.reactive_power_var, q_reactive)
                self._set_float(comp.apparent_power_va, s_apparent)
                self._set_float(comp.frequency_hz, flat_val.get("acfreq"))
                self._set_float(comp.power_factor, pf_factor)
                self._set_float(comp.l1_voltage, flat_val.get("L1_voltage"))
                self._set_float(comp.l2_voltage, flat_val.get("L2_voltage"))
                self._set_float(comp.l3_voltage, flat_val.get("L3_voltage"))
                self._set_float(comp.l1_current, flat_val.get("L1_current"))
                self._set_float(comp.l2_current, flat_val.get("L2_current"))
                self._set_float(comp.l3_current, flat_val.get("L3_current"))
                self._set_float(comp.l1_power_w, flat_val.get("L1_power"))
                self._set_float(comp.l2_power_w, flat_val.get("L2_power"))
                self._set_float(comp.l3_power_w, flat_val.get("L3_power"))
                self._set_float(comp.input_power_w, flat_val.get("input_power"))
                self._set_float(comp.cabinet_temperature_c, flat_val.get("temperature"))
                self._set_float(comp.total_energy_kwh, flat_val.get("total_energy"))
                self._set_float(comp.import_energy_kwh, flat_val.get("import_energy"))
                self._set_float(comp.export_energy_kwh, flat_val.get("export_energy"))

                self._set_float(comp.charged_energy_kwh, flat_val.get("charged_energy"))
                self._set_float(comp.discharged_energy_kwh, flat_val.get("discharged_energy"))

                soc = flat_val.get("SoC")
                if soc is None:
                    soc = flat_val.get("soc")
                self._set_float(comp.soc_percent, soc)

                soh = flat_val.get("SoH")
                if soh is None:
                    soh = flat_val.get("soh")
                self._set_float(comp.soh_percent, soh)

                self._set_float(comp.max_charging_current_limit_a, flat_val.get("maximum_charging_current"))
                self._set_float(comp.max_discharging_current_limit_a, flat_val.get("maximum_discharging_current"))
                self._set_float(comp.max_charging_power_limit_kw, flat_val.get("maximum_charging_power"))
                self._set_float(comp.max_discharging_power_limit_kw, flat_val.get("maximum_discharging_power"))

                self._set_float(comp.max_cell_voltage_v, flat_val.get("maximum_cell_voltage"))
                self._set_float(comp.min_cell_voltage_v, flat_val.get("minimum_cell_voltage"))
                self._set_float(comp.max_cell_temperature_c, flat_val.get("maximum_cell_temperature"))
                self._set_float(comp.min_cell_temperature_c, flat_val.get("minimum_cell_temperature"))

                self._set_int32(comp.cell_num_max_volt, flat_val.get("cell_number_with_maximum_voltage"))
                self._set_int32(comp.cell_num_min_volt, flat_val.get("cell_number_with_minimum_voltage"))
                self._set_int32(comp.cell_num_max_temp, flat_val.get("cell_number_with_maximum_temperature"))
                self._set_int32(comp.cell_num_min_temp, flat_val.get("cell_number_with_minimum_temperature"))

                bms_status = flat_val.get("bms_status")
                if bms_status is None:
                    bms_status = flat_val.get("device_status")
                self._set_int32(comp.bms_status, bms_status)

                if flat_val.get("voltage") is not None:
                    self._set_float(comp.dc_bus_voltage_v, flat_val.get("voltage"))
                if flat_val.get("current") is not None:
                    self._set_float(comp.dc_total_current_a, flat_val.get("current"))

                sub_dict = {}

                def _parse_param(param_k, param_v):
                    if param_v is None or not isinstance(param_v, (int, float)):
                        return

                    m_mppt = re.match(r"^mppt([0-9]+)_(voltage|current|power)$", str(param_k), re.IGNORECASE)
                    if m_mppt:
                        sub_id = int(m_mppt.group(1))
                        metric = m_mppt.group(2).lower()
                        key = (SubcomponentType.MPPT, sub_id)
                        if key not in sub_dict:
                            sub_dict[key] = {}
                        sub_dict[key][metric] = float(param_v)
                        return

                    m_str = re.match(r"^string([0-9]+)_(current|voltage|power)$", str(param_k), re.IGNORECASE)
                    if m_str:
                        sub_id = int(m_str.group(1))
                        metric = m_str.group(2).lower()
                        key = (SubcomponentType.STRING, sub_id)
                        if key not in sub_dict:
                            sub_dict[key] = {}
                        sub_dict[key][metric] = float(param_v)

                for k, v in flat_val.items():
                    _parse_param(k, v)

                sorted_subs = sorted(sub_dict.items(), key=lambda x: (x[0][0], x[0][1]))

                for (sub_type, sub_id), metrics in sorted_subs:
                    sub = batch.subcomponent_readings.add()
                    sub.device_id = numeric_device_id
                    sub.subcomponent_id = sub_id
                    sub.subcomponent_type = int(sub_type)
                    sub.logged_at = epoch_ms

                    if "voltage" in metrics:
                        self._set_float(sub.voltage, metrics["voltage"])
                    if "current" in metrics:
                        self._set_float(sub.current, metrics["current"])
                    if "power" in metrics:
                        self._set_float(sub.power_w, metrics["power"])

        return batch

    def _build_payload_batch(self, data_source, timestamp):
        self._check_and_dispatch_status_on_change(data_source, timestamp)
        return self._build_telemetry_batch_from_aggregated(data_source, timestamp)

    def runDataLoop(self):
        while not os.path.exists(path_config.path_cfg.base_path + "devices.json"):
            time.sleep(1)

        last_report_time = time.time()

        while os.path.exists(path_config.path_cfg.base_path + "devices.json"):
            try:
                try:
                    with open(path_config.path_cfg.base_path + "reports_handling/report_cfg.json") as report_cfg:
                        report_config = json.load(report_cfg)
                    self.report_period = report_config.get("reporting_period", 120)
                except Exception:
                    self.report_period = 120

                current_time = time.time()

                if current_time - last_report_time >= self.report_period:
                    print(f"[DEBUG] {self.report_period}s timer triggered. Querying local database...")
                    
                    end_ts_sec = int(current_time)
                    start_ts_sec = end_ts_sec - self.report_period
                    end_ts_ms = end_ts_sec * 1000
                    start_ts_ms = start_ts_sec * 1000

                    agg_results = self.db_mgr.get_5min_aggregated_metrics(start_ts_ms, end_ts_ms)

                    if agg_results:
                        print(f"[DEBUG] Database returned data. Building telemetry batch...")
                        batch = self._build_telemetry_batch_from_aggregated(agg_results, end_ts_ms)

                        if len(batch.component_readings) > 0 or len(batch.weather_readings) > 0:
                            if self.publisher is not None:
                                self.publisher.queue.append(batch)
                                is_online = getattr(self.publisher, "is_connected", True)
                                state = "Dispatched" if is_online else "Buffered (Offline)"
                                print(f"[CLOUD TELEMETRY] {state} {self.report_period}s aggregated batch ({len(batch.component_readings)} comp, {len(batch.weather_readings)} weather).")
                    else:
                        print("[DEBUG] Database query returned empty results for this window.")

                    last_report_time = current_time

            except Exception as e:
                print(f"[REPORTS ERROR] Failed running telemetry dispatch loop: {e}")

            time.sleep(1)

data_handler: dataBank = None


















################################################################# 17-09-26 ####################################3




'''import os
import json
import sys
import time
from datetime import datetime
import pytz
import enum
import re
import zlib
import math

sys.path.insert(0, "../")
from control import control_base as ctrl
import path_config
from database.local_db import LocalDatabaseManager
from google.protobuf.wrappers_pb2 import FloatValue, DoubleValue, Int64Value, Int32Value

class SubcomponentType(enum.IntEnum):
    STRING = 1
    MPPT = 2
    CELL = 3
    RACK = 4

UNASSIGNED_JSON_PATH = os.path.join(path_config.path_cfg.base_path, "reports_handling", "unassigned_devices.json")
UNASSIGNED_BIN_PATH = os.path.join(path_config.path_cfg.base_path, "reports_handling", "unassigned_data.bin")
UNSENT_TELEMETRY_BIN_PATH = os.path.join(path_config.path_cfg.base_path, "reports_handling", "unsent_telemetry.bin")

def set_localdate():
    epoch_timestamp = int(time.time())
    utc_date = datetime.fromtimestamp(epoch_timestamp, tz=pytz.utc)
    local_timezone = pytz.timezone('Asia/Kolkata')
    return utc_date.astimezone(local_timezone).strftime('%Y-%m-%d')

def _get_temp_negative_id(raw_device_id: str) -> int:
    crc_hash = zlib.crc32(str(raw_device_id).encode('utf-8')) & 0x7FFFFFFF
    return -crc_hash if crc_hash != 0 else -1

class dataBank:
    report_period: int = 120
    publisher = None
    last_known_statuses: dict = {}
    is_first_status_run: bool = True

    _cached_id_map: dict = {}
    _id_map_mtime: float = 0

    def __init__(self) -> None:
        self.db_mgr = LocalDatabaseManager()
        self.db_mgr.init_db()

    def _load_id_map(self):
        map_path = os.path.join(path_config.path_cfg.base_path, "reports_handling", "device_id_map.json")
        if os.path.exists(map_path):
            try:
                current_mtime = os.path.getmtime(map_path)
                if current_mtime != self._id_map_mtime:
                    with open(map_path, 'r') as map_file:
                        self._cached_id_map = json.load(map_file)
                        self._id_map_mtime = current_mtime
            except Exception as e:
                print(f"[ID MAPPING ERROR] Failed loading device_id_map.json: {e}")
        return self._cached_id_map

    def _get_numeric_device_id(self, raw_device_id, id_map):
        raw_id_str = str(raw_device_id).strip()

        if raw_id_str in id_map:
            return int(id_map[raw_id_str])

        if raw_id_str.isdigit():
            return int(raw_id_str)

        parts = raw_id_str.split(":")
        if len(parts) >= 3:
            composite = f"{parts[1]}:{parts[2]}"
            if composite in id_map:
                return int(id_map[composite])
            if parts[2] in id_map:
                return int(id_map[parts[2]])

        return None

    def _set_float(self, field, val):
        if val is None:
            return
        try:
            f_type = type(field)
            if f_type == DoubleValue:
                field.CopyFrom(DoubleValue(value=float(val)))
            elif f_type == FloatValue:
                field.CopyFrom(FloatValue(value=float(val)))
            else:
                field.value = float(val)
        except Exception:
            try:
                field.value = float(val)
            except Exception:
                pass

    def _set_int64(self, field, val):
        if val is None:
            return
        try:
            field.CopyFrom(Int64Value(value=int(val)))
        except Exception:
            try:
                field.value = int(val)
            except Exception:
                pass

    def _set_int32(self, field, val):
        if val is None:
            return
        try:
            field.CopyFrom(Int32Value(value=int(val)))
        except Exception:
            try:
                field.value = int(val)
            except Exception:
                pass

    def _check_and_dispatch_status_on_change(self, data_source, timestamp):
        try:
            import reports_handling.enercog_status_pb2 as enercog_status_pb2

            id_map = self._load_id_map()
            epoch_ms = int(timestamp * 1000) if timestamp < 10000000000 else int(timestamp)
            current_statuses = {}

            for raw_device_id, val in data_source.items():
                if not isinstance(val, dict):
                    continue
                raw_id_str = str(raw_device_id)

                flat = {}
                for k, v in val.items():
                    if isinstance(v, dict):
                        for sub_k, sub_v in v.items():
                            if isinstance(sub_v, dict):
                                for s_k, s_v in sub_v.items():
                                    flat[s_k] = s_v
                            else:
                                flat[sub_k] = sub_v
                    else:
                        flat[k] = v

                is_online = False
                if any(flat.get(k) is not None for k in ("SoC", "soc", "bms_status", "grid_charging_status", "voltage", "L1_voltage", "total_power", "acfreq", "ambient_temperature", "module_temperature", "wind_speed", "gti", "ghi")):
                    is_online = True
                elif any(isinstance(v, (int, float)) for v in flat.values()):
                    is_online = True

                current_statuses[raw_id_str] = 2 if is_online else 1

            changed_statuses = {}
            for dev_id_str, status_val in current_statuses.items():
                if self.is_first_status_run or self.last_known_statuses.get(dev_id_str) != status_val:
                    changed_statuses[dev_id_str] = status_val

            if changed_statuses:
                status_batch = enercog_status_pb2.DeviceStatusBatch()
                sorted_devs = sorted(changed_statuses.items(), key=lambda x: self._get_numeric_device_id(x[0], id_map) or 0)

                for raw_device_id, status_val in sorted_devs:
                    numeric_device_id = self._get_numeric_device_id(raw_device_id, id_map)
                    if numeric_device_id is None:
                        continue

                    dev_status = status_batch.device_statuses.add()
                    dev_status.device_id = numeric_device_id
                    dev_status.device_status = status_val
                    dev_status.last_updated_at = epoch_ms

                if self.publisher is not None and len(status_batch.device_statuses) > 0:
                    self.publisher.queue.append(status_batch)
                    self.last_known_statuses.update(current_statuses)
                    log_reason = "Initial Startup" if self.is_first_status_run else f"State Change ({len(changed_statuses)} dev)"
                    print(f"[STATUS DISPATCH] Dispatched DeviceStatusBatch ({log_reason}).")

                self.is_first_status_run = False

        except Exception as e:
            print(f"[STATUS DISPATCH ERROR] {e}")

    def _build_telemetry_batch_from_aggregated(self, agg_results: dict, timestamp: int, allow_unassigned: bool = False):
        import reports_handling.enercog_telemetry_pb2 as enercog_telemetry_pb2

        id_map = self._load_id_map()
        batch = enercog_telemetry_pb2.TelemetryBatch()
        epoch_ms = int(timestamp * 1000) if timestamp < 10000000000 else int(timestamp)

        sorted_devices = sorted(agg_results.items(), key=lambda x: self._get_numeric_device_id(x[0], id_map) or 0)

        for raw_device_id, raw_val in sorted_devices:
            if not isinstance(raw_val, dict):
                continue

            numeric_device_id = self._get_numeric_device_id(raw_device_id, id_map)
            if numeric_device_id is None:
                if allow_unassigned:
                    numeric_device_id = _get_temp_negative_id(str(raw_device_id))
                else:
                    continue

            flat_val = {}
            for k, v in raw_val.items():
                if isinstance(v, dict):
                    for sub_k, sub_v in v.items():
                        if isinstance(sub_v, dict):
                            for s_k, s_v in sub_v.items():
                                flat_val[s_k] = s_v
                        else:
                            flat_val[sub_k] = sub_v
                else:
                    flat_val[k] = v

            device_type = str(flat_val.get("type", "")).lower()

            is_wms = ("wms" in device_type or "weather" in device_type or 
                      "wms" in str(raw_device_id).lower() or 
                      "ambient_temperature" in flat_val or "global_horizontal_irradiance" in flat_val)

            if is_wms:
                weather = batch.weather_readings.add()
                weather.device_id = numeric_device_id
                weather.logged_at = epoch_ms

                self._set_float(weather.ambient_temperature_c, flat_val.get("ambient_temperature"))
                self._set_float(weather.module_temperature_c, flat_val.get("module_temperature"))
                self._set_float(weather.ghi_irradiance_wm2, flat_val.get("ghi") or flat_val.get("global_horizontal_irradiance"))
                self._set_float(weather.gti_irradiance_wm2, flat_val.get("gti") or flat_val.get("global_tilt_irradiance"))
                self._set_float(weather.wind_speed_ms, flat_val.get("wind_speed"))
                self._set_float(weather.wind_direction_deg, flat_val.get("wind_direction"))
                self._set_float(weather.humidity_percent, flat_val.get("humidity"))
                self._set_float(weather.dni_irradiance_wm2, flat_val.get("dni") or flat_val.get("direct_normal_irradiance"))
                self._set_float(weather.dhi_irradiance_wm2, flat_val.get("dhi") or flat_val.get("diffuse_horizontal_irradiance"))
                self._set_float(weather.barometric_pressure_hpa, flat_val.get("pressure") or flat_val.get("barometric_pressure"))
                self._set_float(weather.rain_gauge_mm, flat_val.get("rain_gauge") or flat_val.get("rain"))
                self._set_float(weather.soiling_ratio, flat_val.get("soiling_ratio") or flat_val.get("soiling"))
                self._set_float(weather.cloud_cover_percent, flat_val.get("cloud_cover"))
                self._set_float(weather.max_wind_speed, flat_val.get("max_wind_speed") or flat_val.get("gust_speed"))
                self._set_int64(weather.max_wind_speed_ts, flat_val.get("max_wind_speed_ts"))
            else:
                comp = batch.component_readings.add()
                comp.device_id = numeric_device_id
                comp.logged_at = epoch_ms

                p_active = flat_val.get("total_power")
                q_reactive = flat_val.get("reactive_power") or flat_val.get("Q_total")
                s_apparent = flat_val.get("apparent_power") or flat_val.get("S_total")
                pf_factor = flat_val.get("Pf") or flat_val.get("power_factor")

                if s_apparent is None and p_active is not None and p_active > 0:
                    l1_v, l2_v, l3_v = flat_val.get("L1_voltage"), flat_val.get("L2_voltage"), flat_val.get("L3_voltage")
                    l1_i, l2_i, l3_i = flat_val.get("L1_current"), flat_val.get("L2_current"), flat_val.get("L3_current")
                    if l1_v and l2_v and l3_v and l1_i and l2_i and l3_i:
                        v_avg = (l1_v + l2_v + l3_v) / 3.0
                        i_avg = (l1_i + l2_i + l3_i) / 3.0
                        s_apparent = 1.73205 * v_avg * i_avg

                if pf_factor is None and p_active is not None and s_apparent is not None and s_apparent > 0:
                    pf_factor = min(1.0, max(0.0, float(p_active) / float(s_apparent)))

                if q_reactive is None and s_apparent is not None and p_active is not None and s_apparent >= p_active:
                    q_reactive = math.sqrt(max(0.0, s_apparent**2 - p_active**2))

                self._set_float(comp.active_power_w, p_active)
                self._set_float(comp.reactive_power_var, q_reactive)
                self._set_float(comp.apparent_power_va, s_apparent)
                self._set_float(comp.frequency_hz, flat_val.get("acfreq"))
                self._set_float(comp.power_factor, pf_factor)
                self._set_float(comp.l1_voltage, flat_val.get("L1_voltage"))
                self._set_float(comp.l2_voltage, flat_val.get("L2_voltage"))
                self._set_float(comp.l3_voltage, flat_val.get("L3_voltage"))
                self._set_float(comp.l1_current, flat_val.get("L1_current"))
                self._set_float(comp.l2_current, flat_val.get("L2_current"))
                self._set_float(comp.l3_current, flat_val.get("L3_current"))
                self._set_float(comp.l1_power_w, flat_val.get("L1_power"))
                self._set_float(comp.l2_power_w, flat_val.get("L2_power"))
                self._set_float(comp.l3_power_w, flat_val.get("L3_power"))
                self._set_float(comp.input_power_w, flat_val.get("input_power"))
                self._set_float(comp.cabinet_temperature_c, flat_val.get("temperature"))
                self._set_float(comp.total_energy_kwh, flat_val.get("total_energy"))
                self._set_float(comp.import_energy_kwh, flat_val.get("import_energy"))
                self._set_float(comp.export_energy_kwh, flat_val.get("export_energy"))

                sub_dict = {}

                def _parse_param(param_k, param_v):
                    if param_v is None or not isinstance(param_v, (int, float)):
                        return

                    m_mppt = re.match(r"^mppt([0-9]+)_(voltage|current|power)$", str(param_k), re.IGNORECASE)
                    if m_mppt:
                        sub_id = int(m_mppt.group(1))
                        metric = m_mppt.group(2).lower()
                        key = (SubcomponentType.MPPT, sub_id)
                        if key not in sub_dict:
                            sub_dict[key] = {}
                        sub_dict[key][metric] = float(param_v)
                        return

                    m_str = re.match(r"^string([0-9]+)_(current|voltage|power)$", str(param_k), re.IGNORECASE)
                    if m_str:
                        sub_id = int(m_str.group(1))
                        metric = m_str.group(2).lower()
                        key = (SubcomponentType.STRING, sub_id)
                        if key not in sub_dict:
                            sub_dict[key] = {}
                        sub_dict[key][metric] = float(param_v)

                for k, v in flat_val.items():
                    _parse_param(k, v)

                sorted_subs = sorted(sub_dict.items(), key=lambda x: (x[0][0], x[0][1]))

                for (sub_type, sub_id), metrics in sorted_subs:
                    sub = batch.subcomponent_readings.add()
                    sub.device_id = numeric_device_id
                    sub.subcomponent_id = sub_id
                    sub.subcomponent_type = int(sub_type)
                    sub.logged_at = epoch_ms

                    if "voltage" in metrics:
                        self._set_float(sub.voltage, metrics["voltage"])
                    if "current" in metrics:
                        self._set_float(sub.current, metrics["current"])
                    if "power" in metrics:
                        self._set_float(sub.power_w, metrics["power"])

        return batch

    def _build_payload_batch(self, data_source, timestamp):
        self._check_and_dispatch_status_on_change(data_source, timestamp)
        return self._build_telemetry_batch_from_aggregated(data_source, timestamp)

    def runDataLoop(self):
        while not os.path.exists(path_config.path_cfg.base_path + "devices.json"):
            time.sleep(1)

        last_report_time = time.time()

        while os.path.exists(path_config.path_cfg.base_path + "devices.json"):
            try:
                try:
                    with open(path_config.path_cfg.base_path + "reports_handling/report_cfg.json") as report_cfg:
                        report_config = json.load(report_cfg)
                    self.report_period = report_config.get("reporting_period", 120)
                except Exception:
                    self.report_period = 120

                current_time = time.time()

                if current_time - last_report_time >= self.report_period:
                    print(f"[DEBUG] {self.report_period}s timer triggered. Querying local database...")
                    
                    end_ts_sec = int(current_time)
                    start_ts_sec = end_ts_sec - self.report_period
                    end_ts_ms = end_ts_sec * 1000
                    start_ts_ms = start_ts_sec * 1000

                    agg_results = self.db_mgr.get_5min_aggregated_metrics(start_ts_ms, end_ts_ms)

                    if agg_results:
                        print(f"[DEBUG] Database returned data. Building telemetry batch...")
                        batch = self._build_telemetry_batch_from_aggregated(agg_results, end_ts_ms)

                        if len(batch.component_readings) > 0 or len(batch.weather_readings) > 0:
                            if self.publisher is not None:
                                self.publisher.queue.append(batch)
                                is_online = getattr(self.publisher, "is_connected", True)
                                state = "Dispatched" if is_online else "Buffered (Offline)"
                                print(f"[CLOUD TELEMETRY] {state} {self.report_period}s aggregated batch ({len(batch.component_readings)} comp, {len(batch.weather_readings)} weather).")
                    else:
                        print("[DEBUG] Database query returned empty results for this window.")

                    last_report_time = current_time

            except Exception as e:
                print(f"[REPORTS ERROR] Failed running telemetry dispatch loop: {e}")

            time.sleep(1)

data_handler: dataBank = None
'''
