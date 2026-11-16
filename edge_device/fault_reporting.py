import os
import json
import time
import logging
from control import control_base as ctrl
import path_config
from google.protobuf.wrappers_pb2 import Int32Value

class FaultProcessor:
    def __init__(self, error_codes_path=None, poll_interval=10, publisher=None):
        if error_codes_path is None:
            self.error_codes_path = os.path.join(path_config.path_cfg.base_path, 'modbus_mappings', 'error_codes.json')
        else:
            self.error_codes_path = error_codes_path

        self.last_faults = {}
        self.poll_interval = poll_interval
        self._running = False
        self.publisher = publisher
        self.error_map = {}
        self._error_map_mtime = 0
        self._load_error_map()

    def _load_error_map(self):
        if os.path.exists(self.error_codes_path):
            try:
                current_mtime = os.path.getmtime(self.error_codes_path)
                if current_mtime != self._error_map_mtime:
                    with open(self.error_codes_path, 'r') as error_json:
                        self.error_map = json.load(error_json)
                        self._error_map_mtime = current_mtime
            except Exception as e:
                logging.error(f"Failed loading error codes file: {e}")

    def _load_id_map(self):
        map_path = os.path.join(path_config.path_cfg.base_path, "reports_handling", "device_id_map.json")
        if os.path.exists(map_path):
            try:
                with open(map_path, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _get_numeric_device_id(self, raw_device_id, id_map):
        raw_id_str = str(raw_device_id)
        if raw_id_str in id_map:
            return int(id_map[raw_id_str])
        elif raw_id_str.isdigit():
            return int(raw_id_str)
        return None

    def _send_faults_to_amqp(self, active_faults_dict):
        if not active_faults_dict:
            return

        try:
            import reports_handling.enercog_alerts_pb2 as enercog_alerts_pb2
            id_map = self._load_id_map()
            alert_batch = enercog_alerts_pb2.DeviceAlertBatch()
            epoch_ms = int(time.time() * 1000)

            # Sort device IDs numerically
            sorted_faults = sorted(active_faults_dict.items(), key=lambda x: self._get_numeric_device_id(x[0], id_map) or 0)

            for raw_device_id, alert_info in sorted_faults:
                numeric_id = self._get_numeric_device_id(raw_device_id, id_map)
                if numeric_id is None:
                    continue

                alert = alert_batch.device_alerts.add()
                alert.device_id = numeric_id
                alert.logged_at = epoch_ms
                
                # Convert string severity to Protobuf Int32Value (1=Info, 2=Medium, 3=High)
                sev_str = str(alert_info.get("severity", "Info")).lower()
                sev_int = 1 if "info" in sev_str or "low" in sev_str else (2 if "medium" in sev_str else 3)
                alert.severity.CopyFrom(Int32Value(value=sev_int))
                
                raw_fault_code = int(alert_info.get("fault_code", 0))
                alert.fault_code.CopyFrom(Int32Value(value=raw_fault_code))

            if self.publisher is not None and len(alert_batch.device_alerts) > 0:
                self.publisher.queue.append(alert_batch)
                print(f"[ALERTS DISPATCH] Enqueued DeviceAlertBatch ({len(alert_batch.device_alerts)} alerts).")

        except Exception as e:
            logging.error(f"Failed constructing DeviceAlertBatch: {e}")

    def stop(self):
        self._running = False

    def run(self):
        self._running = True

        while self._running:
            try:
                self._load_error_map()

                if hasattr(ctrl, 'getFaultData'):
                    fault_data = ctrl.getFaultData()
                else:
                    fault_data = {
                        str(getattr(dev, 'device_id', index)): {'fault': 0}
                        for index, dev in enumerate(getattr(ctrl, 'device_list', []))
                    }

                new_faults_by_device = {}

                for device_id, fault_dict in fault_data.items():
                    fault_code = int(fault_dict.get('fault', 0))
                    
                    parts = device_id.split(':')
                    part_num = parts[1] if len(parts) > 1 else device_id

                    last_code = self.last_faults.get(device_id)

                    if fault_code == last_code:
                        continue

                    self.last_faults[device_id] = fault_code

                    if fault_code != 0:
                        device_config = self.error_map.get(part_num, {}).get("Fault", self.error_map.get(part_num, {}).get("fault", {}))
                        severity_val = "High"

                        if device_config:
                            fault_definitions = device_config.get("codes", {})
                            for complex_key, info in fault_definitions.items():
                                code_parts = complex_key.split('|')
                                matched = False
                                for part in code_parts:
                                    if '-' in part:
                                        r_parts = part.split('-')
                                        if len(r_parts) == 2 and int(r_parts[0]) <= fault_code <= int(r_parts[1]):
                                            matched = True
                                            break
                                    elif part.isdigit() and int(part) == fault_code:
                                        matched = True
                                        break
                                if matched:
                                    severity_val = info.get("severity", "High")
                                    break

                        new_faults_by_device[device_id] = {
                            "fault_code": fault_code,
                            "severity": severity_val
                        }
                    else:
                        new_faults_by_device[device_id] = {
                            "fault_code": 0,
                            "severity": "Info"
                        }

                if new_faults_by_device:
                    self._send_faults_to_amqp(new_faults_by_device)

            except Exception as e:
                logging.error(f"Error in fault processing loop: {e}")

            time.sleep(self.poll_interval)
