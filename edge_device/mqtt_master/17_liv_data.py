import os
import json
import time
import ssl
import pika
import sys

sys.path.insert(0, '../')
import path_config
from control import control_base as ctrl

cached_id_map = {}
id_map_mtime = 0
persistent_channel = None
persistent_connection = None

def get_site_id():
    if hasattr(ctrl, 'site_id') and ctrl.site_id:
        return str(ctrl.site_id)
    try:
        cfg_path = os.path.join(path_config.path_cfg.base_path, "../submodules/RpiBackend/app/json_files/installer_cfg.json")
        with open(cfg_path, "r") as f:
            cfg = json.load(f)
            return str(cfg.get("site id", "default"))
    except Exception as e:
        print(f"[LIVE DATA ERROR] Could not read installer_cfg.json: {e}")
        return "default"

def load_id_map():
    global cached_id_map, id_map_mtime
    map_path = os.path.join(path_config.path_cfg.base_path, "reports_handling", "device_id_map.json")
    if os.path.exists(map_path):
        try:
            current_mtime = os.path.getmtime(map_path)
            if current_mtime != id_map_mtime:
                with open(map_path, 'r') as f:
                    cached_id_map = json.load(f)
                    id_map_mtime = current_mtime
        except Exception as e:
            print(f"[LIVE DATA ERROR] Failed loading device_id_map.json: {e}")
    return cached_id_map

def get_numeric_device_id_str(raw_device_id, id_map):
    raw_id_str = str(raw_device_id).strip()
    if raw_id_str in id_map:
        return str(id_map[raw_id_str])
    if raw_id_str.isdigit():
        return raw_id_str
    parts = raw_id_str.split(":")
    if len(parts) >= 3:
        composite = f"{parts[1]}:{parts[2]}"
        if composite in id_map:
            return str(id_map[composite])
        if parts[2] in id_map:
            return str(id_map[parts[2]])
    return None

def get_ssl_context():
    base_dir = os.path.abspath(path_config.path_cfg.base_path)
    ca_cert = os.path.join(base_dir, "certs", "ca.crt")
    client_cert = os.path.join(base_dir, "certs", "synapse.crt")
    client_key = os.path.join(base_dir, "certs", "synapse.key")

    context = ssl.create_default_context(cafile=ca_cert)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.load_cert_chain(certfile=client_cert, keyfile=client_key)
    return context

def get_channel():
    global persistent_connection, persistent_channel
    if persistent_connection and persistent_channel and persistent_connection.is_open and persistent_channel.is_open:
        return persistent_channel

    cloud_amqp_url = "amqps://enercog:enercog_prod@mqtt.enercog.com:5671/"
    params = pika.URLParameters(cloud_amqp_url)
    params.heartbeat = 60
    params.socket_timeout = 15
    params.credentials = pika.credentials.ExternalCredentials()
    params.ssl_options = pika.SSLOptions(get_ssl_context())

    persistent_connection = pika.BlockingConnection(params)
    persistent_channel = persistent_connection.channel()
    persistent_channel.exchange_declare(exchange='enercog', exchange_type='topic', durable=True)
    return persistent_channel

def livdataHandler(raw_live_power_dict):
    """Maps device IDs and publishes continuously to live-data.{site_id}."""
    global persistent_channel, persistent_connection
    id_map = load_id_map()
    mapped_payload = {}

    for raw_dev_id, power_val in raw_live_power_dict.items():
        num_id_str = get_numeric_device_id_str(raw_dev_id, id_map)
        if num_id_str is not None:
            try:
                mapped_payload[num_id_str] = round(float(power_val), 2)
            except (ValueError, TypeError):
                mapped_payload[num_id_str] = 0.0

    if not mapped_payload:
        return

    site_id = get_site_id()
    publish_routing_key = f"live-data.{site_id}"
    message_body = json.dumps(mapped_payload)

    try:
        channel = get_channel()
        channel.basic_publish(
            exchange='enercog',
            routing_key=publish_routing_key,
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json"
            )
        )
        print(f"[LIVE DATA SENT] -> '{publish_routing_key}': {message_body}")
    except Exception as e:
        print(f"[LIVE DATA PUBLISH ERROR] {e}. Resetting connection.")
        try:
            if persistent_connection and persistent_connection.is_open:
                persistent_connection.close()
        except Exception:
            pass
        persistent_channel = None
        persistent_connection = None







'''import os
import json
import time
import ssl
import pika
import sys

sys.path.insert(0, '../')
import path_config
from control import control_base as ctrl

cached_id_map = {}
id_map_mtime = 0
persistent_channel = None
persistent_connection = None

def get_site_id():
    """Dynamically retrieves the site project code from installer_cfg.json."""
    if hasattr(ctrl, 'site_id') and ctrl.site_id:
        return str(ctrl.site_id)
    try:
        cfg_path = os.path.join(path_config.path_cfg.base_path, "../submodules/RpiBackend/app/json_files/installer_cfg.json")
        with open(cfg_path, "r") as f:
            cfg = json.load(f)
            return str(cfg.get("site id", "default"))
    except Exception as e:
        print(f"[LIVE DATA ERROR] Could not read installer_cfg.json: {e}")
        return "default"

def load_id_map():
    global cached_id_map, id_map_mtime
    map_path = os.path.join(path_config.path_cfg.base_path, "reports_handling", "device_id_map.json")
    if os.path.exists(map_path):
        try:
            current_mtime = os.path.getmtime(map_path)
            if current_mtime != id_map_mtime:
                with open(map_path, 'r') as f:
                    cached_id_map = json.load(f)
                    id_map_mtime = current_mtime
        except Exception as e:
            print(f"[LIVE DATA ERROR] Failed loading device_id_map.json: {e}")
    return cached_id_map

def get_numeric_device_id_str(raw_device_id, id_map):
    raw_id_str = str(raw_device_id).strip()
    if raw_id_str in id_map:
        return str(id_map[raw_id_str])
    if raw_id_str.isdigit():
        return raw_id_str
    parts = raw_id_str.split(":")
    if len(parts) >= 3:
        composite = f"{parts[1]}:{parts[2]}"
        if composite in id_map:
            return str(id_map[composite])
        if parts[2] in id_map:
            return str(id_map[parts[2]])
    return None

def build_live_payload():
    id_map = load_id_map()
    live_power_raw = ctrl.getLivePower()
    payload = {}

    for raw_dev_id, power_val in live_power_raw.items():
        num_id_str = get_numeric_device_id_str(raw_dev_id, id_map)
        if num_id_str is not None:
            try:
                payload[num_id_str] = round(float(power_val), 2)
            except (ValueError, TypeError):
                payload[num_id_str] = 0.0

    return payload

def get_ssl_context():
    base_dir = os.path.abspath(path_config.path_cfg.base_path)
    ca_cert = os.path.join(base_dir, "certs", "ca.crt")
    client_cert = os.path.join(base_dir, "certs", "synapse.crt")
    client_key = os.path.join(base_dir, "certs", "synapse.key")

    context = ssl.create_default_context(cafile=ca_cert)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.load_cert_chain(certfile=client_cert, keyfile=client_key)
    return context

def get_channel():
    global persistent_connection, persistent_channel
    if persistent_connection and persistent_channel and persistent_connection.is_open and persistent_channel.is_open:
        return persistent_channel

    cloud_amqp_url = "amqps://enercog:enercog_prod@mqtt.enercog.com:5671/"
    params = pika.URLParameters(cloud_amqp_url)
    params.heartbeat = 60
    params.socket_timeout = 15
    params.credentials = pika.credentials.ExternalCredentials()
    params.ssl_options = pika.SSLOptions(get_ssl_context())

    persistent_connection = pika.BlockingConnection(params)
    persistent_channel = persistent_connection.channel()
    persistent_channel.exchange_declare(exchange='enercog', exchange_type='topic', durable=True)
    return persistent_channel

def livdataHandler(raw_live_power_dict):
    """Maps device IDs to numeric strings and publishes continuously to live-data.{site_id}."""
    global persistent_channel, persistent_connection
    id_map = load_id_map()
    mapped_payload = {}

    for raw_dev_id, power_val in raw_live_power_dict.items():
        num_id_str = get_numeric_device_id_str(raw_dev_id, id_map)
        if num_id_str is not None:
            try:
                mapped_payload[num_id_str] = round(float(power_val), 2)
            except (ValueError, TypeError):
                mapped_payload[num_id_str] = 0.0

    if not mapped_payload:
        return

    site_id = get_site_id()
    publish_routing_key = f"live-data.{site_id}"
    message_body = json.dumps(mapped_payload)

    try:
        channel = get_channel()
        channel.basic_publish(
            exchange='enercog',
            routing_key=publish_routing_key,
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json"
            )
        )
        print(f"[LIVE DATA SENT] -> '{publish_routing_key}': {message_body}")
    except Exception as e:
        print(f"[LIVE DATA PUBLISH ERROR] {e}. Resetting connection.")
        try:
            if persistent_connection and persistent_connection.is_open:
                persistent_connection.close()
        except Exception:
            pass
        persistent_channel = None
        persistent_connection = None
'''
