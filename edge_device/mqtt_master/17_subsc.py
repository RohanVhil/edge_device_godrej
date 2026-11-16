import os
import json
import time
import ssl
import pika
import sys
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, '../')
import path_config
from control import control_base as ctrl

def get_site_id():
    if hasattr(ctrl, 'site_id') and ctrl.site_id:
        return str(ctrl.site_id)
    try:
        cfg_path = os.path.join(path_config.path_cfg.base_path, "../submodules/RpiBackend/app/json_files/installer_cfg.json")
        with open(cfg_path, "r") as f:
            cfg = json.load(f)
            return str(cfg.get("site id", "default"))
    except Exception as e:
        print(f"[SUBSCRIBER ERROR] Could not read installer_cfg.json: {e}")
        return "default"

def call_start_live_data_api(project_code):
    """Triggers the backend API on firmware startup to request live data streaming."""
    url = f"https://api.enercog.com/api/v1/projects/{project_code}/start-live-data"
    print(f"[LIVE DATA API] Triggering startup API: {url}")
    try:
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        response = session.post(url=url, json={}, verify=False, timeout=10)
        print(f"[LIVE DATA API] Response status: {response.status_code}, body: {response.text}")
    except Exception as e:
        print(f"[LIVE DATA API ERROR] Failed calling startup API: {e}")

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

def start_subscriber():
    cloud_amqp_url = "amqps://enercog:enercog_prod@mqtt.enercog.com:5671/"
    exchange_name = "enercog"

    # Trigger backend API at initial startup
    site_id = get_site_id()
    threading.Thread(target=call_start_live_data_api, args=(site_id,), daemon=True).start()

    while True:
        try:
            site_id = get_site_id()
            listen_routing_key = f"command.{site_id}"
            listen_queue = f"command.{site_id}"

            print(f"[SUBSCRIBER] Connecting to Cloud RabbitMQ for site '{site_id}'...")
            params = pika.URLParameters(cloud_amqp_url)
            params.heartbeat = 60
            params.socket_timeout = 15
            params.credentials = pika.credentials.ExternalCredentials()
            params.ssl_options = pika.SSLOptions(get_ssl_context())

            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)
            channel.queue_declare(queue=listen_queue, durable=True, arguments={"x-message-ttl": 60000})
            channel.queue_bind(exchange=exchange_name, queue=listen_queue, routing_key=listen_routing_key)

            print(f"[SUBSCRIBER] Listening for commands on '{listen_routing_key}'...")

            def callback(ch, method, properties, body):
                try:
                    msg_str = body.decode('utf-8').strip()
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    print(f"[COMMAND RECEIVED] {msg_str}")

                    is_start = False
                    is_stop = False

                    # 1. Plain-text commands
                    if msg_str in ("LIVE_DATA_START", "START_LIVE_DATA"):
                        is_start = True
                    elif msg_str in ("LIVE_DATA_STOP", "STOP_LIVE_DATA"):
                        is_stop = True
                    else:
                        # 2. JSON structured command from backend
                        try:
                            cmd_json = json.loads(msg_str)
                            if isinstance(cmd_json, dict):
                                cmd_type = str(cmd_json.get("command_type", "")).upper()
                                if cmd_type in ("START_LIVE_DATA", "LIVE_DATA_START"):
                                    is_start = True
                                elif cmd_type in ("STOP_LIVE_DATA", "LIVE_DATA_STOP"):
                                    is_stop = True

                                if "project_code" in cmd_json and cmd_json["project_code"]:
                                    ctrl.site_id = cmd_json["project_code"]

                                if "mode" in cmd_json or "param" in cmd_json:
                                    ctrl.setParameter(cmd_json)
                        except (json.JSONDecodeError, ValueError):
                            pass

                    # 3. Update streaming state in control_base
                    if is_start:
                        print("[COMMAND] LIVE_DATA_START accepted -> enabling continuous streaming.")
                        ctrl.startLiveData()
                    elif is_stop:
                        print("[COMMAND] LIVE_DATA_STOP accepted -> stopping streaming.")
                        ctrl.stopLiveData()

                except Exception as err:
                    print(f"[SUBSCRIBER CALLBACK ERROR] {err}")

            channel.basic_consume(queue=listen_queue, on_message_callback=callback)
            channel.start_consuming()

        except (pika.exceptions.AMQPConnectionError, ConnectionRefusedError) as e:
            print(f"[SUBSCRIBER] Connection lost ({e}). Reconnecting in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"[SUBSCRIBER] Unexpected error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)

if __name__ == '__main__':
    path_config.path_cfg = path_config.pathConfig()
    start_subscriber()







'''import os
import json
import time
import ssl
import pika
import sys

sys.path.insert(0, '../')
import path_config
from control import control_base as ctrl

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
        print(f"[SUBSCRIBER ERROR] Could not read installer_cfg.json: {e}")
        return "default"

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

def start_subscriber():
    cloud_amqp_url = "amqps://enercog:enercog_prod@mqtt.enercog.com:5671/"
    exchange_name = "enercog"

    while True:
        try:
            site_id = get_site_id()
            listen_routing_key = f"command.{site_id}"
            listen_queue = f"command.{site_id}"

            print(f"[SUBSCRIBER] Connecting to Cloud RabbitMQ for site '{site_id}'...")
            params = pika.URLParameters(cloud_amqp_url)
            params.heartbeat = 60
            params.socket_timeout = 15
            params.credentials = pika.credentials.ExternalCredentials()
            params.ssl_options = pika.SSLOptions(get_ssl_context())

            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)
            channel.queue_declare(queue=listen_queue, durable=True, arguments={"x-message-ttl": 60000})
            channel.queue_bind(exchange=exchange_name, queue=listen_queue, routing_key=listen_routing_key)

            print(f"[SUBSCRIBER] Listening for commands on '{listen_routing_key}'...")

            def callback(ch, method, properties, body):
                try:
                    msg_str = body.decode('utf-8').strip()
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    print(f"[COMMAND RECEIVED] {msg_str}")

                    is_start = False
                    is_stop = False

                    # 1. Check raw plain text format
                    if msg_str in ("LIVE_DATA_START", "START_LIVE_DATA"):
                        is_start = True
                    elif msg_str in ("LIVE_DATA_STOP", "STOP_LIVE_DATA"):
                        is_stop = True
                    else:
                        # 2. Check JSON payload format from backend
                        try:
                            cmd_json = json.loads(msg_str)
                            if isinstance(cmd_json, dict):
                                cmd_type = str(cmd_json.get("command_type", "")).upper()
                                if cmd_type in ("START_LIVE_DATA", "LIVE_DATA_START"):
                                    is_start = True
                                elif cmd_type in ("STOP_LIVE_DATA", "LIVE_DATA_STOP"):
                                    is_stop = True
                                
                                # Update site_id if project_code is provided in payload
                                if "project_code" in cmd_json and cmd_json["project_code"]:
                                    ctrl.site_id = cmd_json["project_code"]

                                if "mode" in cmd_json or "param" in cmd_json:
                                    ctrl.setParameter(cmd_json)
                        except (json.JSONDecodeError, ValueError):
                            pass

                    # 3. Trigger live data state in control_base
                    if is_start:
                        print("[COMMAND] Triggered -> ctrl.startLiveData()")
                        ctrl.startLiveData()
                    elif is_stop:
                        print("[COMMAND] Triggered -> ctrl.stopLiveData()")
                        ctrl.stopLiveData()

                except Exception as err:
                    print(f"[SUBSCRIBER CALLBACK ERROR] {err}")

            channel.basic_consume(queue=listen_queue, on_message_callback=callback)
            channel.start_consuming()

        except (pika.exceptions.AMQPConnectionError, ConnectionRefusedError) as e:
            print(f"[SUBSCRIBER] Connection lost ({e}). Reconnecting in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"[SUBSCRIBER] Unexpected error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)

if __name__ == '__main__':
    path_config.path_cfg = path_config.pathConfig()
    start_subscriber()
'''
