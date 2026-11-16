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
        cfg_path = os.path.join(
            path_config.path_cfg.base_path,
            "../submodules/RpiBackend/app/json_files/installer_cfg.json"
        )
        with open(cfg_path, "r") as f:
            cfg = json.load(f)
            return str(cfg.get("site id", "default"))
    except Exception as e:
        print(f"[SUBSCRIBER ERROR] Could not read installer_cfg.json: {e}")
        return "default"


def call_start_live_data_api(project_code):
    """Request live-data streaming and retry while the network is unavailable."""
    url = f"https://api.enercog.com/api/v1/projects/{project_code}/start-live-data"

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

    while True:
        print(f"[LIVE DATA API] Triggering startup API: {url}")
        try:
            response = session.post(
                url=url,
                json={},
                verify=False,
                timeout=10
            )
            print(
                f"[LIVE DATA API] Response status: {response.status_code}, "
                f"body: {response.text}"
            )

            if 200 <= response.status_code < 300:
                return

            if response.status_code < 500:
                print(
                    "[LIVE DATA API ERROR] Request rejected by backend. "
                    "Startup API will not be retried."
                )
                return
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(
                f"[LIVE DATA API ERROR] Network unavailable: {e}. "
                "Retrying in 5 seconds..."
            )
        except requests.exceptions.RequestException as e:
            print(
                f"[LIVE DATA API ERROR] Request failed: {e}. "
                "Retrying in 5 seconds..."
            )

        time.sleep(5)


def get_ssl_context():
    base_dir = os.path.abspath(path_config.path_cfg.base_path)
    ca_cert = os.path.join(base_dir, "certs", "ca.crt")
    client_cert = os.path.join(base_dir, "certs", "synapse.crt")
    client_key = os.path.join(base_dir, "certs", "synapse.key")

    context = ssl.create_default_context(cafile=ca_cert)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.load_cert_chain(
        certfile=client_cert,
        keyfile=client_key
    )
    return context


def process_command(msg_str):
    is_start = False
    is_stop = False

    if msg_str in ("LIVE_DATA_START", "START_LIVE_DATA"):
        is_start = True
    elif msg_str in ("LIVE_DATA_STOP", "STOP_LIVE_DATA"):
        is_stop = True
    else:
        try:
            cmd_json = json.loads(msg_str)
        except (json.JSONDecodeError, ValueError):
            print("[COMMAND] Unknown non-JSON command ignored.")
            return

        if not isinstance(cmd_json, dict):
            print("[COMMAND] JSON command is not an object and was ignored.")
            return

        cmd_type = str(
            cmd_json.get("command_type", "")
        ).strip().upper()

        if cmd_type in ("START_LIVE_DATA", "LIVE_DATA_START"):
            is_start = True
        elif cmd_type in ("STOP_LIVE_DATA", "LIVE_DATA_STOP"):
            is_stop = True

        if "project_code" in cmd_json and cmd_json["project_code"]:
            ctrl.site_id = cmd_json["project_code"]

        if not is_start and not is_stop:
            if "mode" in cmd_json or "param" in cmd_json:
                ctrl.setParameter(cmd_json)
                print("[COMMAND] Backend control command applied successfully.")
            else:
                print("[COMMAND] Unsupported JSON command ignored.")

    if is_start:
        print(
            "[COMMAND] LIVE_DATA_START accepted -> "
            "enabling continuous streaming."
        )
        ctrl.startLiveData()
    elif is_stop:
        print(
            "[COMMAND] LIVE_DATA_STOP accepted -> "
            "stopping streaming."
        )
        ctrl.stopLiveData()


def start_subscriber():
    cloud_amqp_url = "amqps://enercog:enercog_prod@mqtt.enercog.com:5671/"
    exchange_name = "enercog"

    site_id = get_site_id()
    threading.Thread(
        target=call_start_live_data_api,
        args=(site_id,),
        daemon=True,
        name="StartLiveDataAPI"
    ).start()

    while True:
        connection = None

        try:
            site_id = get_site_id()
            listen_routing_key = f"command.{site_id}"
            listen_queue = f"command.{site_id}"

            print(
                f"[SUBSCRIBER] Connecting to Cloud RabbitMQ "
                f"for site '{site_id}'..."
            )

            params = pika.URLParameters(cloud_amqp_url)
            params.heartbeat = 10
            params.socket_timeout = 10
            params.blocked_connection_timeout = 10
            params.connection_attempts = 1
            params.credentials = pika.credentials.ExternalCredentials()
            params.ssl_options = pika.SSLOptions(get_ssl_context())

            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            channel.exchange_declare(
                exchange=exchange_name,
                exchange_type='topic',
                durable=True
            )
            channel.queue_declare(
                queue=listen_queue,
                durable=True,
                arguments={"x-message-ttl": 60000}
            )
            channel.queue_bind(
                exchange=exchange_name,
                queue=listen_queue,
                routing_key=listen_routing_key
            )
            channel.basic_qos(prefetch_count=1)

            print(
                f"[SUBSCRIBER] Listening for commands on "
                f"'{listen_routing_key}'..."
            )

            def callback(ch, method, properties, body):
                try:
                    msg_str = body.decode('utf-8').strip()
                    print(f"[COMMAND RECEIVED] {msg_str}")

                    process_command(msg_str)

                    ch.basic_ack(
                        delivery_tag=method.delivery_tag
                    )
                except (
                    pika.exceptions.AMQPError,
                    ConnectionError,
                    OSError
                ):
                    raise
                except Exception as err:
                    print(
                        f"[SUBSCRIBER CALLBACK ERROR] {err}. "
                        "Command rejected without requeue."
                    )
                    if ch.is_open:
                        ch.basic_nack(
                            delivery_tag=method.delivery_tag,
                            requeue=False
                        )

            channel.basic_consume(
                queue=listen_queue,
                on_message_callback=callback,
                auto_ack=False
            )
            channel.start_consuming()

        except (
            pika.exceptions.AMQPError,
            ConnectionError,
            OSError
        ) as e:
            print(
                f"[SUBSCRIBER] Connection lost ({e}). "
                "Reconnecting in 5 seconds..."
            )
        except Exception as e:
            print(
                f"[SUBSCRIBER] Unexpected error: {e}. "
                "Reconnecting in 5 seconds..."
            )
        finally:
            try:
                if connection and connection.is_open:
                    connection.close()
            except Exception:
                pass

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
        cfg_path = os.path.join(
            path_config.path_cfg.base_path,
            "../submodules/RpiBackend/app/json_files/installer_cfg.json"
        )
        with open(cfg_path, "r") as f:
            cfg = json.load(f)
            return str(cfg.get("site id", "default"))
    except Exception as e:
        print(f"[SUBSCRIBER ERROR] Could not read installer_cfg.json: {e}")
        return "default"


def call_start_live_data_api(project_code):
    """Trigger the backend API on firmware startup."""
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
        print(
            f"[LIVE DATA API] Response status: {response.status_code}, "
	            f"body: {response.text}"
        )
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

    site_id = get_site_id()
    threading.Thread(
        target=call_start_live_data_api,
        args=(site_id,),
        daemon=True
    ).start()

    while True:
        connection = None
        try:
            site_id = get_site_id()
            listen_routing_key = f"command.{site_id}"
            listen_queue = f"command.{site_id}"

            print(
                f"[SUBSCRIBER] Connecting to Cloud RabbitMQ "
                f"for site '{site_id}'..."
            )
            params = pika.URLParameters(cloud_amqp_url)
            params.heartbeat = 10
            params.socket_timeout = 10
            params.blocked_connection_timeout = 10
            params.connection_attempts = 1
            params.credentials = pika.credentials.ExternalCredentials()
            params.ssl_options = pika.SSLOptions(get_ssl_context())

            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            channel.exchange_declare(
                exchange=exchange_name,
                exchange_type='topic',
                durable=True
            )
            channel.queue_declare(
                queue=listen_queue,
                durable=True,
                arguments={"x-message-ttl": 60000}
            )
            channel.queue_bind(
                exchange=exchange_name,
                queue=listen_queue,
                routing_key=listen_routing_key
            )
            channel.basic_qos(prefetch_count=1)

            print(
                f"[SUBSCRIBER] Listening for commands on "
                f"'{listen_routing_key}'..."
            )

            def callback(ch, method, properties, body):
                try:
                    msg_str = body.decode('utf-8').strip()
                    print(f"[COMMAND RECEIVED] {msg_str}")

                    is_start = False
                    is_stop = False

                    if msg_str in ("LIVE_DATA_START", "START_LIVE_DATA"):
                        is_start = True
                    elif msg_str in ("LIVE_DATA_STOP", "STOP_LIVE_DATA"):
                        is_stop = True
                    else:
                        try:
                            cmd_json = json.loads(msg_str)
                            if isinstance(cmd_json, dict):
                                cmd_type = str(
                                    cmd_json.get("command_type", "")
                                ).upper()
                                if cmd_type in (
                                    "START_LIVE_DATA",
                                    "LIVE_DATA_START"
                                ):
                                    is_start = True
                                elif cmd_type in (
                                    "STOP_LIVE_DATA",
                                    "LIVE_DATA_STOP"
                                ):
                                    is_stop = True

                                if (
                                    "project_code" in cmd_json
                                    and cmd_json["project_code"]
                                ):
                                    ctrl.site_id = cmd_json["project_code"]

                                if "mode" in cmd_json or "param" in cmd_json:
                                    ctrl.setParameter(cmd_json)
                        except (json.JSONDecodeError, ValueError):
                            pass

                    if is_start:
                        print(
                            "[COMMAND] LIVE_DATA_START accepted -> "
                            "enabling continuous streaming."
                        )
                        ctrl.startLiveData()
                    elif is_stop:
                        print(
                            "[COMMAND] LIVE_DATA_STOP accepted -> "
                            "stopping streaming."
                        )
                        ctrl.stopLiveData()

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as err:
                    print(f"[SUBSCRIBER CALLBACK ERROR] {err}")
                    if ch.is_open:
                        ch.basic_nack(
                            delivery_tag=method.delivery_tag,
                            requeue=True
                        )

            channel.basic_consume(
                queue=listen_queue,
                on_message_callback=callback
            )
            channel.start_consuming()

        except (pika.exceptions.AMQPError, ConnectionError, OSError) as e:
            print(
                f"[SUBSCRIBER] Connection lost ({e}). "
                f"Reconnecting in 5 seconds..."
            )
        except Exception as e:
            print(
                f"[SUBSCRIBER] Unexpected error: {e}. "
                f"Reconnecting in 5 seconds..."
            )
        finally:
            try:
                if connection and connection.is_open:
                    connection.close()
            except Exception:
                pass

        time.sleep(5)


if __name__ == '__main__':
    path_config.path_cfg = path_config.pathConfig()
    start_subscriber()
'''
