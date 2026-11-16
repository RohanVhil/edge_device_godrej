import argparse
import json
import os
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pika

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import path_config

path_config.path_cfg = path_config.pathConfig()

# Import this first because the firmware currently has a control/modbus import
# dependency that expects modbusmasterapi to be initialized before control_base.
from modbus_master import modbusmasterapi as mbus  # noqa: F401, E402
from database.local_db import LocalDatabaseManager  # noqa: E402
from reports_handling import report_handler  # noqa: E402
from reports_handling.amqp_publisher import CloudPublisher  # noqa: E402


LOCAL_TIMEZONE = ZoneInfo("Asia/Kolkata")
DEFAULT_AMQP_URL = (
    "amqps://enercog:enercog_prod@mqtt.enercog.com:5671/"
)


def parse_local_datetime(value):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=LOCAL_TIMEZONE)
    return parsed


def open_amqp_connection(publisher):
    params = pika.URLParameters(publisher.amqp_url)
    params.heartbeat = 60
    params.socket_timeout = 15
    params.blocked_connection_timeout = 30

    if publisher.use_external_auth:
        params.credentials = pika.credentials.ExternalCredentials()

    params.ssl_options = pika.SSLOptions(publisher.get_ssl_context())

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(
        exchange="enercog",
        exchange_type="topic",
        durable=True
    )
    channel.confirm_delivery()
    return connection, channel


def save_checkpoint(checkpoint_path, database_path, next_start_ms):
    temporary_path = checkpoint_path + ".tmp"
    checkpoint = {
        "database": os.path.abspath(database_path),
        "next_start_ms": next_start_ms
    }

    with open(temporary_path, "w") as checkpoint_file:
        json.dump(checkpoint, checkpoint_file)
        checkpoint_file.flush()
        os.fsync(checkpoint_file.fileno())

    os.replace(temporary_path, checkpoint_path)


def load_checkpoint(checkpoint_path, database_path, requested_start_ms):
    if not os.path.exists(checkpoint_path):
        return requested_start_ms

    with open(checkpoint_path, "r") as checkpoint_file:
        checkpoint = json.load(checkpoint_file)

    if checkpoint.get("database") != os.path.abspath(database_path):
        raise RuntimeError(
            "Checkpoint belongs to a different database. Remove it or use "
            "a different --checkpoint path."
        )

    return max(requested_start_ms, int(checkpoint["next_start_ms"]))


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild historical 2-minute telemetry batches from SQLite and "
            "publish them using the firmware protobuf and AMQP format."
        )
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--project-code", required=True)
    parser.add_argument("--period", type=int, default=120)
    parser.add_argument(
        "--checkpoint",
        default="historical_telemetry_backfill.checkpoint.json"
    )
    parser.add_argument("--amqp-url", default=DEFAULT_AMQP_URL)
    parser.add_argument("--send", action="store_true")
    args = parser.parse_args()

    database_path = os.path.abspath(args.database)
    if not os.path.isfile(database_path):
        raise FileNotFoundError(database_path)
    if args.period <= 0:
        raise ValueError("--period must be greater than zero")

    start_ms = int(parse_local_datetime(args.start).timestamp() * 1000)
    end_ms = int(parse_local_datetime(args.end).timestamp() * 1000)
    period_ms = args.period * 1000

    if start_ms >= end_ms:
        raise ValueError("--start must be earlier than --end")

    if args.send:
        start_ms = load_checkpoint(
            args.checkpoint,
            database_path,
            start_ms
        )

    # Create read-only helper objects without running their normal constructors.
    # Their constructors initialize the current live monthly database, which a
    # historical backfill must never touch.
    db_mgr = LocalDatabaseManager.__new__(LocalDatabaseManager)
    db_mgr.db_dir = os.path.dirname(database_path)
    db_mgr._dev_cache = {}
    db_mgr._metric_cache = {}
    db_mgr._cache_db_path = None

    batch_builder = report_handler.dataBank.__new__(
        report_handler.dataBank
    )
    batch_builder._cached_id_map = {}
    batch_builder._id_map_mtime = 0

    certs_dir = os.path.join(
        os.path.abspath(path_config.path_cfg.base_path),
        "certs"
    )
    publisher = CloudPublisher(
        amqp_url=args.amqp_url,
        project_code=args.project_code,
        ca_cert=os.path.join(certs_dir, "ca.crt"),
        client_cert=os.path.join(certs_dir, "synapse.crt"),
        client_key=os.path.join(certs_dir, "synapse.key"),
        use_external_auth=True
    )

    connection = None
    channel = None
    built_count = 0
    sent_count = 0
    empty_count = 0
    window_start_ms = start_ms

    try:
        while window_start_ms < end_ms:
            window_end_ms = min(window_start_ms + period_ms, end_ms)
            aggregated = db_mgr.get_5min_aggregated_metrics(
                window_start_ms,
                window_end_ms,
                db_path=database_path
            )

            if not aggregated:
                empty_count += 1
                print(
                    f"[BACKFILL EMPTY] {window_start_ms} to "
                    f"{window_end_ms}"
                )
                window_start_ms = window_end_ms
                continue

            batch = batch_builder._build_telemetry_batch_from_aggregated(
                aggregated,
                window_end_ms
            )
            reading_count = (
                len(batch.component_readings)
                + len(batch.weather_readings)
                + len(batch.subcomponent_readings)
            )

            if reading_count == 0:
                empty_count += 1
                print(
                    f"[BACKFILL UNMAPPED] {window_start_ms} to "
                    f"{window_end_ms}"
                )
                window_start_ms = window_end_ms
                continue

            built_count += 1

            if not args.send:
                print(
                    f"[BACKFILL PREVIEW] {window_start_ms} to "
                    f"{window_end_ms}: {reading_count} readings"
                )
                window_start_ms = window_end_ms
                continue

            while True:
                try:
                    if connection is None or connection.is_closed:
                        connection, channel = open_amqp_connection(publisher)

                    routing_key, payload, content_type = (
                        publisher.get_routing_key_and_payload(batch)
                    )
                    channel.basic_publish(
                        exchange="enercog",
                        routing_key=routing_key,
                        body=payload,
                        properties=pika.BasicProperties(
                            delivery_mode=pika.DeliveryMode.Persistent,
                            content_type=content_type
                        ),
                        mandatory=True
                    )
                    break
                except (
                    pika.exceptions.AMQPError,
                    ConnectionError,
                    OSError
                ) as error:
                    print(
                        f"[BACKFILL RETRY] {error}. Retrying in 5 seconds..."
                    )
                    try:
                        if connection and connection.is_open:
                            connection.close()
                    except Exception:
                        pass
                    connection = None
                    channel = None
                    time.sleep(5)

            sent_count += 1
            save_checkpoint(
                args.checkpoint,
                database_path,
                window_end_ms
            )
            print(
                f"[BACKFILL CONFIRMED] {window_start_ms} to "
                f"{window_end_ms}: {reading_count} readings"
            )
            window_start_ms = window_end_ms
            time.sleep(0.2)
    finally:
        try:
            if connection and connection.is_open:
                connection.close()
        except Exception:
            pass

    mode = "SEND" if args.send else "PREVIEW"
    print(
        f"[BACKFILL {mode} COMPLETE] Built={built_count}, "
        f"Sent={sent_count}, Empty/Unmapped={empty_count}"
    )


if __name__ == "__main__":
    main()
