import pika
import pika.credentials
import time
import logging
import ssl
import os
import json

class CloudPublisher:
    def __init__(self, amqp_url, project_code, ca_cert=None, client_cert=None, client_key=None, use_external_auth=False):
        self.amqp_url = amqp_url
        self.project_code = str(project_code)
        self.queue = []
        self.ca_cert = ca_cert
        self.client_cert = client_cert
        self.client_key = client_key
        self.use_external_auth = use_external_auth
        self.is_connected = False

    def get_ssl_context(self) -> ssl.SSLContext:
        base_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        
        ca_cert = self.ca_cert if self.ca_cert and os.path.exists(self.ca_cert) else os.path.join(base_dir, "certs", "ca.crt")
        client_cert = self.client_cert if self.client_cert and os.path.exists(self.client_cert) else os.path.join(base_dir, "certs", "synapse.crt")
        client_key = self.client_key if self.client_key and os.path.exists(self.client_key) else os.path.join(base_dir, "certs", "synapse.key")
        
        if not os.path.exists(ca_cert):
            raise FileNotFoundError(f"CA Certificate missing at: {ca_cert}")
        if not os.path.exists(client_cert):
            raise FileNotFoundError(f"Client Certificate missing at: {client_cert}")
        if not os.path.exists(client_key):
            raise FileNotFoundError(f"Client Key missing at: {client_key}")

        context = ssl.create_default_context(cafile=ca_cert)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.load_cert_chain(certfile=client_cert, keyfile=client_key)
        return context

    def get_routing_key_and_payload(self, batch):
        if isinstance(batch, str):
            routing_key = f"alerts.{self.project_code}"
            binary_payload = batch.encode('utf-8')
            content_type = "application/json"
        else:
            batch_type = type(batch).__name__
            if "Status" in batch_type:
                routing_key = f"status.{self.project_code}"
            elif "Alert" in batch_type:
                routing_key = f"alerts.{self.project_code}"
            else:
                routing_key = f"telemetry.{self.project_code}"
                
            binary_payload = batch.SerializeToString()
            content_type = "application/x-protobuf"

        return routing_key, binary_payload, content_type

    def run(self):
        while True:
            try:
                print(f"Attempting secure connection to Cloud RabbitMQ at: {self.amqp_url}")
                params = pika.URLParameters(self.amqp_url)
                params.heartbeat = 60 
                params.socket_timeout = 15
                params.blocked_connection_timeout = 30

                if getattr(self, 'use_external_auth', False):
                    params.credentials = pika.credentials.ExternalCredentials()

                ssl_context = self.get_ssl_context()
                params.ssl_options = pika.SSLOptions(ssl_context)

                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                
                # 1. Declare Topic Exchange
                channel.exchange_declare(exchange='enercog', exchange_type='topic', durable=True)

                # 2. Enable Publisher Confirm Delivery Mode
                channel.confirm_delivery()

                self.is_connected = True
                print(f"Successfully established secure connection & enabled Confirm Delivery mode for site {self.project_code}.")

                while True:
                    while len(self.queue) > 0:
                        batch = self.queue[0]  # Inspect without popping first
                        routing_key, binary_payload, content_type = self.get_routing_key_and_payload(batch)

                        print(f"\n================ [CLOUD AMQP OUTGOING PAYLOAD: {routing_key}] ================")
                        print(batch)
                        print("========================================================================\n")

                        if len(binary_payload) > 0:
                            try:
                                # 3. Publish with Persistent Delivery Mode and Mandatory Routing
                                channel.basic_publish(
                                    exchange='enercog',
                                    routing_key=routing_key,
                                    body=binary_payload,
                                    properties=pika.BasicProperties(
                                        delivery_mode=pika.DeliveryMode.Persistent,
                                        content_type=content_type
                                    ),
                                    mandatory=True
                                )
                                # Remove from queue only after broker ACKs delivery
                                self.queue.pop(0)
                                batch_name = "JSON_Fault_String" if isinstance(batch, str) else type(batch).__name__
                                print(f"[AMQP ACK CONFIRMED] Dispatched {batch_name} ({content_type}) to '{routing_key}'.")

                            except pika.exceptions.UnroutableError:
                                print(f"[AMQP UNROUTABLE ERROR] Message returned by broker! Check routing key binding for '{routing_key}'. Retrying...")
                                time.sleep(2)
                            except pika.exceptions.NackError:
                                print(f"[AMQP NACK ERROR] Broker rejected message for '{routing_key}'. Retrying...")
                                time.sleep(2)

                    connection.process_data_events(time_limit=0.1)
                    time.sleep(0.1)

            except (pika.exceptions.AMQPConnectionError, ConnectionRefusedError, pika.exceptions.AuthenticationError) as e:
                self.is_connected = False
                print(f"Cloud Connection failed to {self.amqp_url}. Retrying in 5 seconds... ({e})")
                time.sleep(5)
            except Exception as e:
                self.is_connected = False
                logging.error(f"Unexpected Cloud AMQP Error: {e}")
                time.sleep(5)


class LocalPublisher:
    def __init__(self, amqp_url, project_code):
        self.amqp_url = amqp_url
        self.project_code = str(project_code)
        self.queue = []

    def get_routing_key_and_payload(self, batch):
        if isinstance(batch, str):
            routing_key = f"alerts.{self.project_code}"
            binary_payload = batch.encode('utf-8')
            content_type = "application/json"
        else:
            batch_type = type(batch).__name__
            if "Status" in batch_type:
                routing_key = f"status.{self.project_code}"
            elif "Alert" in batch_type:
                routing_key = f"alerts.{self.project_code}"
            else:
                routing_key = f"telemetry.{self.project_code}"
                
            binary_payload = batch.SerializeToString()
            content_type = "application/x-protobuf"

        return routing_key, binary_payload, content_type

    def run(self):
        while True:
            try:
                print(f"Attempting unencrypted connection to Local RabbitMQ at: {self.amqp_url}")
                params = pika.URLParameters(self.amqp_url)
                params.heartbeat = 60 
                params.socket_timeout = 10

                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.exchange_declare(exchange='enercog', exchange_type='topic', durable=True)

                print(f"Successfully established unencrypted connection to Local RabbitMQ for site {self.project_code}.")
                while True:
                    while len(self.queue) > 0:
                        batch = self.queue.pop(0)
                        routing_key, binary_payload, content_type = self.get_routing_key_and_payload(batch)

                        if len(binary_payload) > 0:
                            channel.basic_publish(
                                exchange='enercog',
                                routing_key=routing_key,
                                body=binary_payload,
                                properties=pika.BasicProperties(
                                    delivery_mode=2,
                                    content_type=content_type
                                )
                            )
                            batch_name = "JSON_Fault_String" if isinstance(batch, str) else type(batch).__name__
                            print(f"[LOCAL AMQP SENT] Dispatched {batch_name} ({content_type}) to routing key '{routing_key}'")
                    
                    connection.process_data_events(time_limit=0.1)
                    time.sleep(0.1)
                    
            except (pika.exceptions.AMQPConnectionError, ConnectionRefusedError, pika.exceptions.AuthenticationError) as e:
                print(f"Local Connection failed to {self.amqp_url}. Retrying in 5 seconds... ({e})")
                time.sleep(5)
            except Exception as e:
                logging.error(f"Unexpected Local AMQP Error: {e}")
                time.sleep(5)
