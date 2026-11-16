import os
import threading
import time
import json
import sys
import logging

from getmac import get_mac_address as gma
from modbus_master import modbusmasterapi as mbus
import control.control_base as ctrl
from reports_handling import report_handler as rpthndler
import path_config

from reports_handling.amqp_publisher import CloudPublisher, LocalPublisher
from fault_reporting import FaultProcessor
from database.local_db import LocalDatabaseManager
from mqtt_master import subscribe
from mqtt_master import live_data as livedata

sys.path.insert(0, '../submodules')

install_file: bool = False
send_project_details: bool = False

ram_60s_buffer = []
ram_buffer_lock = threading.Lock()

db_mgr = LocalDatabaseManager()


def getAddrMapFromPartNum(part, addr_map: dict, ctrl_map: dict = {}):
    with open(os.path.join(
        path_config.path_cfg.base_path,
        'modbus_mappings/mappings.json'
    )) as mapfile:
        mbus_maps = json.load(mapfile)
        addr_map['map'] = mbus_maps[part]

    with open(os.path.join(
        path_config.path_cfg.base_path,
        'modbus_mappings/control_registers.json'
    )) as ctrlfile:
        mbus_maps = json.load(ctrlfile)
        ctrl_map['map'] = mbus_maps.get(part, {})


def readDeviceList():
    global install_file, send_project_details
    install_file_path = os.path.join(
        path_config.path_cfg.base_path,
        "../submodules/RpiBackend/app/json_files/installer_cfg.json"
    )
    project_device_path = os.path.join(
        path_config.path_cfg.base_path,
        "../submodules/RpiBackend/app/json_files/project_devices.json"
    )

    if not os.path.exists(project_device_path):
        send_project_details = True
        install_file = False
        return
    else:
        install_file = True

    with open(install_file_path) as installer_file:
        installer_cfg = json.load(installer_file)
    ctrl.site_id = installer_cfg["site id"]
    ctrl.controller_id = str(gma())

    for device in installer_cfg["device_list"]:
        device_id = device['device_id']
        num_phases = eval(device['num_phases'])
        phases = [int(s) for s in device.get("phases", "1,2,3").split(',')]

        if device['comm_type'] == 'modbus-tcp':
            devicetype = ctrl.deviceType_l2e[device['device_type']]
            tcp_details = device['modbus_tcp_details']
            read_map, ctrl_map = {}, {}
            getAddrMapFromPartNum(device["part_num"], read_map, ctrl_map)
            slave_id = eval(tcp_details.get('slave_id', '1'))
            ctrl.device_list.append(mbus.modbusTCPDevice(
                devicetype,
                ctrl.commType.modbus_tcp,
                tcp_details['IP'],
                port=eval(tcp_details['port']),
                slave_id=slave_id,
                address_map=read_map,
                ctrl_map=ctrl_map,
                cfg=device
            ))

        elif device['comm_type'] == 'modbus-rtu':
            devicetype = ctrl.deviceType_l2e[device['device_type']]
            rtu_details = device['modbus_rtu_details']
            read_map, ctrl_map = {}, {}
            getAddrMapFromPartNum(device["part_num"], read_map, ctrl_map)
            ctrl.device_list.append(mbus.modbusRTUDevice(
                devicetype,
                ctrl.commType.modbus_rtu,
                read_map,
                ctrl_map,
                rtu_details['port'],
                rtu_details['parity'],
                eval(rtu_details['stop_bits']),
                eval(rtu_details['baudrate']),
                eval(rtu_details['slave_id']),
                cfg=device
            ))

        elif device['comm_type'] == 'none':
            devicetype = ctrl.deviceType_l2e.get(
                device['device_type'],
                ctrl.deviceType.DG
            )
            ctrl.device_list.append(ctrl.systemDevice(
                devicetype=devicetype,
                commtype=ctrl.commType.none,
                cfg=device
            ))

        ctrl.device_list[-1].device_id = device_id
        ctrl.device_list[-1].num_phases = num_phases
        ctrl.device_list[-1].phase = phases
        ctrl.device_list[-1].createMeasureRegisterMap()
        ctrl.device_list[-1].createControlRegisterMap()

        if "rated_power" in device:
            ctrl.device_list[-1].rated_power = eval(str(device["rated_power"]))
        if "connected_to" in device:
            ctrl.device_list[-1].connected_to = ctrl.deviceType_l2e[
                device["connected_to"]
            ]
        if "minimum_limit" in device:
            ctrl.device_list[-1].minimum_limit = eval(
                str(device["minimum_limit"])
            )


def batch_to_disk_loop():
    db_mgr.init_db()
    while True:
        time.sleep(60)
        try:
            with ram_buffer_lock:
                if not ram_60s_buffer:
                    continue
                records_to_flush = list(ram_60s_buffer)
                ram_60s_buffer.clear()

            db_mgr.insert_raw_dict_batch(records_to_flush)
            print(
                f"[SD STORAGE] Flushed {len(records_to_flush)} "
                f"1s metric samples to local database."
            )
        except Exception as e:
            print(f"[STORAGE ERROR] Failed flushing telemetry to SD card: {e}")


def getData(live_1s_pub: LocalPublisher):
    global install_file
    while not os.path.exists(path_config.path_cfg.base_path + "devices.json"):
        install_file = False
        time.sleep(2)

    install_file = True
    print("[SYSTEM] devices.json found. Starting Main Polling & Control Loop...")
    live_1s_pub.routing_key = f"telemetry.{ctrl.site_id}"

    last_live_send_time = 0

    while install_file:
        start_time = time.time()
        current_ts = int(start_time)

        for device in ctrl.device_list:
            try:
                if device.comm_type in (
                    ctrl.commType.modbus_tcp,
                    ctrl.commType.modbus_rtu
                ):
                    raw_data = mbus.getModbusData(device)
                    device.decodeData(raw_data)
            except Exception as e:
                print(f"[POLLING ERROR] Failed parsing {device.device_id}: {e}")

        try:
            #ctrl.runSysControlLoop()

            raw_aggregated_data = ctrl.getAllData()

            live_batch = rpthndler.data_handler._build_payload_batch(
                raw_aggregated_data,
                current_ts
            )
            live_1s_pub.queue.append(live_batch)

            with ram_buffer_lock:
                for dev_id, dev_data in raw_aggregated_data.items():
                    if isinstance(dev_data, dict):
                        for metric, val in dev_data.items():
                            ram_60s_buffer.append((
                                current_ts,
                                str(dev_id),
                                str(metric),
                                val
                            ))

            if getattr(ctrl.system_operating_details, 'live_data', False):
                if time.time() - last_live_send_time >= 1.0:
                    livedata.livdataHandler(ctrl.getLivePower())
                    last_live_send_time = time.time()

                    '''except Exception as e:
                           print(f"[CONTROL LOOP ERROR]: {e}")'''
        
        except Exception as e:
            logging.exception("[CONTROL LOOP ERROR]")

        elapsed_time = time.time() - start_time
        time.sleep(max(0, 1.0 - elapsed_time))


def run_with_restart(target, name, *args):
    while True:
        try:
            target(*args)
        except Exception as e:
            logging.error(f"Thread {name} crashed: {e}", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    path_config.path_cfg = path_config.pathConfig()

    while not install_file:
        readDeviceList()
        time.sleep(1)

    rpthndler.data_handler = rpthndler.dataBank()

    certs_dir = os.path.join(
        os.path.abspath(path_config.path_cfg.base_path),
        "certs"
    )
    cloud_amqp_url = "amqps://enercog:enercog_prod@mqtt.enercog.com:5671/"

    main_cloud_pub = CloudPublisher(
        amqp_url=cloud_amqp_url,
        project_code=str(ctrl.site_id),
        ca_cert=os.path.join(certs_dir, "ca.crt"),
        client_cert=os.path.join(certs_dir, "synapse.crt"),
        client_key=os.path.join(certs_dir, "synapse.key"),
        use_external_auth=True
    )

    rpthndler.data_handler.publisher = main_cloud_pub
    fault_processor = FaultProcessor(
        poll_interval=10,
        publisher=main_cloud_pub
    )
    live_1s_pub = LocalPublisher(
        "amqp://rohan:rohan@192.168.1.112:5672/",
        str(ctrl.site_id)
    )

    threading.Thread(
        target=batch_to_disk_loop,
        daemon=True,
        name="BatchToDisk"
    ).start()
    threading.Thread(
        target=main_cloud_pub.run,
        daemon=True,
        name="CloudPublisher"
    ).start()

    t1 = threading.Thread(
        target=run_with_restart,
        args=(getData, "getData", live_1s_pub),
        name="getData"
    )
    t2 = threading.Thread(
        target=run_with_restart,
        args=(rpthndler.data_handler.runDataLoop, "runDataLoop"),
        name="runDataLoop"
    )
    t3 = threading.Thread(
        target=run_with_restart,
        args=(fault_processor.run, "FaultProcessor"),
        name="FaultProcessor"
    )
    tmqtt = threading.Thread(
        target=run_with_restart,
        args=(subscribe.start_subscriber, "MQTT_Subscriber"),
        name="MQTT_Subscriber"
    )

    t1.start()
    t2.start()
    t3.start()
    tmqtt.start()

    t1.join()
    t2.join()
    t3.join()
    tmqtt.join()
