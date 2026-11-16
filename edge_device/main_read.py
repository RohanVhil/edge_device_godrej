from pymodbus import framer
from pymodbus.client import ModbusTcpClient,ModbusSerialClient
from pymodbus.client import ModbusBaseClient as cltlib

ip='192.168.1.100'
port = 503
slave_id=1

rtu_port = '/dev/pts/8'


tcp_client = ModbusTcpClient(ip,port=port)
rtu_client = ModbusSerialClient(port=rtu_port,framer=framer.FramerType.RTU,baudrate=9600,timeout=3)

print(tcp_client.connect())

read_Data =tcp_client.read_holding_registers(address=0,count=10,slave=1)
print(read_Data.registers)
tcp_client.write_registers(46,1,slave=1)
tcp_client.write_registers(47,105,slave=1)
read_Data =tcp_client.read_holding_registers(address=46,count=10,slave=1)
print(read_Data.registers)
read_Data =tcp_client.read_holding_registers(address=29,count=10,slave=1)
print(read_Data.registers)
read_Data =tcp_client.read_holding_registers(address=5311,count=10,slave=1)
print(read_Data.registers)
