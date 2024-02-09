from pyModbusTCP.client import ModbusClient
import pandas as pd
from datetime import datetime as dt
from time import sleep
import logging


# NEW SCRIPT
class CBMDataExtractor:

    def __init__(self):
        self._server_ip = None
        self._server_port = 502
        self._client = None
        self._connected = False
        
    def set_server_ip(self, server_ip):
        """
        Set the server ip of the PLC to connect to
        """
        self._server_ip = server_ip
        return None
        
    def connect_plc(self):
        """
        Connect to the PLC via the ModbusClient python package
        """
        if self._server_ip is not None:
            self._client = ModbusClient(host = self._server_ip, port = self._server_port)
            self._client.open()
            if self._client.is_open:
                self._connected = True
            else:
                print(f'Failed to connect to PLC at {dt.now()}')
                self._connected = False
            
        return None
    
    def check_connection(self):
        """
        Check connection status to the PLC 
        """
        return self._connected
    
    def close_connection(self):
        """
        Close the connection to the PLC 
        """
        self._client.close()
        self._connected = False
        return None
    
    def extract_sensor_data(self, sensor_number):
        """
        Extract the real-time vibration and temperature data of sensor number i from the PLC using the holding registers (see README)
        """
        if self._connected:
            try:
                _sensor_data_register_address = 5*sensor_number
                _sensor_data_num_registers = 5

                # query raw data from PLC
                _sensor_data = self._client.read_holding_registers(_sensor_data_register_address,
                                                                   _sensor_data_num_registers)

                # vibration and temp data
                _z_vel = _sensor_data[0]/1000           # mm/s
                _z_acc = _sensor_data[1]/1000           # g
                _x_vel = _sensor_data[2]/1000           # mm/s
                _x_acc = _sensor_data[3]/1000           # g
                _temp = _sensor_data[4]/100             # Celsius

                return [sensor_number, dt.now(), _z_vel, _z_acc, _x_vel, _x_acc, _temp]
            
            except:
                # if failed to request data, close the connection to the PLC, and return an empty list
                self._connected = False
                
        return []
            
    def extract_threshold_data(self, sensor_number):
        """
        Extract the vibration threshold data threshold of sensor i from the PLC using the holding registers (see README)
        """
        if self._connected:
            try:
                # query vibration threshold data from PLC
                _threshold_data_register_address = 5180 + 12*sensor_number
                _threshold_data_num_registers = 12
                _vib_thres_data = self._client.read_holding_registers(_threshold_data_register_address,
                                                                     _threshold_data_num_registers)
                # x-vel thresholds
                _x_vel_baseline = _vib_thres_data[0]/1000      # mm/s
                _x_vel_warning = _vib_thres_data[1]/1000       # mm/s
                _x_vel_alarm = _vib_thres_data[2]/1000         # mm/s

                # z-vel thresholds
                _z_vel_baseline = _vib_thres_data[3]/1000      # mm/s
                _z_vel_warning = _vib_thres_data[4]/1000       # mm/s
                _z_vel_alarm = _vib_thres_data[5]/1000         # mm/s

                # x-acc thresholds
                _x_acc_baseline = _vib_thres_data[6]/1000      # g
                _x_acc_warning = _vib_thres_data[7]/1000       # g
                _x_acc_alarm = _vib_thres_data[8]/1000         # g

                # z-acc thesholds
                _z_acc_baseline = _vib_thres_data[9]/1000      # g
                _z_acc_warning = _vib_thres_data[10]/1000      # g
                _z_acc_alarm = _vib_thres_data[11]/1000        # g


                return [_z_vel_baseline, _z_acc_baseline, _x_vel_baseline, _x_acc_baseline,
                              _z_vel_warning, _z_acc_warning, _x_vel_warning, _x_acc_warning,
                              _z_vel_alarm, _z_acc_alarm, _x_vel_alarm, _x_acc_alarm]
            except:
                # if failed to request data, close the connection to the PLC, and return an empty list
                self._connected = False
            
        return []
    
    def extract_temp_warning_data(self, sensor_number):
        """
        Extract the temperature warning data of sensor i from the PLC using the holding register
        """
        if self._connected:
            try:
                # query temp warning data from PLC
                _temp_warning_data_register_address = 7680 + sensor_number
                _temp_warning_data_num_registers = 1
                _temp_warning_data = self._client.read_holding_registers(_temp_warning_data_register_address,
                                                                     _temp_warning_data_num_registers)
                _temp_warning = _temp_warning_data[0]         # Celcius

                return _temp_warning
            
            except:
                # if failed to request data, close the connection to the PLC, and return an empty list
                self._connected = False

        return None
    
    def extract_temp_alarm_data(self, sensor_number):
        """
        Extract the temperature warning data of sensor i from the PLC using the holding register
        """
        if self._connected:
            try:
                # query temp warning data from PLC
                # query temp alarm data from PLC
                _temp_alarm_data_register_address = 7720 + sensor_number
                _temp_alarm_data_num_registers = 1

                _temp_alarm_data = self._client.read_holding_registers(_temp_alarm_data_register_address,
                                                                     _temp_alarm_data_num_registers)


                _temp_alarm = _temp_alarm_data[0]               # Celcius

                return _temp_alarm
            
            except:
                # if failed to request data, close the connection to the PLC, and return an empty list
                self._connected = False
            
        return None
    
    def extract_motor_status(self, sensor_number):
        """
        Extract the status (on/off) of motor attached to sensor i using the PLC holding registers
        """
        if self._connected:
            try:
                # query run flag status from the PLC
                _motor_run_flag_register_address = 240 + sensor_number
                _motor_run_flag_num_registers = 1

                _motor_run_flag_data = self._client.read_holding_registers(_motor_run_flag_register_address,
                                                                          _motor_run_flag_num_registers)

                _motor_run_flag = _motor_run_flag_data[0]

                return _motor_run_flag
            
            except:
                # if failed to request data, close the connection to the PLC, and return an empty list
                self._connected = False
        
        return None
            
            
    def extract_additional_data(self, sensor_number):
        """
        Extract the statistical vibration data of sensor i from the PLC using the holding registers
        """
        if self._connected:
            try:
                # query z-axis peak acc, x-axis peak acc, z-axis peak velocity frequency component, 
                # x-axis peak velocity frequency component, z-axis RMS low acc, x-axis RMS low acc, z-axis kurtosis,
                # x-axis kurtosis, z-axis crest factor, x-axis crest factor

                _additional_vibration_register_address = 6140 + sensor_number*10
                _additional_vibration_num_registers = 10

                _additional_vibration_data = self._client.read_holding_registers(_additional_vibration_register_address,
                                                                                _additional_vibration_num_registers)

                # divide raw data from registers by 1000 to get actual parameter data
                _additional_vibration_data_converted = [x/1000 for x in _additional_vibration_data]
                return _additional_vibration_data_converted
            except:
                # if failed to request data, close the connection to the PLC and return an empty list
                self._connected = False
        
        return []