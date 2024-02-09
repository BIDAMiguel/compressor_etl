import pyodbc
from io import StringIO
import csv
from datetime import datetime as dt


class compressor_extract_class:

    def __init__(self):
        self._server = None
        self._database = None
        self._connection = None
        self._cursor = None
        self._connected = False
    
    def set_database_credentials(self, server, database):
        self._server = server
        self._database = database

    def connect_to_database(self):
        if self._server is not None and self._database is not None:
            _conn_str = 'DRIVER={SQL Server};SERVER='+self._server+';DATABASE='+self._database+';Trusted_Connection=True'
            self._connection = pyodbc.connect(_conn_str)
            self._cursor = self._conn.cursor()
            self._connected = True
        else:
            self._connected = False
            
        return None
    
    def close_connection(self):
        try:
            self._cursor().close()
            self._connection.close()
            self._connected = False
        except:
            self._connected = False

    def query_machine_data(self, machine_ids, sensor_ids, start_date, end_date):
        
        self.connect_to_database()
        if self._connected:
            query = '''
            SELECT * FROM denormalizedview WHERE measurementid > 1
            '''
            params = []
            
            if machine_ids:
                _machine_ids = machine_ids.split(',')
                query += f''' AND machineID IN ({','.join(['?']*len(_machine_ids))})'''
                params.extend(_machine_ids)

            if sensor_ids:
                _sensor_ids = sensor_ids.split(',')
                query += f''' AND sensorID IN ({','.join(['?']*len(_sensor_ids))})'''
                params.extend(_sensor_ids)
                
            if start_date and end_date:
                _start_date = dt.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                _end_date = dt.strptime(end_date, "%Y-%m-%d %H:%M:%S")
                query += ''' AND time_log BETWEEN ? AND ?'''
                params.append(_start_date)
                params.append(_end_date)
                
            if start_date:
                _start_date = dt.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                query += ''' AND time_log >= ?'''
                params.append(_start_date)

            self._cursor.execute(query, tuple(params))
            data =  self._cursor.fetchall()
            
            self.close_connection()

            return data

    def query_daily_data(self, machine_ids, sensor_ids, start_date, end_date):
       
        self.connect_to_database()
        if self._connected:
            query = '''
            SELECT * FROM dailytable WHERE 1=1
            '''
            params = []
            if machine_ids:
                _machine_ids = machine_ids.split(',')
                query += f''' AND machineID IN ({','.join(['?']*len(_machine_ids))})'''
                params.extend(_machine_ids)
                
            if sensor_ids:
                _sensor_ids = sensor_ids.split(',')
                query += f''' AND sensorID IN ({','.join(['?']*len(_sensor_ids))})'''
                params.extend(_sensor_ids)
                
            if start_date and end_date:
                _start_date = dt.strptime(start_date, "%Y-%m-%d")
                _end_date = dt.strptime(end_date, "%Y-%m-%d")
                query += ''' AND DATE BETWEEN ? AND ?'''
                params.append(_start_date)
                params.append(_end_date)
                
            if start_date:
                _start_date = dt.strptime(start_date, "%Y-%m-%d")
                query += ''' AND DATE >= ?'''
                params.append(_start_date)

            self._cursor.execute(query, tuple(params))
            data =  self._cursor.fetchall()

            self.close_connection()

            return data


    def query_compressor_data(self):

        self.connect_to_database()
        if self._connected:
            query = '''
            SELECT machineID, machineName, machineLoc FROM MachineTable
            '''
            
            self._cursor.execute(query)

            data = self._cursor.fetchall()

            self.close_connection()

            return data

    def query_sensor_data(self):

        self.connect_to_database()
        if self._connected:
            query = '''
            SELECT sensorID, machineID, sensorName, sensorType FROM SensorDataTable
            '''
            
            self._cursor.execute(query)

            data = self._cursor.fetchall()

            self.close_connection()

            return data

    def query_additional_vibration_data(self, machine_ids, sensor_ids, start_date, end_date):
        
        self.connect_to_database()
        if self._connected:
            query = '''
            SELECT measurementID, machineID, machineName, machineLoc, sensorID, sensorName, 
            time_log, ZpeakAcc, XpeakAcc, ZpeakVel, XpeakVel, ZRMSlowAcc,
            XRMSlowAcc, Zkurtosis, Xkurtosis, Zcrestfac, Xcrestfac
            FROM additional_vibration_data WHERE measurementid > 1
            '''
            params = []
            
            if machine_ids:
                _machine_ids = machine_ids.split(',')
                query += f''' AND machineID IN ({','.join(['?']*len(_machine_ids))})'''
                params.extend(_machine_ids)

            if sensor_ids:
                _sensor_ids = sensor_ids.split(',')
                query += f''' AND sensorID IN ({','.join(['?']*len(_sensor_ids))})'''
                params.extend(_sensor_ids)
                
            if start_date:
                _start_date = dt.strptime(start_date, "%Y-%m-%d")
                query += ''' AND time_log >= ?'''
                params.append(_start_date)
                
            if end_date:
                _end_date = dt.strptime(end_date, "%Y-%m-%d")
                query += ''' AND time_log <= ?'''
                params.append(_end_date)

            self._cursor.execute(query, tuple(params))
            data =  self._cursor.fetchall()

            self.close_connection()

            return data


    def query_motor_run_data(self, machine_ids, sensor_ids, start_date, end_date):
        
        self.connect_to_database()

        if self._connected:
            query = '''
            SELECT measurementID, machineID, machineName, machineLoc, sensorID, sensorName, time_log,
            MotorRunFlag FROM additional_vibration_data WHERE measurementid > 1
            '''
            params = []
            
            if machine_ids:
                _machine_ids = machine_ids.split(',')
                query += f''' AND machineID IN ({','.join(['?']*len(_machine_ids))})'''
                params.extend(_machine_ids)

            if sensor_ids:
                _sensor_ids = sensor_ids.split(',')
                query += f''' AND sensorID IN ({','.join(['?']*len(_sensor_ids))})'''
                params.extend(_sensor_ids)
                
            if start_date:
                _start_date = dt.strptime(start_date, "%Y-%m-%d")
                query += ''' AND time_log >= ? '''
                params.append(_start_date)
                
            if end_date:
                _end_date = dt.strptime(end_date, "%Y-%m-%d")
                query += ''' AND time_log <= ?'''
                params.append(_end_date)

            self._cursor.execute(query, tuple(params))
            data =  self._cursor.fetchall()

            self.close_connection()

            return data


    def query_latest_median_data(self, sensor_ids):

        self.connect_to_database()

        if self._connected:
            query = '''
            SELECT * FROM dailytable WHERE date = (SELECT MAX(date) FROM dailytable)
            '''
            params = []
            
            if sensor_ids:
                _sensor_ids = sensor_ids.split(',')
                query += f''' AND sensorID IN ({','.join(['?']*len(_sensor_ids))})'''
                params.extend(_sensor_ids)
                
            query += ''' ORDER BY sensorID '''


            self._cursor.execute(query, tuple(params))

            data =  self._cursor.fetchall()

            self.close_connection()

            return data
        
        