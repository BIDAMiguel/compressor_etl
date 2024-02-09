import pyodbc
import logging

class LoadSensorData:
    def __init__(self):
        self._server = None
        self._database = None
        self._conn = None
        self._cursor = None
        self._connected = False
    
    def set_database_credentials(self, server, database):
        self._server = server
        self._database = database
        
        return None
        
    def connect_to_database(self):
        if self._server is not None and self._database is not None:
            _conn_str = 'DRIVER={SQL Server};SERVER='+self._server+';DATABASE='+self._database+';Trusted_Connection=True'
            self._conn = pyodbc.connect(_conn_str)
            self._cursor = self._conn.cursor()
            self._connected = True
        else:
            self._connected = False
            
        return None
        
    def close_connection(self):
        if self._connected:
            # Programatic error if connection is lost, make a try-except block, but ensure that the connection is close
            try:
                self._conn.close()
                self._connected = False
            except:
                self._connected = False

        return None
    
    def load_sensor_data(self, sensor_data):
        # perform data validation check before proceeding
        if len(sensor_data) == 32 and type(sensor_data) == list:              # make sure that sensor_data contains 32 data points
            query = '''
            INSERT INTO machinedatatable (sensorID, time_log, Zvel, Zacc, Xvel, Xacc, Temp, 
            Zvelbase, Zaccbase, Xvelbase, Xaccbase, 
            Zvelwarn, Zaccwarn, Xvelwarn, Xaccwarn,
            Zvelalarm, Zaccalarm, Xvelalarm, Xaccalarm, 
            Tempwarn, Tempalarm, MotorRunFlag, ZpeakAcc, XpeakAcc, ZpeakVel,
            XpeakVel, ZRMSlowAcc, XRMSlowAcc, Zkurtosis, Xkurtosis, Zcrestfac,
            Xcrestfac)
            VALUES (?,?,?,?,?,?,?,?,?,?,
                    ?,?,?,?,?,?,?,?,?,?,
                    ?,?,?,?,?,?,?,?,?,?,
                    ?,?);
            
            '''
            # try-except block to ensure pipeline will not fail, if connection is lost in the middle of operation, just
            # close the connection
            try:
                
                self._cursor.execute(query, tuple(sensor_data))
                self._conn.commit()
                logging.info(f'Successfully loaded sensor {sensor_data[0]} to database with data: {sensor_data}')
                
            except Exception as e:
                
                logging.info(f'Failed to insert data to database due to {str(e)}')
                self._connected = False
                
        else:
            # failed the data validation test!
            print("Data doesn't contain 32 data points")
            
        return None
            
    def create_machine_data(self):
        query = '''
            CREATE TABLE MachineDataTable (
            measurementID INT IDENTITY(1,1) PRIMARY KEY,
            sensorID INT,
            time_log DATETIME,
            Zvel FLOAT,
            Zacc FLOAT,
            Xvel FLOAT,
            Xacc FLOAT,
            Temp FLOAT);
        '''
        self._cursor.execute(query)
        self._conn.commit()
        return None
    
    def create_machine_table(self):
        query = '''
            CREATE TABLE MachineTable (
            machineID INT PRIMARY KEY,
            machineName VARCHAR(50),
            machineLoc VARCHAR(50)
            ); 
        '''
        self._cursor.execute(query)
        self._conn.commit()
        return None
    
    def create_sensor_table(self):
        query = '''
            CREATE TABLE SensorDataTable (
            sensorID INT PRIMARY KEY,
            machineID INT,
            sensorName VARCHAR(20),
            sensorType VARCHAR(20)
            FOREIGN KEY (machineID) REFERENCES MachineTable(machineID)
            );
        '''
        self._cursor.execute(query)
        self._conn.commit()
        return None