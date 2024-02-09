from read import CBMDataExtractor
from load import LoadSensorData
from datetime import datetime as dt
from config import SERVER_IP, SENSOR_COUNT, SERVER, DATABASE
import logging
from time import sleep


def main():
    cbm_extractor = CBMDataExtractor()
    cbm_extractor.set_server_ip(SERVER_IP)

    database_load = LoadSensorData()
    database_load.set_database_credentials(SERVER,DATABASE)

    logging.basicConfig(filename = 'applog.log', level=logging.INFO, format = '%(asctime)s-%(levelname)s-%(message)s')

    while True:
        # create a connection to the host PLC and database
        try:
            cbm_extractor.connect_plc() 
            database_load.connect_to_database()

            for i in range(SENSOR_COUNT):
                # check if connected to PLC before requesting data
                if cbm_extractor._connected:
                    # query data from the PLC

                    sensor_data = cbm_extractor.extract_sensor_data(i)
                    thres_data = cbm_extractor.extract_threshold_data(i)
                    temp_warn = cbm_extractor.extract_temp_warning_data(i)
                    temp_alarm = cbm_extractor.extract_temp_alarm_data(i)
                    motor_data = cbm_extractor.extract_motor_status(i)
                    add_vibration_data = cbm_extractor.extract_additional_data(i)

                    raw_data = []
                    raw_data.extend(sensor_data)
                    raw_data.extend(thres_data)
                    raw_data.append(temp_warn)
                    raw_data.append(temp_alarm)
                    raw_data.append(motor_data)
                    raw_data.extend(add_vibration_data)

                    # check if there is connection to the database
                    if database_load._connected:
                        # load the data to the database
                        database_load.load_sensor_data(raw_data)
                    else:
                        # break the extraction procedure when connection to database is lost
                        break

                else:
                    # if the PLC connection is lost, end the data extraction process
                    print(f'Lost connection to PLC at {dt.now()}')
                    break

            if cbm_extractor._connected:
                cbm_extractor.close_connection()
            if database_load._connected:
                database_load.close_connection()

            sleep(300)
            
        except KeyboardInterrupt:
            cbm_extractor.close_connection()
            database_load.close_connection()
            break


if __name__ == '__main__':
    main()