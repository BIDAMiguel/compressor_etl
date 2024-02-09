from flask import Flask, request, jsonify, make_response, send_file
from apibackend import compressor_extract_class
from datetime import datetime as dt
from config import SERVER, DATABASE

app = Flask(__name__)

db_instance = compressor_extract_class()
db_instance.set_database_credentials(SERVER, DATABASE)

@app.route('/cbmdata/rawdata', methods = ['GET'])
def get_sensor_data(db_instance):
    compressor_ids = request.args.get('compressor_ids')
    sensor_ids = request.args.get('sensor_ids')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    try:
        machine_data = db_instance.query_machine_data(compressor_ids, sensor_ids, start_date, end_date)
        
        machine_data = sorted(machine_data, key = lambda x: x[6])
        response = {}

        for data in machine_data:
            machine_id = data[1]
            sensor_id = data[4]

            if machine_id not in response:
                response[machine_id] = {
                                        'compressorname': data[2],
                                        'loc': data[3],
                                        'sensors': {}
                                       }
            if sensor_id not in response[machine_id]['sensors']:
                response[machine_id]['sensors'][sensor_id] = {
                                                                'asensorname': data[5],
                                                                'data': []
                                                            }
            response[machine_id]['sensors'][sensor_id]["data"].append({
                "timestamp": dt.strftime(data[6], "%Y-%m-%d %H:%M:%S"),
                "z-vel": data[7],
                "z-acc": data[8],
                "x-vel": data[9],
                "x-acc": data[10],
                "temp": data[11],
                "z-vel-baseline": data[12],
                'z-acc-baseline': data[13],
                "x-vel-baseline": data[14],
                "x-acc-baseline": data[15],
                "z-vel-warning": data[16],
                "z-acc-warning": data[17],
                "x-vel-warning": data[18],
                "x-acc-warning": data[19],
                "temp-warning": data[20],
                "z-vel-alarm": data[21],
                "z-acc-alarm": data[22],
                "x-vel-alarm": data[23],
                "x-acc-alarm": data[24],
                "temp-alarm": data[25]
            })

        return jsonify(response)
    except:
        return jsonify({'error': 'error404'})
    
@app.route('/cbmdata/compressorlist', methods = ['GET'])
def get_compressor_table(db_instance):
    try:
        compressor_data = db_instance.query_compressor_data()
        response = {}
        
        for compressor in compressor_data:
            machine_id = compressor[0]
            response[machine_id] = {
                                    'compressorname': compressor[1],
                                    'location': compressor[2]
                                   }
        return jsonify(response)
    except:
        return jsonify({'error': 'error404'})
    
@app.route('/cbmdata/sensorlist', methods = ['GET'])
def get_sensor_table(db_instance):
    try:
        sensor_data = db_instance.query_sensor_data()
        response = {}
        
        for sensor in sensor_data:
            sensor_id = sensor[0]
            response[sensor_id] = {
                                    'sensorName': sensor[2],
                                    'machineID': sensor[1],
                                    'sensorType': sensor[3]
                                }
        return jsonify(response)
        
    except:
        return jsonify({'error': 'error404'})
    
@app.route('/cbmdata/dailydata', methods = ['GET'])
def get_daily_data(db_instance):
    compressor_ids = request.args.get('compressor_ids')
    sensor_ids = request.args.get('sensor_ids')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    try:
        machine_data = db_instance.query_daily_data(compressor_ids, sensor_ids, start_date, end_date)
        machine_data = sorted(machine_data, key = lambda x: x[6])
        response = {}

        for data in machine_data:
            machine_id = data[2]
            sensor_id = data[0]

            if machine_id not in response:
                response[machine_id] = {
                                        'compressorname': data[3],
                                        'sensors': {}
                                       }
            if sensor_id not in response[machine_id]['sensors']:
                response[machine_id]['sensors'][sensor_id] = {
                                                                'asensorname': data[1],
                                                                'data': []
                                                            }
            response[machine_id]['sensors'][sensor_id]["data"].append({
                "date": dt.strftime(dt.strptime(data[4], "%Y-%m-%d"), "%Y-%m-%d"),
                "median z-vel": round(data[6],3),
                "median z-acc": round(data[7],3),
                "median x-vel": round(data[8],3),
                "median x-acc": round(data[9],3),
                "median temp": round(data[5],3)
            })

        return jsonify(response)
    except Exception as e:
        return jsonify({'error': str(e)})
    
@app.route('/cbmdata/addvibdata', methods = ['GET'])
def get_add_vib_data(db_instance):
    compressor_ids = request.args.get('compressor_ids')
    sensor_ids = request.args.get('sensor_ids')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    machine_data = db_instance.query_additional_vibration_data(compressor_ids, sensor_ids, start_date, end_date)
    response = {}


    for data in machine_data:
        machine_id = data[1]
        sensor_id = data[4]

        if machine_id not in response:
            response[machine_id] = {
                                    'compressorname': data[2],
                                    'loc': data[3],
                                    'sensors': {}
                                   }
        if sensor_id not in response[machine_id]['sensors']:
            response[machine_id]['sensors'][sensor_id] = {
                                                            'asensorname': data[5],
                                                            'data': []
                                                        }
        response[machine_id]['sensors'][sensor_id]["data"].append({
            "timestamp": dt.strftime(data[6], "%Y-%m-%d %H:%M:%S"),
            "z-peak-acc": data[7],
            "x-peak-acc": data[8],
            "z-peak-vel": data[9],
            "x-peak-vel": data[10],
            "z-rms-low-acc": data[11],
            "x-rms-low-acc": data[12],
            "z-kurtosis": data[13],
            "x-kurtosis": data[14],
            "z-crest-factor": data[15],
            "x-crest-factor": data[16]
        })
    return jsonify(response)

@app.route('/cbmdata/motorstatus', methods = ['GET'])
def get_motor_run_data(db_instance):
    compressor_ids = request.args.get('compressor_ids')
    sensor_ids = request.args.get('sensor_ids')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    machine_data = db_instance.query_motor_run_data(compressor_ids, sensor_ids, start_date, end_date)

    response = {}

    for data in machine_data:
        machine_id = data[1]
        sensor_id = data[4]

        if machine_id not in response:
            response[machine_id] = {
                                    'compressorname': data[2],
                                    'loc': data[3],
                                    'sensors': {}
                                   }
        if sensor_id not in response[machine_id]['sensors']:
            response[machine_id]['sensors'][sensor_id] = {
                                                            'asensorname': data[5],
                                                            'data': []
                                                        }
        response[machine_id]['sensors'][sensor_id]["data"].append({
            "timestamp": dt.strftime(data[6], "%Y-%m-%d %H:%M:%S"),
            "motor-run-flag": data[7]
        })
    return jsonify(response)


@app.route('/cbmdata/dailydata/latest', methods = ['GET'])
def get_latest_median_data(db_instance):
    
    sensor_ids = request.args.get('sensor_ids')

    try:
        machine_data = db_instance.query_latest_median_data(sensor_ids)
    except:
        return jsonify({'error': 'error404'})
    
    if machine_data:
        response = {}

        for data in machine_data:
            machine_id = data[2]
            sensor_id = data[0]

            if machine_id not in response:
                response[machine_id] = {
                                        'compressorname': data[3],
                                        'sensors': {}
                                       }
            if sensor_id not in response[machine_id]['sensors']:
                response[machine_id]['sensors'][sensor_id] = {
                                                                'asensorname': data[1],
                                                                'data': []
                                                            }
            response[machine_id]['sensors'][sensor_id]["data"].append({
                "date": dt.strftime(dt.strptime(data[4], "%Y-%m-%d"), "%Y-%m-%d"),
                "median z-vel": round(data[6],3),
                "median z-acc": round(data[7],3),
                "median x-vel": round(data[8],3),
                "median x-acc": round(data[9],3),
                "median temp": round(data[5],3)
            })
            
        return jsonify(response)
    else:
        return jsonify({'error': 'error404'})
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)