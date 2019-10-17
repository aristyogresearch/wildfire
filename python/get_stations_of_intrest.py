import logging
import yaml
logging.basicConfig(
    filename='./logs/get_stations_of_intrest.log', level=logging.DEBUG)


def get_sensor_ids(source_sensors):
    next(source_sensors)
    target_sensor_ids = []

    for sensor in source_sensors:
        sensor_info = sensor.split(",")
        sensor_id = sensor_info[0]
        target_sensor_ids.append(sensor_id)

    return target_sensor_ids


source_sensors = open("./data/weather_station_data/target_sensors_short.csv", "r")
target_sensor_ids = get_sensor_ids(source_sensors)
source_sensors.close()

station_sensors = open("./data/weather_station_data/station_sensors.yaml", "r")
station_sensors_dict = yaml.safe_load(station_sensors)
station_sensors.close()

output = open("./data/weather_station_data/target_stations.csv", "w")
output.write("station,elevation,latitude,longitude\n")

for key in station_sensors_dict:
    sensor_list = station_sensors_dict[key]

    if len(set(target_sensor_ids).intersection(sensor_list)) > 0:
        output.write(f"{key}\n")
