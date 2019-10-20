# Finds stations which have the sensors we are intrested 
# in. Needs list of what sensors are present at each staiton
# in 'station_sensors.yaml' and a list of target sensors 
# in 'target_sensors.csv'. Writes list of station with location
# info. to 'target_stations.csv'

import logging
import yaml

logging.basicConfig(
    filename='logs/get_stations_of_intrest.log', level=logging.DEBUG)


def get_sensor_ids(source_sensors):
    ''' Takes list of sensor types of intrest
    returns a list of just the sensor ids'''
    next(source_sensors)
    target_sensor_ids = []

    for sensor in source_sensors:
        sensor_info = sensor.split(",")
        sensor_id = sensor_info[0]
        target_sensor_ids.append(sensor_id)

    return target_sensor_ids

# Read in list of the sensor types we are intrested in
source_sensors = open("data/CDEC_weather_station_data/target_sensors.csv", "r")
target_sensor_ids = get_sensor_ids(source_sensors)
source_sensors.close()

# Read in dict of which sensors are present at each station
station_sensors = open("data/CDEC_weather_station_data/station_sensors.yaml", "r")
station_sensors_dict = yaml.safe_load(station_sensors)
station_sensors.close()

# Find all stations which have at least one of the target sensor types
output = open("data/CDEC_weather_station_data/target_stations.csv", "w")
output.write("station,elevation,latitude,longitude\n")

for key in station_sensors_dict:
    sensor_list = station_sensors_dict[key]

    if len(set(target_sensor_ids).intersection(sensor_list)) > 0:
        output.write(f"{key}\n")

output.close()