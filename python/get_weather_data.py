# Gets weather data from target stations and sensors
# in a date a range. Saves data into separate csv files
# for each sensor type.

import requests
import csv
import logging
import yaml
import config

logging.basicConfig(
    filename=config.get_weather_data_log, level=logging.DEBUG)


def split_list(long_list, chunk_size):
    """Yield successive chunks from a list."""
    for i in range(0, len(long_list), chunk_size):
        yield long_list[i:i + chunk_size]


def get_target_sensor_ids(source_sensors):
    """ Returns ids for target sensors """
    next(source_sensors)
    sensor_ids = []

    for sensor in source_sensors:
        sensor_data = [x.strip() for x in sensor.split(',')]
        sensor_ids.append(sensor_data[0])

    return sensor_ids


def get_station_list(source_stations):
    """ Returns list of source stations """
    next(source_stations)
    station_list = []

    for station in source_stations:
        station = station.rstrip()
        station_info = (station).split(',')
        logging.info(' Station info reads: %s', station)
        station_id = station_info[0]
        station_list.append(station_id)

    return station_list


def main():
    """ Takes list of sensors of intrest and list of target staitons,
    returns weather data """

    # read in sensor we want data from
    source_sensors = open(config.target_sensors_list_short, 'r')
    sensor_ids = get_target_sensor_ids(source_sensors)
    source_sensors.close()

    # read in stations to query
    source_stations = open(config.target_stations_list, 'r')
    station_list = get_station_list(source_stations)
    source_stations.close()

    # load dict. to translate numeric sensor ID to sensor name
    sensor_defs = open(config.sensor_definitions, 'r')
    sensor_defs_dict = yaml.safe_load(sensor_defs)
    sensor_defs.close()

    # loop over each sensor type
    for sensor_id in sensor_ids:
        # get sensor name and setup output file for weather data
        sensor_name = sensor_defs_dict[sensor_id]
        logging.info(' Getting data for sensor %s', sensor_name)
        weather_data = open(f'{config.weather_data_base_filename}{sensor_name}{config.weather_data_filname_end}', 'w')
        weather_data.write(config.weather_data_header)

        # split station list into groups so we dont ask for too much data at once
        station_groups = split_list(station_list, 20)

        # loop over station gropus
        for station_group in station_groups:
            # constuct API call and query CDEC
            logging.info(f' Getting data from station group: {station_group}')
            station_group = (',').join(station_group)
            url = (f'{config.weather_data_url_base}{station_group}{config.weather_data_url_sensor_list}{sensor_id}{config.weather_data_url_date_range_start}{config.data_date_range_start}{config.weather_data_url_date_range_end}{config.data_date_range_end}{config.weather_data_url_end}')
            logging.info(' Querying url: %s', url)

            with requests.get(url, stream=True) as r:
                try:
                    lines = (line.decode('utf-8') for line in r.iter_lines())
                    next(lines)

                    # write weather data to file
                    for row in csv.reader(lines):
                        value = row[6]
                        if value != '---':
                            row_string = ",".join(row)
                            weather_data.write(row_string+"\n")
                except:
                    logging.info(f' Failed to get data for: {station_group}')

        weather_data.close()


if __name__ == "__main__":
    main()
