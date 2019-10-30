# Reads a list of staitons in 'complete_station_list.csv' 
# and queries CDEC to discover what sensors are present. 
# Saves to yaml. Key is station ID, elevation, latitude
# and longitude all concatenated together. Value is list
# of sensor types.

import logging
import urllib.request
import yaml
from bs4 import BeautifulSoup

from config import query_station_sensors_log
from config import station_sensors_list
from config import station_sensors_header
from config import complete_CDEC_station_list
from config import station_info_base_base_url

logging.basicConfig(
    filename=query_station_sensors_log, level=logging.DEBUG)


def get_html_from_url(target_url):
    """Take url and return html"""
    with urllib.request.urlopen(target_url) as response:
        output = response.read()

    return output


def get_html_tags(input_html, tag_types):
    """Take html and tag type, return values of matching tags as list"""
    soup = BeautifulSoup(input_html, 'html.parser')
    tags = soup.find_all([tag_types])
    return tags


def parse_station_info(station):
    """Gets station information from station record and assembles
    dict key for sensor list dict"""
    station_info = station.split(',')
    station_id = station_info[1]
    station_elevation = station_info[2]
    station_latitude = station_info[3]
    station_longitude = station_info[4]
    station_key = f'{station_id}, {station_elevation},{station_latitude}, {station_longitude}'

    # if (len(station_id) == 3) and (station_id.isalpha):
    return(station_id, station_key)


def get_sensor_types(station_key, tags):
    """Get all sensor type associate with a station"""
    station_sensors = {}
    # first sensor number should be on line 27
    station_sensors[station_key] = [tags[27].get_text()]
    # second sensor number should be on line 33
    i = 33

    while i < len(tags):
        sensor_type = tags[i].get_text()

        if sensor_type.isnumeric():
            station_sensors[station_key].append(sensor_type)

        i += 6

    return station_sensors


def main():
    """ Take complete station list and determine which sensor
    types are present at each station"""
    output = open(station_sensors_list, 'w')
    output.write(station_sensors_header)
    output.close()

    station_list = open(complete_CDEC_station_list, 'r')
    next(station_list)
    station_list = list(set(station_list))

    for station in station_list:
        station_id, station_key = parse_station_info(station)
        logging.info(' Getting metadata for %s staton.', station_id)
        url = f'{station_info_base_base_url}{station_id}'

        try:
            html = get_html_from_url(url)

        except (SystemExit, KeyboardInterrupt):
            logging.warning(' Failed to fetch html for station %s', station_id)

        else:
            try:
                tags = get_html_tags(html, ["th", "td"])

            except (SystemExit, KeyboardInterrupt):
                logging.warning(
                    ' Failed to get sensor table tags for station %s', station_id)

            else:
                if len(tags) > 27:
                    if tags[20].get_text() == "Sensor Description":
                        try:
                            station_sensors = get_sensor_types(
                                station_key, tags)

                        except (SystemExit, KeyboardInterrupt):
                            logging.warning(
                                ' Failed to get sensor types for station %s', station_id)

                        else:
                            with open(station_sensors_list, 'a') as output:
                                yaml.dump(station_sensors, output)
                            output.close()


if __name__ == "__main__":
    main()
