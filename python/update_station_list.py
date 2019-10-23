# Queries the CDEC server to get current list
# of stations reporting hourly and daily. Stores
# as single csv file 'complete_CDEC_station_list.csv'

import csv
import logging
import urllib.request
from bs4 import BeautifulSoup

logging.basicConfig(
    filename = 'logs/update_station_list.log', level = logging.DEBUG)


def get_html_from_url(url):
    """Take url and return html"""
    try:
        with urllib.request.urlopen(url) as response:
            html = response.read()

    except (SystemExit, KeyboardInterrupt):
        logging.warning(" Url request failed")

    else:
        return html


def split_list(long_list, chunk_size):
    """Yield successive chunks from a list."""
    for i in range(0, len(long_list), chunk_size):
        yield long_list[i:i + chunk_size]


def get_html_table_text(html, tag):
    """Take html and tag type, return values of matching tags as list"""
    soup = BeautifulSoup(html, 'html.parser')
    tags = soup.find_all([tag])
    tag_text = [tag.get_text().strip() for tag in tags]
    return tag_text


def main():
    """Retreive and save station data"""
    station_list = open("data/CDEC_weather_station_data/complete_CDEC_station_list.csv", "w", newline = "")
    station_list.write(
        "Station,ID,Elev_(feet),Latitude,Longitude,County,River_Basin\n")

    urls = ["http://cdec.water.ca.gov/misc/dailyStations.html",
            "http://cdec.water.ca.gov/misc/realStations.html"]

    for url in urls:

        html = get_html_from_url(url)
        tag_text = get_html_table_text(html, "td")
        stations = split_list(tag_text, 7)
        print(type(stations))
        writer = csv.writer(station_list)
        writer.writerows(stations)

    station_list.close()


if __name__ == "__main__":
    main()
