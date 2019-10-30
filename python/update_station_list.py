# Queries the CDEC server to get current list
# of stations reporting hourly and daily. Stores
# as single csv file 'complete_CDEC_station_list.csv'

import csv
import logging
import urllib.request
from bs4 import BeautifulSoup
import config

logging.basicConfig(
    filename=config.station_update_log, level=logging.DEBUG)


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
    station_list = open(config.complete_CDEC_station_list, "w", newline = "")
    station_list.write(config.complete_CDEC_station_list_header)

    urls = [config.daily_reporting_CDEC_station_url, config.hourly_reporting_CDEC_station_url]

    for url in urls:

        html = get_html_from_url(url)
        tag_text = get_html_table_text(html, "td")
        stations = split_list(tag_text, 7)
        writer = csv.writer(station_list)
        writer.writerows(stations)

    station_list.close()


if __name__ == "__main__":
    main()
