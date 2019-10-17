import pandas as pd
from datetime import datetime
import numpy as np

def format_date_time(date_time_string):
    return datetime.strptime(date_time_string,'%Y%m%d %H%M')

weather_data_in = open("weather_data_time_formatted.csv", "r")
next(weather_data_in)

weather_data_out = open("weather_data_time_formatted_cleaned.csv", "w")
weather_data_out.write("STATION_ID,DURATION,SENSOR_NUMBER,SENSOR_TYPE,OBS_DATE,VALUE,UNITS\n")

for line in weather_data_in:
    if not "---" in line:
        data = line.rstrip().split(",")
        del data[4]
        del data[6]
        weather_data_out.write(str(",".join(data))+"\n")

weather_data_in.close()
weather_data_out.close()
        