import pandas as pd
from datetime import datetime
import numpy as np

def format_date_time(date_time_string):
    return datetime.strptime(date_time_string,'%Y%m%d %H%M')

weather_data_in = open("weather_data.csv", "r")
next(weather_data_in)

weather_data_out = open("weather_data_time_formatted.csv", "w")
weather_data_out.write("STATION_ID,DURATION,SENSOR_NUMBER,SENSOR_TYPE,DATE_TIME,OBS_DATE,VALUE,DATA_FLAG,UNITS\n")

for line in weather_data_in:
    data = line.rstrip().split(",")
    data[4] = str(format_date_time(data[4]))
    data[5] = str(format_date_time(data[5]))
    weather_data_out.write(str(",".join(data))+"\n")

weather_data_in.close()
weather_data_out.close()
        