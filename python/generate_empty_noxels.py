import pandas as pd
import numpy as np
from multiprocessing import Pool
from scipy import spatial


def apply_to_bins(time_series, bins):
    return bins.assign(time=time_series)


bins = pd.read_csv('./data/spatial_data/california_bins.csv')
stations = pd.read_csv('./data/CDEC_weather_station_data/target_stations.csv')
stations.columns = ['station', 'elevation', 'lat', 'long']
stations = stations[stations.long != 0]
bin_array = np.column_stack([bins['long'], bins['lat']])
station_array = np.column_stack([stations['long'], stations['lat']])

station_tree = spatial.cKDTree(station_array)
dist, indexes = station_tree.query(bin_array)

nearest_station_names = []
for index in indexes:
    nearest_station_names.append(stations.iloc[index, 0])

bins['nearest_station_name'] = nearest_station_names

time_series = pd.date_range("2015-01-01 23:00:00",
                            "2015-01-02 23:00:00", freq="H")
time_series = time_series.to_series()

noxels = pd.concat(time_series.apply(apply_to_bins, args=(bins,)).tolist())
noxels.to_csv('./data/spatial_data/empty_noxels.csv', index=False)
