import pandas as pd
import numpy as np
from multiprocessing import Pool
from scipy import spatial
import config


def apply_to_bins(time_series, bins):
    return bins.assign(time=time_series)
    

def main():
    bins = pd.read_csv(config.california_geospatial_bins)
    stations = pd.read_csv(config.target_stations_list)
    stations.columns = config.station_dataframe_column_names
    stations = stations[stations.long != 0]
    bin_array = np.column_stack([bins['long'], bins['lat']])
    station_array = np.column_stack([stations['long'], stations['lat']])

    station_tree = spatial.cKDTree(station_array)
    dist, indexes = station_tree.query(bin_array)

    nearest_station_names = []
    for index in indexes:
        nearest_station_names.append(stations.iloc[index, 0])

    bins['nearest_station_name'] = nearest_station_names

    time_series = pd.date_range(f'{config.data_date_range_start} 23:00:00',
                                f'{config.data_date_range_end} 23:00:00', 
                                freq=config.data_frequency)
    time_series = time_series.to_series()

    noxels = pd.concat(time_series.apply(apply_to_bins, args=(bins,)).tolist())
    noxels.to_csv(config.empty_noxels, index=False)

if __name__ == "__main__":
    main()