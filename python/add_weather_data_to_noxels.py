# Time regularizes temperature data and adds
# to noxels. Need to refactor to work on all
# weather data, not just temperature.

import warnings
import pandas as pd
import numpy as np
from multiprocessing import Pool
import config


def try_except(row):
    '''Try except block to be used within parallel lambda function'''
    try:
        return regularized_temp_data.loc[(row['time'], row['nearest_station_name']), 'VALUE']
    except:
        return np.nan


def add_data(noxels):
    '''Uses try except to add weather data to noxel from regularized
    weather data'''
    noxels[config.weather_data_type] = noxels.apply(lambda row: try_except(row), axis=1)
    return noxels


def regularize(group):
    '''Takes a block of weather data from parallelized group 
    by, return time frequency regularized version of data'''
    group = group.loc[~group.index.duplicated()]
    group = group.resample('min')
    group = group.interpolate(method='linear')
    group = group.resample(config.data_frequency)
    group = group.interpolate(method='linear')
    return group


def group_data(temp_data_split):
    '''Called by parallelize, takes block of lines from
    dataframe, groups by station id and send to 
    regularization function'''
    grouped_data = temp_data_split.groupby('STATION_ID')
    return grouped_data.apply(regularize)


def parallelize(input_data, func, n_threads):
    '''Parallelizes a function, takes a data frame
    and splits it up over n threads. Concatenates and
    retuns resulting dataframes'''
    input_data_split = np.array_split(input_data, config.n_threads)
    pool = Pool(n_threads)
    result = pd.concat(pool.map(func, input_data_split))
    pool.close()
    pool.join()
    return result


# Annoying, but here goes...
warnings.simplefilter(action='ignore', category=FutureWarning)

# Read an format empty noxels from file
noxels = pd.read_csv(config.empty_noxels)
noxels['time'] = pd.to_datetime(noxels['time'])

# Read in weather data
temp_data = pd.read_csv(
    f'{config.weather_data_base_filename}{config.weather_data_type}{config.weather_data_filname_end}',
    parse_dates=['OBS_DATE'],
    usecols=["STATION_ID", "OBS_DATE", "VALUE"], index_col="OBS_DATE")

# Regularize and format temp. data
regularized_temp_data = parallelize(temp_data, group_data, config.n_threads)

regularized_temp_data['STATION_ID'] = regularized_temp_data.index.get_level_values(0)
regularized_temp_data = regularized_temp_data.reset_index(level=0, drop=True)
regularized_temp_data = regularized_temp_data.set_index(['STATION_ID'], append=True)
regularized_temp_data['VALUE'].replace('', np.nan, inplace=True)
regularized_temp_data.dropna(subset=['VALUE'], inplace=True)
regularized_temp_data = regularized_temp_data.loc[~regularized_temp_data.index.duplicated()]

# Add temp. data to noxels and write to file
noxels = parallelize(noxels, add_data, config.n_threads)
noxels = noxels.dropna()
noxels.to_csv(config.training_data, index=False)