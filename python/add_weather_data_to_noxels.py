import pandas as pd
import numpy as np
from multiprocessing import Pool

# Annoying, but here goes...
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

noxels = pd.read_csv('./data/training_data/noxels_1day.csv')
noxels['time'] = pd.to_datetime(noxels['time'])

temp_data = pd.read_csv('./data/weather_data/WIND_SP_1day.csv', parse_dates=[
                        'OBS_DATE'], usecols=["STATION_ID", "OBS_DATE", "VALUE"], index_col="OBS_DATE")
temp_data = temp_data[temp_data.VALUE != '---']

n_threads = 14
print(noxels.info())
print(temp_data.head())


def regularize(group):
    group = group.loc[~group.index.duplicated()]
    group = group.resample('min')
    group = group.interpolate(method='linear')
    group = group.resample('H')
    group = group.interpolate(method='linear')
    return group


def group_data(temp_data_split):
    grouped_data = temp_data_split.groupby('STATION_ID')
    return grouped_data.apply(regularize)


def parallelize(temp_data, func, n_cores=n_threads):
    '''Parallelizes regularization, takes temp data and
    splits up regularization fuction over avalibile threads'''
    temp_data_split = np.array_split(temp_data, n_cores)
    pool = Pool(n_cores)
    result = pd.concat(pool.map(func, temp_data_split))
    pool.close()
    pool.join()
    return result


mt_regularized_temp_data = parallelize(temp_data, group_data)

mt_regularized_temp_data['STATION_ID'] = mt_regularized_temp_data.index.get_level_values(
    0)
mt_regularized_temp_data = mt_regularized_temp_data.reset_index(
    level=0, drop=True)
mt_regularized_temp_data = mt_regularized_temp_data.set_index(
    ['STATION_ID'], append=True)
mt_regularized_temp_data['VALUE'].replace('', np.nan, inplace=True)
mt_regularized_temp_data.dropna(subset=['VALUE'], inplace=True)
mt_regularized_temp_data = mt_regularized_temp_data.loc[~mt_regularized_temp_data.index.duplicated(
)]
mt_regularized_temp_data.head()

n_threads = 14


def try_except(row):
    try:
        return mt_regularized_temp_data.loc[(row['time'], row['nearest_station_name']), 'VALUE']
    except:
        return np.nan


def add_data(noxels):
    noxels['wind_sp'] = noxels.apply(lambda row: try_except(row), axis=1)
    return noxels


def parallelize(noxels, func, n_cores=n_threads):
    '''Parallelizes downsampling, takes list of stations and
    splits up the downsampling fuction over avalibile threads'''
    noxels_split = np.array_split(noxels, n_cores)
    pool = Pool(n_cores)
    result = pd.concat(pool.map(func, noxels_split))
    pool.close()
    pool.join()
    return result


noxels = parallelize(noxels, add_data)

noxels = noxels.dropna()
noxels.to_csv('./data/training_data/noxels_1day.csv', index=False)
