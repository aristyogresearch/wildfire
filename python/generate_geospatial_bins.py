import pandas as pd
import numpy as np
import geopandas as gpd
from multiprocessing import Pool
from shapely.geometry import MultiPolygon, Polygon, Point
import config


def point_in_california(point):
    '''Takes point and returns point if in California'''
    coord = Point(point['long'], point['lat'])
    if coord.within(california) == True:
        return point
    else:
        return empty


def cull_points(points):
    '''Takes a dataframe containing long, lat points and 
    uses apply find points which fall in California'''
    keepers = points.apply(point_in_california, axis=1)
    return keepers


def parallelize(df, func, n_threads):
    '''Parallelizes point picking, takes dataframe of points
    and splits up the cull fuction over avalibile threads'''
    df_split = np.array_split(df, n_threads)
    pool = Pool(n_threads)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


# Load US states shapefile
gdf = gpd.read_file(config.US_states_shapefile)

# Get just California
california = gdf[gdf['NAME'] == 'California']

# Get just the main landmass of California
multipoly = california.loc[16, 'geometry']
california = multipoly[-1]

# Bin parameters
lat_coord = config.lat_start + config.bin_width
long_coord = config.long_start + config.bin_width

# Generate bin rectangle
with open(config.full_geospatial_bin_rectangle, 'w') as output:
    output.write(config.bin_file_header)

    while lat_coord < config.lat_end:
        long_coord = config.long_start + config.bin_width

        while long_coord < config.long_end:
            output.write(
                f'{round(long_coord, config.bin_precision)},{round(lat_coord, config.bin_precision)}\n')
            long_coord += config.bin_width

        lat_coord += config.bin_width

output.close()

# Read bins into pandas dataframe
bins = pd.read_csv(config.full_geospatial_bin_rectangle)

# Empty series to be returned in place of a point
# which is outside of californa
empty = pd.Series([np.nan, np.nan])
empty.index = ['long', 'lat']

# Discard bins which fall outside of California
keeper_bins = parallelize(bins, cull_points, config.n_threads).dropna()
keeper_bins.to_csv(config.california_geospatial_bins, index=None, header=True)
