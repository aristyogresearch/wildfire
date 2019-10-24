import pandas as pd
import numpy as np
import geopandas as gpd
from multiprocessing import Pool
from shapely.geometry import MultiPolygon, Polygon, Point

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
    
def parallelize(df, func, n_cores = n_threads):
    '''Parallelizes point picking, takes dataframe of points
    and splits up the cull fuction over avalibile threads'''
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

# Load US states shapefile
gdf = gpd.read_file('./data/spatial_data/cb_2018_us_state_500k.shp')

# Get just California
california = gdf[gdf['NAME'] == 'California']

# Get just the main landmass of California
multipoly = california.loc[16, 'geometry']
california = multipoly[-1]

# Bin parameters
lat_start = 32.52
long_start = -124.48
lat_end = 42.0
long_end = -114.131
bin_width = .1
lat_coord = lat_start + bin_width
long_coord = long_start + bin_width

# Generate bin rectangle
with open("./data/spatial_data/bins.csv","w") as output:
    output.write("long,lat\n")

    while lat_coord < lat_end:
            long_coord = long_start + bin_width

            while long_coord < long_end:
                output.write(f"{round(long_coord, 2)},{round(lat_coord, 2)}\n")
                long_coord += bin_width

            lat_coord += bin_width

output.close()

# Read bins into pandas dataframe
bins = pd.read_csv('./data/spatial_data/bins.csv')

# Set number of worker processes
n_threads = 14

# Empty series to be returned in place of a point
# which is outside of californa
empty = pd.Series([np.nan, np.nan ])
empty.index = ['long', 'lat']

# Discard bins which fall outside of California
keeper_bins = parallelize(bins, cull_points).dropna()
keeper_bins.to_csv('./data/spatial_data/california_bins.csv', index = None, header = True)