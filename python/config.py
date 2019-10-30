# This config file contains varaibles and parameters for the training
# colection data pipeline

# Weather data type
weather_data_type = 'REL_HUM'

# Threads to use for parallel tasks
n_threads = 16

# Geospatial bin parameters
# Lat/long start and end define the
# vertices of the bounding box from which
# the California bins are derived
lat_start = 32.52
long_start = -124.48
lat_end = 42.0
long_end = -114.131

bin_width = 0.1    # gegrees lat/long
bin_precision = 2  # number of decimals to round bin coordinates to

# Date & time parameters
data_date_range_start = '2015-01-01'
data_date_range_end = '2016-01-01'
data_frequency = 'H'

# Datafiles
complete_CDEC_station_list = 'data/CDEC_weather_station_data/complete_CDEC_station_list.csv'
target_sensors_list = 'data/CDEC_weather_station_data/target_sensors.csv'
target_sensors_list_short = './data/CDEC_weather_station_data/target_sensors_short.csv'
station_sensors_list = 'data/CDEC_weather_station_data/station_sensors.yaml'
target_stations_list = 'data/CDEC_weather_station_data/target_stations.csv'
sensor_definitions = './data/CDEC_weather_station_data/sensor_definitions.yaml'

weather_data_base_filename = './data/weather_data/'
weather_data_filname_end = '_1yr.csv'

US_states_shapefile = './data/spatial_data/cb_2018_us_state_500k.shp'
full_geospatial_bin_rectangle = './data/spatial_data/bins.csv'
california_geospatial_bins = './data/spatial_data/california_bins.csv'
#empty_noxels = './data/spatial_data/empty_noxels.csv'
empty_noxels = './data/training_data/noxels.csv'
training_data = './data/training_data/noxels.csv'

# URLs
daily_reporting_CDEC_station_url = 'http://cdec.water.ca.gov/misc/dailyStations.html'
hourly_reporting_CDEC_station_url = 'http://cdec.water.ca.gov/misc/realStations.html'
station_info_base_base_url = 'http://cdec.water.ca.gov/dynamicapp/staMeta?station_id='

weather_data_url_base = 'http://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet?Stations='
weather_data_url_sensor_list = '&SensorNums='
weather_data_url_date_range_start = '&Start='
weather_data_url_date_range_end = 'T23%3A00&End='
weather_data_url_end = 'T23%3A00'

# Header strings
complete_CDEC_station_list_header = 'Station,ID,Elev_(feet),Latitude,Longitude,County,River_Basin\n'
target_stations_header = 'station,elevation,latitude,longitude\n'
station_sensors_header = '# sensors by station\n'
weather_data_header = 'STATION_ID,DURATION,SENSOR_NUMBER,SENSOR_TYPE,DATE_TIME,OBS_DATE,VALUE,DATA_FLAG,UNITS\n'
bin_file_header = 'long,lat\n'
station_dataframe_column_names = ['station', 'elevation', 'lat', 'long']

# Logfiles
station_update_log = 'logs/update_station_list.log'
get_stations_of_intrest_log = 'logs/get_stations_of_intrest.log'
query_station_sensors_log = 'logs/query_station_sensors.log'
get_weather_data_log = './logs/get_weather_data.log'
