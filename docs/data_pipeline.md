## Wildfire data wrangling notes

### Monitoring stations
Current best practice in the US for wildfire risk assessment is the National Fire Danger Rating System - version 2016 ([NFDRS2016](https://sites.google.com/firenet.gov/nfdrs/the-models)). The NFDRS uses knowledge based models to predict wildfire risk based on the following data inputs:

1. Windspeed
2. Relative humidity
3. Temperature
4. Solar radiation
5. Rainfall
6. Carry-over stick states - can’t find any info. on what this is
7. Vapor pressure deficit - difference between current air moisture content and max air moisture content, depends on temperature
8. Minimum daily temperature
9. Day length
10. Maximum daily temperature
11. Daily rainfall

The first step will be to assemble a list of all monitoring stations in California which report parameters of interest to the [California Data Exchange Center](https://info.water.ca.gov/queryTools.html). There are 606 stations listed as daily reporting and 1607 stations listed as real time reporting. The station data was scraped from the CDEC site using urllib and beautifulsoup then parsed and saved to a csv file.

The station metadata product [staMeta](http://cdec.water.ca.gov/dynamicapp/staMeta) was queried and parsed using beautifulsoup with station IDs to generate a YAML file containing the sensor types for each station. Querying and parsing data from all 2213 stations took about 30 min.

Manually curated list of ‘sensors of interest’, based on NFDRS inputs as follows:

1. 30,TEMP_AV,TADAZZZ,TEMPERATURE_AIR_AVERAGE,DEG_F
2. 31,TEMP_MX,TXDZZZZ,TEMPERATURE_AIR_MAXIMUM,DEG_F
3. 32,TEMP_MN,TNDZZZZ,TEMPERATURE_AIR_MINIMUM,DEG_F
4. 11,FUEL_MS,MMIZZZZ,FUEL_MOISTURE_WOOD,%
5. 13,FUEL_TP,MTIZZZZ,FUEL_TEMP_WOOD_PROBE,DEG_F
6. 2,RAIN,PCHZZZZ,PRECIPITATION_ACCUMULATED,INCHES
7. 12,REL_HUM,XRIZZZZ,RELATIVE_HUMIDITY,%
8. 4,TEMP,TAHZZZZ,TEMPERATURE_AIR,DEG_F
9. 10,WIND_DR,UDIZZZZ,WIND_DIRECTION,DEG
10. 77,PEAK_WS,UGIZZZZ,WIND_PEAK_GUST,MPH
11. 78,PEAK_WD,UXIZZZZ,WIND_DIRECTION_OF_PEAK_GUST,DEG
12. 9,WIND_SP,USIZZZZ,WIND_SPEED,MPH

Of the 2213 total station 834 (38%) have at least one of the sensors listed above. Downloading one month of data from all stations took about two hours.

Now we have a bit of a problem. The intent is to bin California into a set of 'blocks' by latitude and longitude each of which will be given a wildfire risk prediction by the model. However, most of these block will not actualy contain a monitoring station. The hypothetical fix will be to feed the model a value and a distance for each data type at training time. This means we need find the nearest station to each block that reports each data type. We can then assign each block a new value, distance pair for prediction as new data comes in.

How to do all of that?

1. Set up grid: start with the min/max latitude and longitude of California (this can/will be pruned later)
2. Quantize latitude and longitude - using a bin width of 0.01 degree results in ~ 1 million bins
3. Round all station locations to the nearest thousandth of a degree. Store for later use. Include sensor types at each staiton.
4. For each grid point, find the nearest station reporting each data type. Store data type and distance for each grid point.
5. Profit?