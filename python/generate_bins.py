lat_start = 32.52
lon_start = -124.48
lat_end = 42.0
lon_end = -114.131
bin_width = 0.1
lat_coord = lat_start + bin_width
lon_coord = lon_start + bin_width

with open("./data/spatial_data/bins.csv","w") as output:
    output.write("lat,long\n")

    while lat_coord < lat_end:
            lon_coord = lon_start + bin_width

            while lon_coord < lon_end:
                output.write(f"{round(lat_coord, 2)},{round(lon_coord, 2)}\n")
                lon_coord += bin_width

            lat_coord += bin_width

output.close()
