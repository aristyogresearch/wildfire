bins = []
lat_start = 32.5121
lon_start = -124.6509
lat_end = 42.0126
lon_end = -114.1315
bin_width = 0.01
lat_coord = lat_start + bin_width
lon_coord = lon_start + bin_width

while lat_coord < lat_end:
    lon_coord = lon_start + bin_width

    while lon_coord < lon_end:
        bins.append([round(lat_coord, 2), round(lon_coord, 2)])
        lon_coord += bin_width

    lat_coord += bin_width

# print(bins)
print(len(bins))
