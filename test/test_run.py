import makedata

nc_file = 'output/netcdf/2023/0101.nc'
target_lat = 43.5789
target_lon = 144.5288
output_csv = 'test.csv'

makedata.extract_msm_data_to_csv(nc_file, target_lat, target_lon, output_csv)