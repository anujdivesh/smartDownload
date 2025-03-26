from netCDF4 import Dataset, num2date, date2num
import numpy as np
import datetime
import os

# Set up server and output paths
SERVER = "http://opendap.bom.gov.au:8080/thredds/dodsC/nmoc/oceanmaps_ofam_fc/ops/latest/"
OUTPUT_DIR = "/home/pop/Desktop/pipeline/smart_data/tasks"

class BlueLink:
    @staticmethod
    def Download_Compile_Bluelink_Currents(data_dir, server):
        output_filename = os.path.join(data_dir, "Bluelink_currents_tmp.nc")
        os.makedirs(data_dir, exist_ok=True)

        # User Input Variables
        latStart, latEnd = -75, 75
        lonStart, lonEnd = 110, 290
        start_timestamp, end_timestamp = 0, 144
        num_of_time_steps = len(range(start_timestamp, end_timestamp + 24, 24))
        
        gridcell = 0.1
        variable_name1, variable_name2 = "u", "v"
        time_unit = "hours since 2000-01-01 00:00:00"

        # Determine array cell references
        lonRange = range(int(lonStart/gridcell), int(lonEnd/gridcell) + 1)
        latRange = range(int((75 + latStart)/gridcell), int((75 + latEnd)/gridcell))

        # Find valid data files
        for days_ago in range(1, 4):
            try:
                d = datetime.date.today() - datetime.timedelta(days=days_ago)
                filename = f"ocean_fc_res001_016_{d.year}{d.month:02d}{d.day:02d}12_{start_timestamp * 3:03d}_{variable_name1}.nc"
                filename2 = f"ocean_fc_res001_016_{d.year}{d.month:02d}{d.day:02d}12_{start_timestamp * 3:03d}_{variable_name2}.nc"
                cdf1 = Dataset(server + filename, "r")
                cdf2 = Dataset(server + filename2, "r")
                break
            except OSError:
                continue
        else:
            raise OSError("No valid data found for the last 3 days.")

        # Process data
        for i, x in enumerate(range(start_timestamp, end_timestamp + 24, 24)):
            if x == start_timestamp:
                lat, lon, water_u, water_v, time = BlueLink.extract_all(cdf1, cdf2, lonRange, latRange, variable_name1, variable_name2)
                BlueLink.create_nc(output_filename, len(lonRange), len(latRange), "u", "v", lat, lon, time_unit, gridcell, num_of_time_steps)
                BlueLink.add_nc(output_filename, "u", "v", water_u, water_v, time, 0, time_unit)
            else:
                filename = f"ocean_fc_res001_016_{d.year}{d.month:02d}{d.day:02d}12_{x:03d}_{variable_name1}.nc"
                filename2 = f"ocean_fc_res001_016_{d.year}{d.month:02d}{d.day:02d}12_{x:03d}_{variable_name2}.nc"
                try:
                    cdf1 = Dataset(server + filename, "r")
                    cdf2 = Dataset(server + filename2, "r")
                    water_u, water_v, time = BlueLink.extract_data(cdf1, cdf2, lonRange, latRange, variable_name1, variable_name2)
                    BlueLink.add_nc(output_filename, "u", "v", water_u, water_v, time, i, time_unit)
                except OSError:
                    continue

        # Create subset
        full_file = os.path.join(data_dir, "Bluelink_currents_tmp.nc")
        subset_file = os.path.join(data_dir, "Bluelink_currents.nc")
        BlueLink.subset_nc_file(full_file, subset_file, 100, 300, -45, 45)

    @staticmethod
    def extract_all(cdf1, cdf2, lonRange, latRange, nc_data_name1, nc_data_name2):
        lat = np.squeeze(cdf1.variables["yu_ocean"][latRange])
        lon = np.squeeze(cdf1.variables["xu_ocean"][lonRange])
        data = cdf1.variables[nc_data_name1][0, 0, latRange, lonRange]
        data2 = cdf2.variables[nc_data_name2][0, 0, latRange, lonRange]
        time = cdf1.variables["Time"][0]
        time_stamp = num2date(time, units=cdf1.variables["Time"].units, calendar="gregorian")
        cdf1.close()
        cdf2.close()
        return lat, lon, data, data2, time_stamp

    @staticmethod
    def extract_data(cdf1, cdf2, lonRange, latRange, nc_data_name1, nc_data_name2):
        data = cdf1.variables[nc_data_name1][0, 0, latRange, lonRange]
        data2 = cdf2.variables[nc_data_name2][0, 0, latRange, lonRange]
        time = cdf1.variables["Time"][0]
        time_stamp = num2date(time, units=cdf1.variables["Time"].units, calendar="gregorian")
        cdf1.close()
        cdf2.close()
        return data, data2, time_stamp

    @staticmethod
    def create_nc(output_filename, lon_len, lat_len, var_name1, var_name2, lat_array, lon_array, time_unit, gridcell, num_time_steps):
        with Dataset(output_filename, "w", format="NETCDF4") as nc:
            # Global attributes
            nc.title = "Bluelink Ocean Current Data"
            nc.history = f"Created {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            nc.Conventions = "CF-1.8"
            
            # Dimensions
            nc.createDimension("time", num_time_steps)
            nc.createDimension("lat", lat_len)
            nc.createDimension("lon", lon_len)
            
            # Variables
            time_var = nc.createVariable("time", "f8", ("time",))
            time_var.units = time_unit
            time_var.calendar = "gregorian"
            
            lat_var = nc.createVariable("lat", "f4", ("lat",))
            lat_var.units = "degrees_north"
            lat_var[:] = lat_array
            
            lon_var = nc.createVariable("lon", "f4", ("lon",))
            lon_var.units = "degrees_east"
            lon_var[:] = lon_array
            
            # Data variables with fill_value
            u_var = nc.createVariable(
                var_name1, "f4", ("time", "lat", "lon"),
                zlib=True, complevel=5, fill_value=-30000, shuffle=True
            )
            u_var.units = "m s-1"
            
            v_var = nc.createVariable(
                var_name2, "f4", ("time", "lat", "lon"),
                zlib=True, complevel=5, fill_value=-30000, shuffle=True
            )
            v_var.units = "m s-1"

    @staticmethod
    def add_nc(output_filename, var_name1, var_name2, raw_data1, raw_data2, data_time, x, time_unit):
        with Dataset(output_filename, "r+", format="NETCDF4") as nc:
            nc.variables["time"][x] = date2num(data_time, units=time_unit, calendar="gregorian")
            nc.variables[var_name1][x, :, :] = np.where(np.isnan(raw_data1), -30000, raw_data1)
            nc.variables[var_name2][x, :, :] = np.where(np.isnan(raw_data2), -30000, raw_data2)

    @staticmethod
    def subset_nc_file(input_file, output_file, lon_min=100, lon_max=300, lat_min=-45, lat_max=45):
        with Dataset(input_file, 'r') as src:
            lons = src.variables['lon'][:]
            lats = src.variables['lat'][:]
            
            lon_idx = np.where((lons >= lon_min) & (lons <= lon_max))[0]
            lat_idx = np.where((lats >= lat_min) & (lats <= lat_max))[0]
            
            if len(lon_idx) == 0 or len(lat_idx) == 0:
                raise ValueError("No data in specified range")
            
            with Dataset(output_file, 'w', format='NETCDF4') as dst:
                # Copy global attributes
                for attr in src.ncattrs():
                    dst.setncattr(attr, src.getncattr(attr))
                
                # Create dimensions
                dst.createDimension("time", len(src.dimensions['time']))
                dst.createDimension("lat", len(lat_idx))
                dst.createDimension("lon", len(lon_idx))
                
                # Copy variables
                for name, var in src.variables.items():
                    if name == 'time':
                        new_var = dst.createVariable(name, var.datatype, var.dimensions)
                        new_var[:] = var[:]
                    elif name == 'lat':
                        new_var = dst.createVariable(name, var.datatype, var.dimensions)
                        new_var[:] = var[lat_idx]
                    elif name == 'lon':
                        new_var = dst.createVariable(name, var.datatype, var.dimensions)
                        new_var[:] = var[lon_idx]
                    elif var.dimensions == ('time', 'lat', 'lon'):
                        fill = var.getncattr('_FillValue') if '_FillValue' in var.ncattrs() else -30000
                        new_var = dst.createVariable(
                            name, var.datatype, var.dimensions,
                            zlib=True, complevel=5, fill_value=fill, shuffle=True
                        )
                        new_var[:] = var[:, lat_idx[0]:lat_idx[-1]+1, lon_idx[0]:lon_idx[-1]+1]
                    
                    # Copy attributes
                    for attr in var.ncattrs():
                        if attr != '_FillValue':
                            new_var.setncattr(attr, var.getncattr(attr))
