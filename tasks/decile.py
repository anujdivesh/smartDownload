# -*- coding: utf-8 -*-
"""
Created on Mon Mar 17 16:06:18 2025

@author: molenit
"""

import numpy as np
import netCDF4 as nc
import glob
import os
import matplotlib.pyplot as plt
# Define input and output paths
input_folder = "/home/pop/ocean_portal/datasets/model/regional/noaa/hindcast/monthly/sst_anomalies"  # Update this with your folder path
output_folder = "/home/pop/ocean_portal/datasets/model/regional/noaa/nrt/monthly"

variable_name = "sst"  # Update with your variable name (e.g., 'sst')

# Define the year and month you want to analyze
chosen_year = 2025  # Change this to the desired year
chosen_month = 1  # Change this to the desired month (1 = January, 2 = February, ..., 12 = December)

# Get all NetCDF files in the folder
files = sorted(glob.glob(os.path.join(input_folder, "*.nc")))

# Filter files to prioritize final over preliminary
final_files = []
preliminary_files = []

for file in files:
    if '_preliminary.nc' in file:
        preliminary_files.append(file)
    else:
        final_files.append(file)

# Create a dictionary to map base names to final files
final_file_dict = {os.path.basename(file).replace('_preliminary.nc', '.nc'): file for file in final_files}

# Select files, preferring final over preliminary
selected_files = []
for file in files:
    base_name = os.path.basename(file)
    if base_name in final_file_dict:
        selected_files.append(final_file_dict[base_name])
    elif '_preliminary.nc' not in file:
        selected_files.append(file)

# Remove duplicates by converting to a set and back to a list
selected_files = list(set(selected_files))
selected_files.sort()
if not selected_files:
    raise ValueError("No valid NetCDF files found in the input directory.")

# Read data and stack into a 3D array (time, lat, lon)
data_list = []
time_list = []
for file in selected_files:
    with nc.Dataset(file, "r") as ds:
        data = ds.variables[variable_name][:]  # Read variable
        data = np.squeeze(data)  # Remove single-length dimensions
        time = nc.num2date(ds.variables['time'][:], ds.variables['time'].units)  # Convert time
        data_list.append(data)
        time_list.append(time)

# Convert list to 3D numpy array
data_array = np.ma.array(data_list)  # (time, lat, lon)
time_array = np.array(time_list).flatten()

# Filter for the chosen month across all years
month_indices = [i for i, t in enumerate(time_array) if t.month == chosen_month]
month_data = data_array[month_indices, :, :]

# Compute deciles (percentiles 10, 20, ..., 90)
percentiles = [10 * i for i in range(1, 10)]
deciles = np.percentile(month_data, percentiles, axis=0)

# Get the latest SST for the chosen month and year
latest_sst = data_array[[i for i, t in enumerate(time_array) if t.year == chosen_year and t.month == chosen_month], :, :][-1, :, :]

# Classify the latest SST into decile categories
category_map = np.zeros_like(latest_sst, dtype=int)
category_map[latest_sst <= deciles[0]] = 1  # Lowest on record
category_map[(latest_sst > deciles[0]) & (latest_sst <= deciles[1])] = 2  # Very much below average
category_map[(latest_sst > deciles[1]) & (latest_sst <= deciles[2])] = 3  # Below average
category_map[(latest_sst > deciles[2]) & (latest_sst <= deciles[6])] = 5  # Average
category_map[(latest_sst > deciles[6]) & (latest_sst <= deciles[7])] = 8  # Above average
category_map[(latest_sst > deciles[7]) & (latest_sst <= deciles[8])] = 10  # Very much above average
category_map[latest_sst > deciles[8]] = 11  # Highest on record

# Save deciles to a new NetCDF file
month_name = time_array[month_indices[-1]].strftime("%B").lower()  # Get month name (e.g., "february")
output_file = os.path.join(output_folder, f"{chosen_year}_{month_name}_deciles.nc")
with nc.Dataset(output_file, "w", format="NETCDF4") as ds_out:
    with nc.Dataset(selected_files[0], "r") as ds_ref:
        # Copy dimensions from the reference file
        for dim_name, dim_obj in ds_ref.dimensions.items():
            ds_out.createDimension(dim_name, len(dim_obj))

        # Copy latitude & longitude variables while ds_ref is still open
        for var_name in ["lat", "lon"]:
            var_ref = ds_ref.variables[var_name]
            var_out = ds_out.createVariable(var_name, var_ref.datatype, var_ref.dimensions)
            var_out[:] = var_ref[:]  # Copy data

    # Save category map
    category_var = ds_out.createVariable("sst_category", "i4", ("lat", "lon"))
    category_var[:] = category_map

print(f"{chosen_year} {month_name.capitalize()} decile file saved: {output_file}")
