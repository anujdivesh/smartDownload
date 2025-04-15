import requests
import xml.etree.ElementTree as xmltree
from owslib.wms import WebMapService
import pandas as pd
from datetime import datetime, timedelta
import xarray as xr
import pandas as pd
import os
import sys
input_folder = "/home/pop/Desktop/REYNOLDS/cmems"
output_folder = "/home/pop/Desktop/REYNOLDS/multiply"  # Create this folder first

# Create output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)


# Loop through all files in the input directory
for filename in os.listdir(input_folder):
    if filename.endswith('.nc'):  # Process only NetCDF files
        input_path = os.path.join(input_folder, filename)
        output_path = os.path.join(output_folder, filename)
        
        try:
            # Open the NetCDF file
            ds = xr.open_dataset(input_path)
            
            # Multiply all variables by 1000 (adjust if you only want specific variables)
            ds_multiplied = ds * 1000
            
            # Copy attributes to maintain metadata
            ds_multiplied.attrs = ds.attrs
            for var in ds_multiplied.variables:
                ds_multiplied[var].attrs = ds[var].attrs
            
            # Save to output file
            ds_multiplied.to_netcdf(output_path)
            print(f"Processed and saved: {filename}")
            
            # Close the datasets
            ds.close()
            ds_multiplied.close()
            
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")

print("Processing complete!")

print("Processing complete!")

"""
ds = xr.open_dataset(file_path)

# Subset the data
# Note: Longitude range 100 to 300 is equivalent to -260 to -60 in -180 to 180 system
# We'll need to handle the longitude wrapping
subset = ds.sel(
    lat=slice(-45, 45),
    lon=slice(100, 300)
)

# Save the subset to a new NetCDF file
subset.to_netcdf(out_path)

print(f"Subset saved to {out_path}")
"""