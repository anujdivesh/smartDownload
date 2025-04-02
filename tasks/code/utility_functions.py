import requests
from datetime import datetime, timedelta
import netCDF4 as nc
import xarray as xr
from controller_country import initialize_countryController
import os
import shutil
from controller_server_path import PathManager
from dateutil.relativedelta import relativedelta
import calendar
from copernicusmarine import subset
import numpy as np
import glob

class Utility:
    @staticmethod
    def remove_substrings(original_string, substrings):
        for substring in substrings:
            original_string = original_string.replace(substring, "")
        return original_string
    
    @staticmethod
    def add_time(current_time, month=0, days=0, hours=0, minutes=0):
        new_time = current_time + timedelta(days=days, hours=hours, minutes=minutes)
        return new_time + relativedelta(months=month)
    
    @staticmethod
    def update_api(url, data, headers=None):
        try:
            tokenPreGen = Utility.validate_or_generate_token()
            token=headers={"Authorization":"Bearer "+tokenPreGen}
            #print(token, data, url)
            response = requests.put(url+"/", json=data, headers=token)
            response.raise_for_status()  # Raise an error for bad status codes
            print('api updated.')
            return response.json()  # Return the response content as JSON
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")  # Handle HTTP errors
        except Exception as err:
            print(f"An error occurred: {err}")  # Handle other errors
        return None
    
    @staticmethod
    def url_exists(url):
        try:
            response = requests.head(url,verify=False)
            # You can also use requests.get(url) if you need to access the content of the page
            if response.status_code == 200:
                return True
            else:
                return False
        except requests.exceptions.RequestException as e:
            # Handle any exceptions that occur
            print(f"An error occurred: {e}")
            return False
        
    @staticmethod
    def download_large_file(url, destination):
        try:
            with requests.get(url, stream=True,verify=False) as response:
                response.raise_for_status()
                with open(destination, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            print("File downloaded successfully!")
        except requests.exceptions.RequestException as e:
            print("Error downloading the file:", e)

    @staticmethod
    def time_diff(time1, time2):
        difference = time1 - time2
        difference_in_minutes = difference.total_seconds() / 60
        #print(difference_in_minutes)
        var = False
        #if abs(difference_in_minutes) > 0 and abs(difference_in_minutes) < 20:
        #    var = True
        if time2 < time1:
            var = True
        
        return var
    
    @staticmethod
    def get_subset(ds):
        subset_region = False
        if ds.subset == True:
            url = f"https://dev-oceanportal.spc.int/v1/api/country/%s" % (ds.subset_region)
            subset_region = initialize_countryController(url)
        return subset_region
    
    @staticmethod
    def get_variables(ds, old_path, new_path):
        variab = xr.open_dataset(old_path)
        varibles = ds.variables
        variab = variab[varibles.split(",")]
        variab.to_netcdf(path=new_path ,mode='w',format='NETCDF4',  engine='netcdf4')
        

    @staticmethod
    def subset_netcdf(ds, old_path, new_path):
        try:
            subset = xr.open_dataset(old_path)
            lon, lat = "", ""
            #CHECK IF DIMENSIONS ARE CORRECT
            for dim_name, dim in subset.dims.items():
                origname = dim_name.strip()
                tolower = origname.lower()
                if 'lon' in tolower:
                    lon = tolower
                if 'lat' in tolower:
                    lat = tolower
            
            #CHECK IF SUBSET IS REQUIRED
            if ds.subset:
                #CONTINUE SUBSETTING
                xmin_xmax = ds.xmin_xmax.strip()
                xmin_xmax_arr = xmin_xmax.split(',')
                ymin_ymax = ds.ymin_ymax.strip()
                ymin_ymax_arr = ymin_ymax.split(',')
                #print(xmin_xmax_arr[0], xmin_xmax_arr[1], ymin_ymax_arr[0], ymin_ymax_arr[1])
                if lon.lower() == "lon":
                    if ds.convert_longitude:
                        subset = subset.sel(lat=slice(int(xmin_xmax_arr[0]), int(xmin_xmax_arr[1])),\
                            lon=slice(int(ymin_ymax_arr[0]), int(ymin_ymax_arr[1])))
                    else:
                        latmax = int(ymin_ymax_arr[1]) - 360
                        print(ymin_ymax_arr[0], latmax)
                        subset_lat = subset.sel(lat=slice(int(xmin_xmax_arr[0]), int(xmin_xmax_arr[1])))
                        subset_lon1 = subset_lat.sel(lon=slice(int(ymin_ymax_arr[0]), 180))
                        subset_lon2 = subset_lat.sel(lon=slice(-180, latmax))
                        subset = xr.concat([subset_lon1, subset_lon2], dim='lon')
                else:
                    if ds.convert_longitude:
                        subset = subset.sel(latitude=slice(int(xmin_xmax_arr[0]), int(xmin_xmax_arr[1])),\
                            longitude=slice(int(ymin_ymax_arr[0]), int(ymin_ymax_arr[1])))
                    else:
                        latmax = int(ymin_ymax_arr[1]) - 360
                        subset_lat = subset.sel(latitude=slice(int(xmin_xmax_arr[0]), int(xmin_xmax_arr[1])))
                        subset_lon1 = subset_lat.sel(longitude=slice(ymin_ymax_arr[0], 180))
                        subset_lon2 = subset_lat.sel(longitude=slice(-180, latmax))
                        subset = xr.concat([subset_lon1, subset_lon2], dim='lon')
            
            #SAVE NEW NETCDF
            subset  = xr.decode_cf(subset )
            subset.to_netcdf(path=new_path ,mode='w',format='NETCDF4',  engine='netcdf4')
            
            return True
        except Exception as e:
            return False
    
    @staticmethod
    def remove_file(url):
        if os.path.exists(url):
            os.remove(url)
        else:
            print("File was already removed.") 

    @staticmethod
    def rename_file(old_name, new_name):
        try:
            shutil.move(old_name, new_name)
        except FileNotFoundError:
            print(f"File '{old_name}' not found.")
        except Exception as e:
            print(f"Error occurred: {e}")


    
    @staticmethod
    def update_tasks(download_succeed, is_error, new_file_name,new_download_time, task, ds):
        if download_succeed:
            data = {
                "next_run_time":new_download_time.strftime("%Y-%m-%d %H:%M:%S"),
                "last_run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "next_download_file":new_file_name,
                "last_download_file":task.next_download_file,
                "success_count":task.success_count + 1,
                "health":"Excellent"
            }
            Utility.update_api(PathManager.get_url('ocean-api','task_download',str(task.id)), data)
            print('File download successful!')
        else:
            print('File does not exist, try again later')
            healthy = "Excellent"
            currcount = task.attempt_count + 1
            if currcount > 32:
                healthy = "Poor"
            update_time = Utility.add_time(datetime.strptime(task.next_run_time,"%Y-%m-%dT%H:%M:%SZ"),ds.check_months,ds.check_days, ds.check_hours,ds.check_minutes).strftime("%Y-%m-%d %H:%M:%S")
            data = {
                "next_run_time":update_time,
                "last_run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "attempt_count":task.attempt_count + 1,
                "health":healthy
            }
            Utility.update_api(PathManager.get_url('ocean-api','task_download',str(task.id)), data)

        if is_error:
            update_time = Utility.add_time(datetime.strptime(task.next_run_time,"%Y-%m-%dT%H:%M:%SZ"),ds.check_months,ds.check_days, ds.check_hours,ds.check_minutes).strftime("%Y-%m-%d %H:%M:%S")
            data = {
                    "next_run_time":update_time,
                    "last_run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "fail_count":task.fail_count + 1,
                    "health":"Failed"
            }
            Utility.update_api(PathManager.get_url('ocean-api','task_download',str(task.id)), data)
            print('Download Failed')
        pass
    
    @staticmethod
    def copy_file(source_file, destination_file):
        try:
            shutil.copy(source_file, destination_file)
        except FileNotFoundError:
            print(f"Error: File '{source_file}' not found.")
        except PermissionError:
            print(f"Error: Permission denied to copy '{source_file}' to '{destination_file}'.")
        except Exception as e:
            print(f"Error: {e}")

    @staticmethod
    def get_first_last_day_of_month(year, month):
        """Returns the first day of the given month and year."""
        last_day = calendar.monthrange(year, month)[1]
        return datetime(year, month, 1), datetime(year, month, last_day)

    @staticmethod
    def download_from_copernicus(ds,dataset_id, varibales, start_datetime, end_datetime,new_file_name, download_directory):
        try:
            xmin_xmax = ds.xmin_xmax.strip()
            xmin_xmax_arr = xmin_xmax.split(',')
            ymin_ymax = ds.ymin_ymax.strip()
            ymin_ymax_arr = ymin_ymax.split(',')
            #print(xmin_xmax_arr[0], xmin_xmax_arr[1], ymin_ymax_arr[0], ymin_ymax_arr[1])
            subset(
            dataset_id=dataset_id,
            variables=varibales,
            minimum_longitude= int(ymin_ymax_arr[0]),
            maximum_longitude= int(ymin_ymax_arr[1]),
            minimum_latitude= int(xmin_xmax_arr[0]),
            maximum_latitude= int(xmin_xmax_arr[1]),
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            force_download=True,
            overwrite_output_data=True,
            output_filename=new_file_name,
            output_directory=download_directory,
            credentials_file=PathManager.get_url('copernicus-credentials')
            )
            
            return True
        except Exception as e:
            print(e)
            return False

    @staticmethod
    def get_last_sunday(year, month):
        # Determine the first day of the next month
        if month == 12:
            next_month = 1
            next_year = year + 1
        else:
            next_month = month + 1
            next_year = year
        
        # First day of the next month
        first_day_next_month = datetime(next_year, next_month, 1)
        
        # Last day of the current month is one day before the first day of the next month
        last_day_of_month = first_day_next_month - timedelta(days=1)
        
        # Calculate the last Sunday of the month
        # If the last day of the month is Sunday (6), no need to change
        days_to_last_sunday = (last_day_of_month.weekday() + 1) % 7
        last_sunday = last_day_of_month - timedelta(days=days_to_last_sunday)
        
        return last_sunday

    @staticmethod
    def special_filename_coral_bleaching(prepare_new_time):
        #add 3 months
        new_date_time_months = prepare_new_time + relativedelta(months=4)
        #add 14 days
        new_date_time_obj = prepare_new_time + timedelta(days=14)
        lastSunday = Utility.get_last_sunday(new_date_time_months.year, new_date_time_months.month)

        return new_date_time_obj,lastSunday
    
    @staticmethod
    def subtract_special_one_month_coral(prepare_new_time):
        datetime_str = prepare_new_time.replace(".nc", "")
        new_date = datetime.strptime(datetime_str, "%Y%m%d")
        #add 3 months
        new_date_time_months = new_date + relativedelta(months=-1)
        lastSunday = Utility.get_last_sunday(new_date_time_months.year, new_date_time_months.month)

        return "%s.%s" % (lastSunday.strftime("%Y%m%d"), "nc")
    
    @staticmethod
    def reproject_netcdf(ds, old_path, new_path):
        try:
            subset = xr.open_dataset(old_path)
            lon, lat = "", ""
            #CHECK IF DIMENSIONS ARE CORRECT
            for dim_name, dim in subset.dims.items():
                origname = dim_name.strip()
                tolower = origname.lower()
                if 'lon' in tolower:
                    lon = tolower
                if 'lat' in tolower:
                    lat = tolower
            
            
            #CHECK IF LON BETWEEN 0 TO 360
            below_zero = (subset[lon] > 180).any().values

            print('converting from 0-360 to -180-180')
            lons = np.asarray(subset[lon].values)
            lons = (lons + 180) % 360 - 180
            subset[lon] = lons
            subset  = xr.decode_cf(subset )
            subset.to_netcdf(path=new_path ,mode='w',format='NETCDF4',  engine='netcdf4')
            
            return True
        except Exception as e:
            return False
    @staticmethod
    def download_obdaac(url, local_filename):
        try:
        # Send a GET request to the URL
            with requests.get(url, stream=True, timeout=120) as response:
                # Check if the request was successful
                response.raise_for_status()
                # Open a local file with the desired name
                with open(local_filename, 'wb') as file:
                    # Write the content to the file in chunks
                    #file.write(response.content)
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)
            print(f"File downloaded as {local_filename}")
            return True
        except Exception as e:
            print(e)
            return False
            print('File not found.')

    @staticmethod
    def remove_files_by_extension(directory, extension):
        # Create a pattern to match files with the specified extension
        pattern = os.path.join(directory, f"*.{extension}")
        
        # Use glob to find all files matching the pattern
        files_to_remove = glob.glob(pattern)
        
        # Loop through the files and remove each one
        for file in files_to_remove:
            try:
                os.remove(file)
                print(f"Removed: {file}")
            except Exception as e:
                print(f"Error removing {file}: {e}")

    @staticmethod
    def merge_weekly_coral_bleaching_forecast(fpath, fout):
        indeces = [4, 8, 12, 16]
        outname = 'CRW_BAA'
        other_keys = []


        vname_fmt = 'CRW_BAA_Week_{:02d}'
        dtime_fmt = '%Y%m%dT%H%M%SZ'
        vnames = [ vname_fmt.format(idx) for idx in indeces ]



        # Loading the data
        with xr.open_dataset(fpath) as fncdf:
            fncdf = fncdf.copy()
            
        # Creating a datetime list based on the variables metadata
        time = [ datetime.strptime(fncdf[vname].attrs['start_time'], dtime_fmt) for vname in vnames ]
        time = np.array(time, dtype='datetime64[ns]')


        # Extracting the arrays we will be using
        data = np.array([ fncdf[name].data.squeeze().copy() for name in vnames ])

        longitude = fncdf['lon'].data.copy()
        latitude = fncdf['lat'].data.copy()

        # Creating a new dataset with only the data we previously selected
        coords = {'time' : time, 'lat' : latitude, 'lon' : longitude}
        dims = list(coords.keys())

        # Copying the variable attributes to the new dataset and updating some of them
        attrs = fncdf[vnames[0]].attrs.copy()
        attrs['start_time'] = fncdf[vnames[0]].attrs['start_time']
        attrs['time_coverage_start'] = fncdf[vnames[0]].attrs['time_coverage_start']
        attrs['stop_time'] = fncdf[vnames[-1]].attrs['stop_time']

        ds = xr.DataArray(data,
                        coords = coords,
                        dims   = dims,
                        attrs  = attrs ).to_dataset(name=outname)

        # Updating the last attributes (coordinates attributes)
        ds['lon'].attrs = fncdf['lon'].attrs
        ds['lat'].attrs = fncdf['lat'].attrs

        # Making sure we are using the right fillValues
        # ds[outname].data[~np.isfinite(ds[outname].data)] = -5


        ds_optional = [ fncdf[key].copy() for key in other_keys ]

        if ds_optional:
            ds = xr.merge([ds, *ds_optional], compat='no_conflicts')


        encoding = { 'time': 
                        {'dtype' : 'float', 
                        'units' : "days since 1950-01-01T00:00:00Z", 
                        'calendar' : "proleptic_gregorian" }
                    }

        # Uncomment this line to generate the netcdf
        ds.to_netcdf(path=fout, format='NETCDF4_CLASSIC', encoding=encoding)
        print('merged weekly forecast')

    @staticmethod
    def generate_token():
        token_file = PathManager.get_url('token-credentials')
        response1 = requests.post(PathManager.get_url('ocean-api',"token/"),json={"username":"admin","password":"Oceanportal2017*"})
        res1 = response1.json()
        token=res1['access']
        with open(token_file, 'w') as file:
            file.write(token)

        print(f"Token saved to {token_file}")
        return token
    
    @staticmethod
    def load_token():
        token_file = PathManager.get_url('token-credentials')
        try:
            with open(token_file, 'r') as file:
                token = file.read().strip()
                return token
        except FileNotFoundError:
            print(f"Error: Token file '{token_file}' not found.")
            return None
        
    @staticmethod
    def validate_or_generate_token():
        validation_url = PathManager.get_url('ocean-api','account/1')
        token = Utility.load_token()

        if token:
            try:
                # Make a GET request to the validation endpoint with the Bearer token
                headers = {
                    "Authorization": f"Bearer {token}"
                }
                response = requests.get(validation_url, headers=headers)
                response.raise_for_status()  # Raise an error for bad status codes (4xx, 5xx)

                # If the request is successful, return the token
                print("Token is valid.")
                return token

            except requests.exceptions.RequestException as e:
                print(f"Error: Token validation failed. {e}")

        # If the token is invalid or not found, generate a new token
        print("Generating a new token...")
        new_token = Utility.generate_token()

        if new_token:
            return new_token
        else:
            print("Error: Failed to generate a new token.")
            return None
    @staticmethod
    def multiply_netcdf_values(file_path, value):
        """Multiply numeric variables and save safely using a temporary file."""
        temp_path = file_path + ".tmp"
        
        try:
            with xr.open_dataset(file_path) as ds:
                for var_name in ds.variables:
                    if var_name not in ds.dims and ds[var_name].dtype.kind in ['f', 'i', 'u']:
                        ds[var_name] = ds[var_name] * value
                
                # Write to temporary file first
                ds.to_netcdf(temp_path, mode='w')
            
            # Replace original file with the temporary one
            os.replace(temp_path, file_path)
            print(f"Values multiplied by {value} and saved to {file_path}.")
        
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)  # Clean up temp file on error
            raise e
        
    @staticmethod
    def adjust_netCDF(file_path, threshold=50):
        """
        Set all values above a specified threshold to NaN in all variables of a NetCDF file.
        Preserves the original file structure and metadata.

        Args:
            file_path (str): Path to the NetCDF file
            threshold (float): Values above this will be set to NaN (default: 50)
        """
        # Create temp file path
        temp_dir = os.path.dirname(file_path) or '.'
        temp_path = os.path.join(temp_dir, f"temp_{os.path.basename(file_path)}")
        
        try:
            with xr.open_dataset(file_path) as ds:
                # Create a copy to modify
                masked_ds = ds.copy()
                
                # Process each variable
                for var_name in ds.variables:
                    if var_name not in ds.dims:  # Skip coordinate variables
                        # Mask values > threshold
                        masked_ds[var_name] = ds[var_name].where(ds[var_name] <= threshold)
                        print(f"Masked values > {threshold} in variable '{var_name}'")
                
                # Write to temporary file first
                masked_ds.to_netcdf(temp_path, mode='w')
                
                # Replace original file
                shutil.move(temp_path, file_path)
                print(f"Successfully masked values > {threshold} in {file_path}")
                
        except Exception as e:
            # Clean up temp file if error occurs
            if os.path.exists(temp_path):
                os.remove(temp_path)
            print(f"Error processing {file_path}: {str(e)}")
            raise