
from model_task import task
from controller_dataset import initialize_datasetController
from controller_country import initialize_countryController
import requests
from datetime import datetime, timedelta
from utility_functions import Utility
from controller_server_path import PathManager
import subprocess
import os, sys
import netCDF4
import xarray as xr
from copernicusmarine import subset
from dateutil.relativedelta import relativedelta
import calendar
import numpy as np
import glob
from nco import Nco
from bluelink import BlueLink
import netCDF4 as nc

class taskController(task):
    def __init__(self, id, task_name, class_id, dataset_id,status,priority,\
                 duration,task_start_time,next_run_time,last_run_time,next_download_file,last_download_file,enabled,health,fail_count,\
                    success_count,reset_count,attempt_count,predecessor_class,predecessor_class_id,successor_class,successor_class_id,created_by,launched_by,\
                        retain,retention_days):
        super().__init__( id, task_name, class_id, dataset_id,status,priority,\
                 duration,task_start_time,next_run_time,last_run_time,next_download_file,last_download_file,enabled,health,fail_count,\
                    success_count,reset_count,attempt_count,predecessor_class,predecessor_class_id,successor_class,successor_class_id,created_by,launched_by,\
                        retain,retention_days)
    
    def generate_current_download_time(self,ds):
        convert_to_datetime = ""
        suffix = ds.download_file_suffix
        if "{special}" in suffix:
            substrings_to_remove = [ds.download_file_prefix, ".nc"]
            new_string = Utility.remove_substrings(self.next_download_file, substrings_to_remove)
            seperate = new_string.split("_")
            convert_to_datetime = datetime.strptime(seperate[0], ds.download_file_infix)
        else:
            substrings_to_remove = [ds.download_file_prefix, ds.download_file_suffix]
            new_string = Utility.remove_substrings(self.next_download_file, substrings_to_remove)
            convert_to_datetime = datetime.strptime(new_string, ds.download_file_infix)

        return convert_to_datetime

    #GENERATE NEXT DOWNLOAD FILENAME AND NEXT DOWNLOAD TIME
    def generate_next_download_filename(self,ds):
        new_file_name,new_download_time = "",""
        suffix = ds.download_file_suffix
        if "{bluelink}" in suffix:
            new_file_name = self.next_download_file
            prepare_new_time = datetime.strptime(self.next_run_time, "%Y-%m-%dT%H:%M:%SZ")
            new_download_time = Utility.add_time(prepare_new_time,ds.check_months,ds.check_days, ds.check_hours,ds.check_minutes)
        else:
            if "{special}" in suffix:
                print('Special file name generation...')
                """
                substrings_to_remove = [ds.download_file_prefix, ".nc"]
                new_string = Utility.remove_substrings(self.next_download_file, substrings_to_remove)
                seperate = new_string.split("_")
                convert_to_datetime = datetime.strptime(seperate[0], ds.download_file_infix)
                prepare_new_time = Utility.add_time(convert_to_datetime,ds.frequency_months,ds.frequency_days, ds.frequency_hours,ds.frequency_minutes)
                first_date, second_date = Utility.special_filename_coral_bleaching(prepare_new_time)
                new_file_name = ds.download_file_prefix+prepare_new_time.strftime('%Y%m%d')+"_for_"+first_date.strftime('%Y%m%d')+"to"+second_date.strftime('%Y%m%d')+".nc"
                new_download_time = Utility.add_time(prepare_new_time,ds.check_months,ds.check_days, ds.check_hours,ds.check_minutes)
                """
                print('Special file name generation...')
                substrings_to_remove = [ds.download_file_prefix, ".nc"]
                new_string = Utility.remove_substrings(self.next_download_file, substrings_to_remove)
                seperate = new_string.split("_")
                convert_to_datetime = datetime.strptime(seperate[0], ds.download_file_infix)
                prepare_new_time = Utility.add_time(convert_to_datetime,0,7, 0,0)
                second_date = prepare_new_time + timedelta(days=14)
                thirddate = second_date + relativedelta(months=3)
                last_sun = Utility.get_last_sunday(thirddate.year,thirddate.month)
                new_file_name = ds.download_file_prefix+prepare_new_time.strftime('%Y%m%d')+"_for_"+second_date.strftime('%Y%m%d')+"to"+last_sun.strftime('%Y%m%d')+".nc"
                new_download_time = Utility.add_time(prepare_new_time,ds.check_months,ds.check_days, ds.check_hours,ds.check_minutes)
            else:
                if ds.download_file_infix.strip() == "none":
                    new_file_name = self.next_download_file
                    prepare_new_time = datetime.strptime(self.next_run_time, "%Y-%m-%dT%H:%M:%SZ")
                    new_download_time = Utility.add_time(prepare_new_time,ds.check_months,ds.check_days, ds.check_hours,ds.check_minutes)

                else:
                    substrings_to_remove = [ds.download_file_prefix, ds.download_file_suffix]
                    new_string = Utility.remove_substrings(self.next_download_file, substrings_to_remove)

                    if "_" in new_string:
                        print("extreme datetime")
                        date_split = new_string.split('_')
                        infix_split = ds.download_file_infix.split('_')
                        convert_to_datetime = datetime.strptime(date_split[0], infix_split[0])
                        prepare_new_time = Utility.add_time(convert_to_datetime,ds.frequency_months,ds.frequency_days, ds.frequency_hours,ds.frequency_minutes)
                        
                        if ds.download_method == 3:
                            new_file_name = ds.download_file_prefix + "" +prepare_new_time.strftime(infix_split[0]) +"_"+prepare_new_time.strftime(infix_split[1])+ ds.download_file_suffix
                            new_download_time = Utility.add_time(prepare_new_time,ds.check_months,ds.check_days, ds.check_hours,ds.check_minutes)
                        elif ds.download_method == 2: 
                            first_day,last_day = Utility.get_first_last_day_of_month(prepare_new_time.year, prepare_new_time.month)
                            new_file_name = ds.download_file_prefix + "" +first_day.strftime(infix_split[0]) +"_"+last_day.strftime(infix_split[1])+ ds.download_file_suffix
                            new_download_time = Utility.add_time(first_day,ds.check_months,ds.check_days, ds.check_hours,ds.check_minutes)
                        else:
                            convert_to_datetime = datetime.strptime(new_string, ds.download_file_infix)
                            prepare_new_time = Utility.add_time(convert_to_datetime,ds.frequency_months,ds.frequency_days, ds.frequency_hours,ds.frequency_minutes)
                            new_file_name = ds.download_file_prefix + "" +prepare_new_time.strftime(ds.download_file_infix) + ds.download_file_suffix
                            new_download_time = Utility.add_time(prepare_new_time,ds.check_months,ds.check_days, ds.check_hours,ds.check_minutes)

                    else:
                        print("normal datetime")
                        convert_to_datetime = datetime.strptime(new_string, ds.download_file_infix)
                        prepare_new_time = Utility.add_time(convert_to_datetime,ds.frequency_months,ds.frequency_days, ds.frequency_hours,ds.frequency_minutes)
                        new_file_name = ds.download_file_prefix + "" +prepare_new_time.strftime(ds.download_file_infix) + ds.download_file_suffix
                        new_download_time = Utility.add_time(prepare_new_time,ds.check_months,ds.check_days, ds.check_hours,ds.check_minutes)
                
        return new_file_name,new_download_time

    #CORAL BLEACHING HAS ISSUES SOMETIMES 
    def try_multiple_only_coral(self,ds,url):
        exists = Utility.url_exists(url)
        
        print('trying multiple')
        suffix = ds.download_file_suffix
        new_url = url
        if not exists:
            if "{special}" in suffix:
                split = url.split("to")
                #print(split[1])
                last = Utility.subtract_special_one_month_coral(split[1])
                new_url = url.replace(split[1], last)
                print("new url: "+new_url)
        return new_url


    def create_product_directory(self,ds):
        local_dir = ds.local_directory_path
        root_dir = PathManager.get_url('root-dir')
        if "{root-dir}" in local_dir:
            local_dir = local_dir.replace("{root-dir}", root_dir)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        pass

    def download_bluelink(self,ds):
        download_complete = False
        is_error = False
        try:
            #GET URL
            url = ds.data_download_url
            if "{blue_link_download}" in url:
                url = url.replace('{blue_link_download}', '')
            tmp_out_dir = PathManager.get_url('tmp','')
            BlueLink.Download_Compile_Bluelink_Currents(tmp_out_dir, url)
            
            #MOVE FILES TO LOCAL OR SCP
            local_dir = ds.local_directory_path
            if ds.download_to_local_dir:
                root_dir = PathManager.get_url('root-dir')
                if "{root-dir}" in local_dir:
                    local_dir = local_dir.replace("{root-dir}", root_dir)
                print(local_dir)
                Utility.copy_file(PathManager.get_url('tmp',self.next_download_file), local_dir)
                Utility.remove_file(PathManager.get_url('tmp',self.next_download_file))
                Utility.remove_file(PathManager.get_url('tmp','Bluelink_currents_tmp.nc'))
            

            #SET STAUS
            download_complete = True
        except Exception as e:
            print(e)
            is_error = True
        return download_complete, is_error


    def download_http(self,ds):
        download_complete = False
        is_error = False
        try:
            #GET URL
            url = ds.data_download_url
            
            #CHECK FOR YEAR
            if "{year}" in url:
                convert_to_datetime = self.generate_current_download_time(ds)
                url = url.replace('{year}', convert_to_datetime.strftime('%Y'))

            #CHECK FOR MONTH
            if "{month}" in url:
                convert_to_datetime = self.generate_current_download_time(ds)
                url = url.replace('{month}', convert_to_datetime.strftime('%m'))
            
            #REPLACE WITH FILE TO DOWNLOAD
            if "{download_file_name}" in url:
                url = url.replace('{download_file_name}', self.next_download_file)
            

            #TRY MULTIPLE FOR CORAL
            suffix = ds.download_file_suffix
            if "{special}" in suffix:
                url = self.try_multiple_only_coral(ds,url)

            print(url)

            if Utility.url_exists(url):
                #DOWNLOAD THE FILE
                Utility.download_large_file(url,PathManager.get_url('tmp',self.next_download_file))

                if self.id == 10:
                    tmp_name2 = "%s%s" % (PathManager.get_url('tmp',self.next_download_file), "_climate.nc")
                    Utility.rename_file(PathManager.get_url('tmp',self.next_download_file), tmp_name2)
                    new_opath = PathManager.get_url('tmp',self.next_download_file)
                    print(tmp_name2)
                    print(new_opath)
                    self.addClimatology(tmp_name2,new_opath)
                    Utility.remove_file(tmp_name2)
                    print('adding climatalogy')
                
                #RENAME TO TMP IN THE SAME DIR
                tmp_name = "%s%s" % (PathManager.get_url('tmp',self.next_download_file), "_tmp.nc")
                Utility.rename_file(PathManager.get_url('tmp',self.next_download_file), tmp_name)

                #PATHS
                tmp_path = tmp_name
                new_name = "%s" % (PathManager.get_url('tmp',self.next_download_file))
                
                #IF VARIABLES
                if ds.has_variables:
                    print('getting variables...')
                    Utility.get_variables(ds,tmp_path,"%s%s" % (PathManager.get_url('tmp',self.next_download_file), "_varib.nc"))
                
                #IF SUBSET
                if ds.subset:
                    if ds.has_variables:
                        print('subsetting with variables...')
                        is_subsetted = Utility.subset_netcdf(ds, "%s%s" % (PathManager.get_url('tmp',self.next_download_file), "_varib.nc"), new_name)
                    else:
                        print('subsetting...')
                        is_subsetted = Utility.subset_netcdf(ds, tmp_path, new_name)

                #IF CONVERT LON
                #if ds.convert_longitude:
                #    print('converting longitude....')
                #    Utility.reproject_netcdf(ds,tmp_path,new_name )
                
                #IF NOTHING
                if not ds.has_variables and not ds.subset and not ds.convert_longitude:
                    print('nothing to do, moving file...')
                    Utility.rename_file(tmp_name,PathManager.get_url('tmp',self.next_download_file))

                #MOVE FILES TO LOCAL OR SCP
                local_dir = ds.local_directory_path
                if ds.download_to_local_dir:
                    root_dir = PathManager.get_url('root-dir')
                    if "{root-dir}" in local_dir:
                        local_dir = local_dir.replace("{root-dir}", root_dir)
                    Utility.copy_file(PathManager.get_url('tmp',self.next_download_file), local_dir)
                    Utility.remove_file(PathManager.get_url('tmp',self.next_download_file))
            
                #CREATE_LATEST
                if ds.create_latest:
                    Utility.remove_file("%s/%s" % (local_dir, "latest.nc"))
                    Utility.copy_file("%s/%s" % (local_dir, self.next_download_file),"%s/%s" % (local_dir, "latest.nc"))

                #CLEANUP IF NEEDED, try
                Utility.remove_file(tmp_name)
                
                #SET STAUS
                download_complete = True
            else:
                download_complete = False
        except Exception as e:
            print(e)
            is_error = True
        return download_complete, is_error

    def download_obdaac(self,ds):
        #CHECK VARS
        download_complete = False
        is_error = False

        try:
            #GET URL
            url = ds.data_download_url
            
            #REPLACE WITH FILE TO DOWNLOAD
            url = url.replace('{download_file_name}', self.next_download_file)

            #CHECK
            substrings_to_remove = [ds.download_file_prefix, ds.download_file_suffix]
            new_string = Utility.remove_substrings(self.next_download_file, substrings_to_remove)
            newstr = new_string
            infixstr = ds.download_file_infix
            if "_" in new_string:
                newsplot = new_string.split("_")
                infix_split = ds.download_file_infix.split('_')
                newstr =  newsplot[0]
                infixstr = infix_split[0]
            convert_to_datetime = datetime.strptime(newstr, infixstr)
            
            #CHECK FOR YEAR
            if "{year}" in url:
                url = url.replace('{year}', convert_to_datetime.strftime('%Y'))
            #CHECK FOR MONTH
            if "{month}" in url:
                url = url.replace('{month}', convert_to_datetime.strftime('%m'))
            
            #CHECK FOR DAY
            if "{day}" in url:
                url = url.replace('{day}', convert_to_datetime.strftime('%d'))

            print(url)
            
            #DOWNLOAD FILE
            result = Utility.download_obdaac(url,PathManager.get_url('tmp',self.next_download_file))            
            
            if os.path.exists("%s/%s" % (PathManager.get_url('tmp'),self.next_download_file)):
                #RENAME TO TMP IN THE SAME DIR
                tmp_name = "%s%s" % (PathManager.get_url('tmp',self.next_download_file), "_tmp.nc")
                Utility.rename_file(PathManager.get_url('tmp',self.next_download_file), tmp_name)
                #PATHS
                tmp_path = tmp_name
                new_name = "%s" % (PathManager.get_url('tmp',self.next_download_file))
                
                #IF VARIABLES
                if ds.has_variables:
                    print('getting variables...')
                    Utility.get_variables(ds,tmp_path,"%s%s" % (PathManager.get_url('tmp',self.next_download_file), "_varib.nc"))
                
                #IF SUBSET
                if ds.subset:
                    if ds.has_variables:
                        print('subsetting with variables...')
                        is_subsetted = Utility.subset_netcdf(ds, "%s%s" % (PathManager.get_url('tmp',self.next_download_file), "_varib.nc"), new_name)
                    else:
                        print('subsetting...')
                        is_subsetted = Utility.subset_netcdf(ds, tmp_path, new_name)

                #IF NOTHING
                if not ds.has_variables and not ds.subset and not ds.convert_longitude:
                    print('nothing to do, moving file...')
                    Utility.rename_file(tmp_name,PathManager.get_url('tmp',self.next_download_file))

                #MOVE FILES TO LOCAL OR SCP
                local_dir = ds.local_directory_path
                if ds.download_to_local_dir:
                    root_dir = PathManager.get_url('root-dir')
                    if "{root-dir}" in local_dir:
                        local_dir = local_dir.replace("{root-dir}", root_dir)
                    Utility.copy_file(PathManager.get_url('tmp',self.next_download_file), local_dir)
                    Utility.remove_file(PathManager.get_url('tmp',self.next_download_file))
            
                #CREATE_LATEST
                if ds.create_latest:
                    Utility.remove_file("%s/%s" % (local_dir, "latest.nc"))
                    Utility.copy_file("%s/%s" % (local_dir, self.next_download_file),"%s/%s" % (local_dir, "latest.nc"))

                #CLEANUP IF NEEDED, try
                Utility.remove_file(tmp_name)
                """
                #IF SUBSET OR VARIBLES
                if ds.has_variables or ds.subset:
                    print('subsetting')
                    output_file = "%s" % (PathManager.get_url('tmp',self.next_download_file))
                    is_subsetted = Utility.subset_netcdf(ds, tmp_name, output_file)
                    if not is_subsetted:
                        Utility.rename_file(tmp_name,PathManager.get_url('tmp',self.next_download_file))

                else:
                    Utility.rename_file(tmp_name,PathManager.get_url('tmp',self.next_download_file))
                
                #MOVE FILES TO LOCAL OR SCP
                local_dir = ds.local_directory_path
                if ds.download_to_local_dir:
                    root_dir = PathManager.get_url('root-dir')
                    if "{root-dir}" in local_dir:
                        local_dir = local_dir.replace("{root-dir}", root_dir)
                    Utility.copy_file(PathManager.get_url('tmp',self.next_download_file), local_dir)
                    Utility.remove_file(PathManager.get_url('tmp',self.next_download_file))
                
                #REPROJECT FROM 0-360 to -180-180
                old_path = "%s/%s" % (local_dir, self.next_download_file)
                Utility.reproject_netcdf(ds,old_path,old_path )
                
                #CREATE LATEST
                if ds.create_latest:
                    Utility.copy_file("%s/%s" % (local_dir, self.next_download_file),"%s/%s" % (local_dir, "latest.nc"))

                #CLEANUP IF NEEDED, try
                Utility.remove_file(tmp_name)
                """

                download_complete = True
            else:
                download_complete = False
        except Exception as e:
            print(e)
            is_error = True
        return download_complete, is_error

    def download_copernicusmarine(self,ds):
        #CHECK VARS
        download_complete = False
        is_error = False
        try:
            #PREPARE PARAMETERS
            substrings_to_remove = [ds.download_file_prefix, ds.download_file_suffix]
            new_string = Utility.remove_substrings(self.next_download_file, substrings_to_remove)
            splitdates = new_string.split('_')
            infixsplit = ds.download_file_infix.split('_')

            #PARAMS TO BE PASSED
            start_datetime = datetime.strptime(splitdates[0], infixsplit[0])
            end_datetime = datetime.strptime(splitdates[1], infixsplit[1])
            dataset_id = ds.data_download_url
            varib = ds.variables
            variables = [x.strip() for x in varib.split(',')]
            new_file_name = self.next_download_file
            download_directory = ds.local_directory_path
            root_dir = PathManager.get_url('root-dir')

            #CHECK TO SEE IF THERE IS NEED TO CHANGE END DATE
            if ds.force_forecast:
                print('it is a 7-day forecast')
                end_datetime = Utility.add_time(end_datetime,0,ds.force_days, 0,0)
            
            if "{root-dir}" in download_directory:
                download_directory = download_directory.replace("{root-dir}", root_dir)

            #DOWNLOAD THE DATA
            get_data = Utility.download_from_copernicus(ds,dataset_id, variables, start_datetime, end_datetime,\
                new_file_name,download_directory)
            
            if get_data:
                #REPROJECT FROM 0-360 to -180-180
                old_path = "%s/%s" % (download_directory, new_file_name)
                Utility.reproject_netcdf(ds,old_path,old_path )
                #CREATE LATEST
                if ds.create_latest:
                    Utility.remove_file("%s/%s" % (download_directory, "latest.nc"))
                    Utility.copy_file("%s/%s" % (download_directory, new_file_name),"%s/%s" % (download_directory, "latest.nc"))
                download_complete = True
            else:
                download_complete = False
            
        except Exception as e:
            is_error = True
            print(e)
        return download_complete, is_error

    def merge_weekly_coral_bleaching_forecast(self,fpath, fout):
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

    def dataDownload(self):
        #GET DATASET
        url= PathManager.get_url('ocean-api','dataset')
        #dataset_url = "https://dev-oceanportal.spc.int/v1/api/dataset/%s" % (self.dataset_id)
        dataset_url = "%s/%s" % (url,self.dataset_id)
        print(dataset_url)
        ds = initialize_datasetController(dataset_url)

        #CHECK IF FILE EXISTS AND DOWNLOAD
        download_succeed, is_error = False, False

        #create product directory if it does not exist
        self.create_product_directory(ds)

        #SET TO RUNNING
        Utility.update_api(PathManager.get_url('ocean-api','task_download',str(self.id)), {"health":"Running", "attempt_count":"0"})
        
        if ds.download_method == 1:
            print('download with https')
            download_succeed, is_error = self.download_http(ds)
        elif ds.download_method == 2:
            print('downloading with obdaac..')
            download_succeed, is_error = self.download_obdaac(ds)
        elif ds.download_method == 3:
            print('downloading with copernicusmarine..')
            download_succeed, is_error = self.download_copernicusmarine(ds)
        elif ds.download_method == 4:
            print('downloading with bluelink..')
            download_succeed, is_error = self.download_bluelink(ds)
        else:
            print('nothing to download.')
        
        #COMPULSORY THINGS TO DO, UPDATE THE API
        new_file_name,new_download_time = self.generate_next_download_filename(ds)
        #download_succeed, is_error = True, False
        Utility.update_tasks(download_succeed, is_error, new_file_name,new_download_time,self,ds)
        
        print('data download completed.')
        return download_succeed, is_error


    def CalcOneMonthly(self,prelim=False,prelim_id=0,max_missing_days=5):
        nco = Nco()
        #GET DATASET
        url= PathManager.get_url('ocean-api','dataset')
        #dataset_url = "https://dev-oceanportal.spc.int/v1/api/dataset/%s" % (self.dataset_id)
        dataset_url = "%s/%s" % (url,self.dataset_id)
        ds = initialize_datasetController(dataset_url)

        #CHECK IF FILE EXISTS AND DOWNLOAD
        download_succeed, is_error = False, False
        if prelim:
            url= PathManager.get_url('ocean-api','task_download')
            tasks = initialize_taskController(url)
            prelim_task = []
            for task in tasks:
                if task.id == prelim_id:
                    prelim_task = task
            #GET PATH OF ORIG
            path_to_scan = ds.local_directory_path
            root_dir = PathManager.get_url('root-dir')
            if "{root-dir}" in path_to_scan:
                path_to_scan = path_to_scan.replace("{root-dir}", root_dir)

            #GET PRELIM PATH
            url= PathManager.get_url('ocean-api','dataset')
            dataset_url = "%s/%s" % (url,prelim_task.dataset_id)
            ds2 = initialize_datasetController(dataset_url)
            path_to_scan_prelim = ds2.local_directory_path
            root_dir = PathManager.get_url('root-dir')
            if "{root-dir}" in path_to_scan_prelim:
                path_to_scan_prelim = path_to_scan_prelim.replace("{root-dir}", root_dir)

            #FILE TO DOWNLOAD
            output_file_name = self.next_download_file
            year_month = output_file_name[len(ds.download_file_prefix): -len(ds.download_file_suffix) if ds.download_file_suffix else None]

            #LOOP TO GET
            year = int(year_month[:4])
            month = int(year_month[4:])

            # Get the first and last day of the month
            first_day = datetime(year, month, 1).date()
            # Get the last day of the month using the monthrange method
            last_day = datetime(year, month, 1).date().replace(day=28) + timedelta(days=4)  # this will give us a date in the next month
            last_day = last_day - timedelta(days=last_day.day)
            input_files = []
            missing_days = []
            # Loop through all the days in the month
            current_day = first_day
            while current_day <= last_day:
                input_file = os.path.join(path_to_scan, ds.download_file_prefix + current_day.strftime("%Y%m%d") + ds.download_file_suffix)
                if not os.path.exists(input_file):
                    input_file = os.path.join(path_to_scan_prelim, ds2.download_file_prefix + current_day.strftime("%Y%m%d") + ds2.download_file_suffix)
                if os.path.exists(input_file):
                    nco.ncks(
                        input=input_file,
                        output=input_file,
                        options=["--mk_rec_dmn", "time"]
                    )
                    input_files.append(input_file)
                else:
                    missing_days.append( current_day.strftime("%Y%m%d"))
                current_day += timedelta(days=1)
            new_out_path2 = path_to_scan_prelim.replace('daily', 'monthly')
            new_out_path = new_out_path2.replace('nrt', 'hindcast')
            if not os.path.exists(new_out_path):
                os.makedirs(new_out_path)
            if len(missing_days) < max_missing_days:
                nco.ncra(input=input_files, output=new_out_path+"/"+output_file_name)
                download_succeed = True
                print('Monthly calculated successfully')
            else:
                print('Not enough files to calculate monthly')
                download_succeed = False
                is_error = True

            #UPDATE API 
            if download_succeed:
                date_obj = datetime.strptime(year_month, "%Y%m")
                new_date_obj = date_obj + relativedelta(months=1)
                new_year_month = new_date_obj.strftime("%Y%m")
                new_file_name = ds.download_file_prefix+new_year_month+ds.download_file_suffix
                new_date_obj2 = date_obj + relativedelta(months=2)
                new_date_obj2 -= timedelta(days=1)
                Utility.update_tasks(download_succeed, is_error, new_file_name,new_date_obj2,self,ds)
        else:
            #GET PATH OF ORIG
            path_to_scan = ds.local_directory_path
            root_dir = PathManager.get_url('root-dir')
            if "{root-dir}" in path_to_scan:
                path_to_scan = path_to_scan.replace("{root-dir}", root_dir)

            #FILE TO DOWNLOAD
            output_file_name = self.next_download_file
            year_month_new = output_file_name[len(ds.download_file_prefix): -len(ds.download_file_suffix) if ds.download_file_suffix else None]
            year_month = ""
            if "_" in year_month_new:
                year_month = year_month_new.split("_")[0]
            else:
                year_month = year_month_new

            year = int(year_month[:4])
            month = int(year_month[4:])
            
            # Get the first and last day of the month
            first_day = datetime(year, month, 1).date()
            # Get the last day of the month using the monthrange method
            last_day = datetime(year, month, 1).date().replace(day=28) + timedelta(days=4)  # this will give us a date in the next month
            last_day = last_day - timedelta(days=last_day.day)
            input_files = []
            missing_days = []
            # Loop through all the days in the month
            current_day = first_day
            while current_day <= last_day:
                if "_" in year_month_new:
                    input_file = os.path.join(path_to_scan, ds.download_file_prefix + current_day.strftime("%Y%m%d")+"_"+current_day.strftime("%Y%m%d") + ds.download_file_suffix)
                    
                else:
                    input_file = os.path.join(path_to_scan, ds.download_file_prefix + current_day.strftime("%Y%m%d") + ds.download_file_suffix)
                if os.path.exists(input_file):
                    #nco.ncks(
                    #    input=input_file,
                    #    output=input_file,
                    #    options=["--mk_rec_dmn", "time"]
                    #)
                    input_files.append(input_file)
                else:
                    missing_days.append( current_day.strftime("%Y%m%d"))
                current_day += timedelta(days=1)
            
            new_out_path2 = path_to_scan.replace('daily', 'monthly')
            new_out_path = new_out_path2.replace('nrt', 'hindcast')
            #CREATE NEW DIR
            if not os.path.exists(new_out_path):
                os.makedirs(new_out_path)

            #EXECUTE
            if len(missing_days) < max_missing_days:
                nco.ncra(input=input_files, output=new_out_path+"/"+output_file_name)
                download_succeed = True
                print('Monthly calculated successfully')
            else:
                print('Not enough files to calculate monthly')
                download_succeed = False
                is_error = True

            #UPDATE API 
            if download_succeed:
                date_obj = datetime.strptime(year_month, "%Y%m")
                new_date_obj = date_obj + relativedelta(months=1)
                new_year_month = new_date_obj.strftime("%Y%m")
                new_file_name = ds.download_file_prefix+new_year_month+ds.download_file_suffix
                if "_" in year_month_new:
                    new_file_name = ds.download_file_prefix+new_year_month+"_"+new_year_month+ds.download_file_suffix
                new_date_obj2 = date_obj + relativedelta(months=2)
                new_date_obj2 -= timedelta(days=1)
                Utility.update_tasks(download_succeed, is_error, new_file_name,new_date_obj,self,ds)
        return

    def Calc3Monthly(self,prelim=False,prelim_id=0,max_missing_days=5):
        nco = Nco()
        #GET DATASET
        url= PathManager.get_url('ocean-api','dataset')
        #dataset_url = "https://dev-oceanportal.spc.int/v1/api/dataset/%s" % (self.dataset_id)
        dataset_url = "%s/%s" % (url,self.dataset_id)
        ds = initialize_datasetController(dataset_url)

        #CHECK IF FILE EXISTS AND DOWNLOAD
        download_succeed, is_error = False, False
        if prelim:
            url= PathManager.get_url('ocean-api','task_download')
            tasks = initialize_taskController(url)
            prelim_task = []
            for task in tasks:
                if task.id == prelim_id:
                    prelim_task = task
            #GET PATH OF ORIG
            path_to_scan = ds.local_directory_path
            root_dir = PathManager.get_url('root-dir')
            if "{root-dir}" in path_to_scan:
                path_to_scan = path_to_scan.replace("{root-dir}", root_dir)

            #GET PRELIM PATH
            url= PathManager.get_url('ocean-api','dataset')
            dataset_url = "%s/%s" % (url,prelim_task.dataset_id)
            ds2 = initialize_datasetController(dataset_url)
            path_to_scan_prelim = ds2.local_directory_path
            root_dir = PathManager.get_url('root-dir')
            if "{root-dir}" in path_to_scan_prelim:
                path_to_scan_prelim = path_to_scan_prelim.replace("{root-dir}", root_dir)

            #FILE TO DOWNLOAD
            output_file_name = self.next_download_file
            year_month = output_file_name[len(ds.download_file_prefix): -len(ds.download_file_suffix) if ds.download_file_suffix else None]
            datesplit = year_month.split("_")
            first_month = datesplit[0]
            last_month = datesplit[1]
            first_year = int(first_month[:4])
            first_month = int(first_month[4:])
            last_year = int(last_month[:4])
            last_month = int(last_month[4:])

            first_day = datetime(first_year, first_month, 1).date()
            last_day = datetime(last_year, last_month, 1).date().replace(day=28) + timedelta(days=4)  # this will give us a date in the next month
            last_day = last_day - timedelta(days=last_day.day)
            
            input_files = []
            missing_days = []
            # Loop through all the days in the month
            current_day = first_day
            while current_day <= last_day:
                input_file = os.path.join(path_to_scan, ds.download_file_prefix + current_day.strftime("%Y%m%d") + ds.download_file_suffix)
                if not os.path.exists(input_file):
                    input_file = os.path.join(path_to_scan_prelim, ds2.download_file_prefix + current_day.strftime("%Y%m%d") + ds2.download_file_suffix)
                if os.path.exists(input_file):
                    input_files.append(input_file)
                else:
                    missing_days.append( current_day.strftime("%Y%m%d"))
                current_day += timedelta(days=1)
            new_out_path2 = path_to_scan_prelim.replace('daily', '3monthly')
            new_out_path = new_out_path2.replace('nrt', 'hindcast')
            if not os.path.exists(new_out_path):
                os.makedirs(new_out_path)
            if len(missing_days) < max_missing_days:
                nco.ncra(input=input_files, output=new_out_path+"/"+output_file_name)
                download_succeed = True
                print('Monthly calculated successfully')
            else:
                print('Not enough files to calculate monthly, total missing: '+str(len(missing_days)))
                download_succeed = False
                is_error = True

            #UPDATE API 
            if download_succeed:
                firstdate_obj = datetime.strptime(datesplit[0], "%Y%m")
                first_new_date_obj = firstdate_obj + relativedelta(months=1)
                firstnew_year_month = first_new_date_obj.strftime("%Y%m")

                last_date_obj = datetime.strptime(datesplit[1], "%Y%m")
                last_new_date_obj = last_date_obj + relativedelta(months=1)
                last_new_year_month = last_new_date_obj.strftime("%Y%m")

                new_file_name = ds.download_file_prefix+firstnew_year_month+"_"+last_new_year_month+ds.download_file_suffix
                new_date_obj2 = last_date_obj + relativedelta(months=2)
                new_date_obj2 -= timedelta(days=1)
                Utility.update_tasks(download_succeed, is_error, new_file_name,new_date_obj2,self,ds)
        else:
            #GET PATH OF ORIG
            path_to_scan = ds.local_directory_path
            root_dir = PathManager.get_url('root-dir')
            if "{root-dir}" in path_to_scan:
                path_to_scan = path_to_scan.replace("{root-dir}", root_dir)

            #FILE TO DOWNLOAD
            output_file_name = self.next_download_file
            year_month_new = output_file_name[len(ds.download_file_prefix): -len(ds.download_file_suffix) if ds.download_file_suffix else None]
            year_month = ""
            if "_" in year_month_new:
                year_month = year_month_new.split("_")[0]
            else:
                year_month = year_month_new

            year = int(year_month[:4])
            month = int(year_month[4:])
            
            # Get the first and last day of the month
            first_day = datetime(year, month, 1).date()
            # Get the last day of the month using the monthrange method
            last_day = datetime(year, month, 1).date().replace(day=28) + timedelta(days=4)  # this will give us a date in the next month
            last_day = last_day - timedelta(days=last_day.day)
            input_files = []
            missing_days = []
            # Loop through all the days in the month
            current_day = first_day
            while current_day <= last_day:
                if "_" in year_month_new:
                    input_file = os.path.join(path_to_scan, ds.download_file_prefix + current_day.strftime("%Y%m%d")+"_"+current_day.strftime("%Y%m%d") + ds.download_file_suffix)
                    
                else:
                    input_file = os.path.join(path_to_scan, ds.download_file_prefix + current_day.strftime("%Y%m%d") + ds.download_file_suffix)
                if os.path.exists(input_file):
                    nco.ncks(
                        input=input_file,
                        output=input_file,
                        options=["--mk_rec_dmn", "time"]
                    )
                    input_files.append(input_file)
                else:
                    missing_days.append( current_day.strftime("%Y%m%d"))
                current_day += timedelta(days=1)
            
            new_out_path = path_to_scan.replace('daily', 'monthly')

            #CREATE NEW DIR
            if not os.path.exists(new_out_path):
                os.makedirs(new_out_path)

            #EXECUTE
            if len(missing_days) < max_missing_days:
                nco.ncra(input=input_files, output=new_out_path+"/"+output_file_name)
                download_succeed = True
                print('Monthly calculated successfully')
            else:
                print('Not enough files to calculate monthly')
                download_succeed = False
                is_error = True

            #UPDATE API 
            if download_succeed:
                date_obj = datetime.strptime(year_month, "%Y%m")
                new_date_obj = date_obj + relativedelta(months=1)
                new_year_month = new_date_obj.strftime("%Y%m")
                new_file_name = ds.download_file_prefix+new_year_month+ds.download_file_suffix
                if "_" in year_month_new:
                    new_file_name = ds.download_file_prefix+new_year_month+"_"+new_year_month+ds.download_file_suffix
                new_date_obj2 = date_obj + relativedelta(months=2)
                new_date_obj2 -= timedelta(days=1)
                Utility.update_tasks(download_succeed, is_error, new_file_name,new_date_obj,self,ds)
        return
    

    def addClimatology(self, orig_file, output_file):
        root_dir = PathManager.get_url('root-dir')
        cli_suffix = "/model/regional/noaa/climatology/"
        cli_path = root_dir + cli_suffix

        with nc.Dataset(orig_file, "r") as ds_anom:
            sst_anom = ds_anom.variables['sst'][:]
            lat = ds_anom.variables['lat'][:]
            lon = ds_anom.variables['lon'][:]
            time = ds_anom.variables['time'][:]
            
            lon_grid, lat_grid = np.meshgrid(lon, lat)
            epoch = datetime(1970, 1, 1)
            
            # Create output file outside the time loop
            with nc.Dataset(output_file, 'w', format='NETCDF4') as ds_out:
                # Create dimensions - add time first
                ds_out.createDimension('time', None)  # Unlimited dimension
                ds_out.createDimension('lat', len(lat))
                ds_out.createDimension('lon', len(lon))
                
                # Coordinate variables - add time variable
                v_time = ds_out.createVariable('time', 'f4', ('time',))
                v_lat = ds_out.createVariable('lat', 'f4', ('lat',))
                v_lon = ds_out.createVariable('lon', 'f4', ('lon',))
                
                # Data variables - add time dimension to all variables
                v_sst = ds_out.createVariable('sst_total', 'f4', ('time', 'lat', 'lon'), zlib=True)
                v_clim = ds_out.createVariable('sst_clim', 'f4', ('time', 'lat', 'lon'), zlib=True)
                v_current_contour = ds_out.createVariable('current_29C', 'i1', ('time', 'lat', 'lon'), zlib=True)
                v_clim_contour = ds_out.createVariable('clim_29C', 'i1', ('time', 'lat', 'lon'), zlib=True)
                
                # Add attributes
                v_time.units = "days since 1970-01-01"  # Match your input time units
                v_time.long_name = "Time"
                v_lat.units = "degrees_north"
                v_lat.long_name = "Latitude"
                v_lon.units = "degrees_east"
                v_lon.long_name = "Longitude"
                v_sst.units = "°C"
                v_sst.long_name = "Total Sea Surface Temperature"
                v_clim.units = "°C"
                v_clim.long_name = "Climatology Sea Surface Temperature"
                v_current_contour.units = "1 (SST = 29±0.1°C), 0 otherwise"
                v_current_contour.long_name = "Current SST 29°C Exact Contour Mask"
                v_clim_contour.units = "1 (Climatology SST = 29±0.1°C), 0 otherwise"
                v_clim_contour.long_name = "Climatology SST 29°C Exact Contour Mask"
                
                # Assign coordinate data
                v_lat[:] = lat
                v_lon[:] = lon
                
                for t in range(sst_anom.shape[0]):
                    try:
                        time_step = epoch + timedelta(int(time[t]))
                        month = time_step.month
                        year = time_step.year
                        
                        # Load climatology
                        clim_file = f"{cli_path}clim-oisst-avhrr-sst-v02r01_1982-2018.{month:02d}.nc"
                        with nc.Dataset(clim_file) as ds_clim:
                            sst_clim = ds_clim.variables['sst'][0, 0, :, :]
                        
                        # Calculate total SST
                        sst_total = sst_anom[t, :, :] + sst_clim
                        
                        # Create exact 29°C contour masks
                        tolerance = 0.1
                        current_contour = np.where(np.abs(sst_total - 29) <= tolerance, 1, 0).astype('i1')
                        clim_contour = np.where(np.abs(sst_clim - 29) <= tolerance, 1, 0).astype('i1')
                        
                        # Write data for this time step
                        v_time[t] = time[t]  # Copy time value from input
                        v_sst[t, :, :] = sst_total
                        v_clim[t, :, :] = sst_clim
                        v_current_contour[t, :, :] = current_contour
                        v_clim_contour[t, :, :] = clim_contour
                        
                    except Exception as e:
                        print(f'Error occurred at time step {t}: {str(e)}')
                        continue
    """
    def addClimatology(self,orig_file,output_file):
        root_dir = PathManager.get_url('root-dir')
        cli_suffix = "/model/regional/noaa/climatology/"
        cli_path = root_dir+cli_suffix

        with nc.Dataset(orig_file, "r") as ds_anom:
            sst_anom = ds_anom.variables['sst'][:]
            lat = ds_anom.variables['lat'][:]
            lon = ds_anom.variables['lon'][:]
            time = ds_anom.variables['time'][:]
            
            lon_grid, lat_grid = np.meshgrid(lon, lat)
            epoch = datetime(1970, 1, 1)
            
            for t in range(sst_anom.shape[0]):
                try:
                    time_step = epoch + timedelta(int(time[t]))
                    month = time_step.month
                    year = time_step.year
                    
                    # Load climatology
                    clim_file = f"{cli_path}clim-oisst-avhrr-sst-v02r01_1982-2018.{month:02d}.nc"
                    with nc.Dataset(clim_file) as ds_clim:
                        sst_clim = ds_clim.variables['sst'][0, 0, :, :]
                    
                    # Calculate total SST
                    sst_total = sst_anom[t, :, :] + sst_clim
                    
                    # Create exact 29°C contour masks (with tolerance for floating point comparison)
                    tolerance = 0.1  # Degrees C tolerance for identifying 29°C contour
                    current_contour = np.where(np.abs(sst_total - 29) <= tolerance, 1, 0).astype('i1')
                    clim_contour = np.where(np.abs(sst_clim - 29) <= tolerance, 1, 0).astype('i1')
                    
                    # Save to NetCDF with proper attributes
                    #output_file = f"{output_path}sst_climatology_combined.nc"
                    with nc.Dataset(output_file, 'w', format='NETCDF4') as ds_out:
                        ds_out.createDimension('lat', len(lat))
                        ds_out.createDimension('lon', len(lon))
                        
                        # Coordinate variables
                        v_lat = ds_out.createVariable('lat', 'f4', ('lat',))
                        v_lon = ds_out.createVariable('lon', 'f4', ('lon',))
                        
                        # Data variables
                        v_sst = ds_out.createVariable('sst_total', 'f4', ('lat', 'lon'), zlib=True)
                        v_clim = ds_out.createVariable('sst_clim', 'f4', ('lat', 'lon'), zlib=True)
                        v_current_contour = ds_out.createVariable('current_29C', 'i1', ('lat', 'lon'), zlib=True)
                        v_clim_contour = ds_out.createVariable('clim_29C', 'i1', ('lat', 'lon'), zlib=True)
                        
                        # Add attributes
                        v_lat.units = "degrees_north"
                        v_lat.long_name = "Latitude"
                        v_lon.units = "degrees_east"
                        v_lon.long_name = "Longitude"
                        v_sst.units = "°C"
                        v_sst.long_name = "Total Sea Surface Temperature"
                        v_clim.units = "°C"
                        v_clim.long_name = "Climatology Sea Surface Temperature"
                        v_current_contour.units = "1 (SST = 29±0.1°C), 0 otherwise"
                        v_current_contour.long_name = "Current SST 29°C Exact Contour Mask"
                        v_clim_contour.units = "1 (Climatology SST = 29±0.1°C), 0 otherwise"
                        v_clim_contour.long_name = "Climatology SST 29°C Exact Contour Mask"
                        
                        # Assign data
                        v_lat[:] = lat
                        v_lon[:] = lon
                        v_sst[:] = sst_total
                        v_clim[:] = sst_clim
                        v_current_contour[:] = current_contour
                        v_clim_contour[:] = clim_contour
                    
                except Exception as e:
                    print('Error occured'+e)
                    continue
    """
    def CalcDecile(self):
        url= PathManager.get_url('ocean-api','dataset')
        #dataset_url = "https://dev-oceanportal.spc.int/v1/api/dataset/%s" % (self.dataset_id)
        dataset_url = "%s/%s" % (url,self.dataset_id)
        dset = initialize_datasetController(dataset_url)
        path_to_scan = dset.local_directory_path
        root_dir = PathManager.get_url('root-dir')
        if "{root-dir}" in path_to_scan:
            path_to_scan = path_to_scan.replace("{root-dir}", root_dir)
        
        path_to_scan2 = path_to_scan.replace("daily", "monthly")
        input_path = path_to_scan2.replace("nrt", "hindcast")
        out_path = path_to_scan2.replace("monthly", "decile")
        if not os.path.exists(out_path):
            os.makedirs(out_path)
        
        download_succeed, is_error = False, False
        output_file_name = self.next_download_file
        year_monthtmp = output_file_name[len(dset.download_file_prefix): -len(dset.download_file_suffix) if dset.download_file_suffix else None]
        year_month = year_monthtmp.replace('decile.', '')

        year = int(year_month[:4])
        month = int(year_month[4:])

        files = sorted(glob.glob(os.path.join(input_path, "*.nc")))

        variable_name = "sst"  
        
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
        month_indices = [i for i, t in enumerate(time_array) if t.month == month]
        month_data = data_array[month_indices, :, :]

        # Compute deciles (percentiles 10, 20, ..., 90)
        percentiles = [10 * i for i in range(1, 10)]
        deciles = np.percentile(month_data, percentiles, axis=0)

        # Get the latest SST for the chosen month and year
        latest_sst = data_array[[i for i, t in enumerate(time_array) if t.year == year and t.month == month], :, :][-1, :, :]

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
        output_file = os.path.join(out_path, output_file_name)
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
        
        print('Decile computed successfully.')
        download_succeed = True
        if download_succeed:
            date_obj = datetime.strptime(year_month, "%Y%m")
            new_date_obj = date_obj + relativedelta(months=1)
            new_year_month = new_date_obj.strftime("%Y%m")
            new_file_name = dset.download_file_prefix+"decile."+new_year_month+dset.download_file_suffix
            new_date_obj2 = date_obj + relativedelta(months=2)
            new_date_obj2 -= timedelta(days=1)
            Utility.update_tasks(download_succeed, is_error, new_file_name,new_date_obj2,self,ds)
        #print(year,month)


#LOAD VARIABLES FROM API INTO THE CLASS
def initialize_taskController(url):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        enqueue = []
        for item in data:
            queue = taskController(item['id'],item['task_name'].strip(), item['class_id'].strip(), item['dataset_id'],item['status'].strip(),\
                                item['priority'].strip(),\
                                    item['duration'].strip(),item['task_start_time'],item['next_run_time'],\
                                        item['last_run_time'],item['next_download_file'].strip(),item['last_download_file'].strip(),\
                                        item['enabled'],item['health'].strip(),item['fail_count'],item['success_count'],item['reset_count'],\
                                            item['attempt_count'],item['predecessor_class'],item['predecessor_class_id'],item['successor_class'],\
                                            item['successor_class_id'],\
                                                item['created_by'].strip(),item['launched_by'].strip(),item['retain'],item['retention_days'])
            enqueue.append(queue)
        return enqueue
    else:
        print("Failed to retrieve data: {response.status_code}")
        return None
