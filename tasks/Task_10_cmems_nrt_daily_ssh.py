import os, sys
current_directory = os.path.dirname(os.path.abspath(__file__))
tasks_code_path = os.path.join(current_directory, 'code')
sys.path.append(tasks_code_path)
from controller_task import initialize_taskController
from datetime import datetime
from utility_functions import Utility
from controller_server_path import PathManager
from update_thredds import thredds
import requests

download_succeed, is_error = True, False

def task_1():
    url= PathManager.get_url('ocean-api','task_download')
    tasks = initialize_taskController(url)
    
    for task in tasks:
        if task.class_id == "download":
            execute = Utility.time_diff(datetime.now(),datetime.strptime(task.next_run_time,"%Y-%m-%dT%H:%M:%SZ"))
            #REMOVE THIS IN PROD
            #execute = True
            if task.id == 12 and execute:
                print('Executing Task No.%s - %s' % (task.id, task.task_name))
                fname = task.next_download_file
                task.dataDownload()
                task_3(download_succeed, fname,12)
            else:
                print('nothing to do.')

def task_2():
  layer_id = [17]

  for id in layer_id:
      api_url = PathManager.get_url('ocean-api',"layer_web_map/"+str(id)+"/")
      
      api_response = thredds.get_data_from_api(api_url)
      if api_response['period'] == "COMMA":
          thredds.get_specific(api_response)
      else:
          raise ValueError("Dataset Period not found.")


def task_3(download_succeed,file_name,id):
    root_dir = PathManager.get_url('root-dir')
    api_url = PathManager.get_url('ocean-api',"dataset/"+str(id)+"/")
    response = requests.get(api_url)
    data = response.json()

    api_path = data['local_directory_path']
    new_text = api_path.replace("{root-dir}", "")
    usable_path = "%s%s" % (root_dir,new_text)
    if download_succeed:
        orig_file = "%s/%s" % (usable_path, file_name)
        print("Multiplying results...")
        Utility.multiply_netcdf_values(orig_file,1000)
    else:
        print("Skipping merging due to errors or failed download.")

def task_4():
    url= PathManager.get_url('ocean-api','task_download')
    tasks = initialize_taskController(url)
    
    for task in tasks:
        if task.class_id == "calculate":
            execute = Utility.time_diff(datetime.now(),datetime.strptime(task.next_run_time,"%Y-%m-%dT%H:%M:%SZ"))
            if task.id == 21 and execute:
                task.CalcOneMonthly(prelim=False,prelim_id=8,max_missing_days=50)


def cmems_nrt_daily_ssh():
    task_1()
    task_2()
    task_4()
    return
