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

download_succeed, is_error = False, False
def task_1():
    url = PathManager.get_url('ocean-api', 'task_download')
    tasks = initialize_taskController(url)

    for task in tasks:
        if task.class_id == "download":
            execute = Utility.time_diff(datetime.now(), datetime.strptime(task.next_run_time, "%Y-%m-%dT%H:%M:%SZ"))
            if task.id == 6 and execute:
                print(f"Executing Task No.{task.id} - {task.task_name}")
                download_succeed, is_error = task.dataDownload()
                task_3(download_succeed)
                return download_succeed
            else:
                print('Nothing to do.')

def task_2():
  layer_id = [19]

  for id in layer_id:
      api_url = PathManager.get_url('ocean-api',"layer_web_map/"+str(id)+"/")
      
      api_response = thredds.get_data_from_api(api_url)
      if api_response['period'] == "OPENDAP":
          thredds.get_specific_stamp_bureau(api_response)
      else:
          raise ValueError("Dataset Period not found.")

def task_3(download_succeed):
    root_dir = PathManager.get_url('root-dir')
    api_url = PathManager.get_url('ocean-api',"dataset/"+str(6)+"/")
    response = requests.get(api_url)
    data = response.json()

    api_path = data['local_directory_path']
    new_text = api_path.replace("{root-dir}", "")
    usable_path = "%s%s" % (root_dir,new_text)
    print(usable_path)
    if download_succeed:
        print("Merging results...")
        fpath = usable_path+"/latest.nc"
        fout = usable_path+"/latest_merged.nc"
        Utility.merge_weekly_coral_bleaching_forecast(fpath,fout)
        print(f"Merging {fpath} into {fout}")
    else:
        print("Skipping merging due to errors or failed download.")


def noaa_forecast_monthly_coral_bleaching():
    task_1()
    task_2()
    return
