import os, sys
current_directory = os.path.dirname(os.path.abspath(__file__))
tasks_code_path = os.path.join(current_directory, 'code')
sys.path.append(tasks_code_path)
from controller_task import initialize_taskController
from datetime import datetime
from utility_functions import Utility
from controller_server_path import PathManager
from update_thredds import thredds


def task_1():
    url= PathManager.get_url('ocean-api','task_download')
    tasks = initialize_taskController(url)
    
    for task in tasks:
        if task.class_id == "download":
            execute = Utility.time_diff(datetime.now(),datetime.strptime(task.next_run_time,"%Y-%m-%dT%H:%M:%SZ"))
            #REMOVE THIS IN PROD
            #execute = True
            if task.id == 2 and execute:
                print('Executing Task No.%s - %s' % (task.id, task.task_name))
                task.dataDownload()
            else:
                print('nothing to do.')

def task_2():
    layer_id = [2,11,13,10,14]

    for id in layer_id:
        api_url = PathManager.get_url('ocean-api',"layer_web_map/"+str(id)+"/")
        
        api_response = thredds.get_data_from_api(api_url)
        if api_response['period'] == "PT6H":
            thredds.get_6hourly(api_response)
        else:
            raise ValueError("Dataset Period not found.")


def bom_forecast_hourly_ww3():
    task_1()
    task_2()
    return

