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
            if task.id == 3 and execute:
                print('Executing Task No.%s - %s' % (task.id, task.task_name))
                task.dataDownload()
            else:
                print('nothing to do.')

def task_2():
    layer_id = [15]

    for id in layer_id:
        api_url = PathManager.get_url('ocean-api',"layer_web_map/"+str(id)+"/")
        
        api_response = thredds.get_data_from_api(api_url)
        if api_response['period'] == "COMMA":
            thredds.get_specific(api_response)
        else:
            raise ValueError("Dataset Period not found.")

def nasa_nrt_daily_choloropphyll():
    task_1()
    task_2()
    return

