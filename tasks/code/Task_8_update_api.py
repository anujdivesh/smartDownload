from controller_task import initialize_taskController
from datetime import datetime
from utility_functions import Utility
from controller_server_path import PathManager
from update_thredds import thredds


if __name__ == "__main__":
    #layer_id = [2,11,13,10,12,14]
    layer_id = [6]

    for id in layer_id:
        api_url = PathManager.get_url('ocean-api',"layer_web_map/"+str(id)+"/")
        
        api_response = thredds.get_data_from_api(api_url)
        if api_response['period'] == "OPENDAP":
            thredds.get_specific_stamp_bureau(api_response)
        else:
            raise ValueError("Dataset Period not found.")

    

    """
    for x in api_response:
        if x['update_thredds']:
            url = x['url']
            period = x['period']

            #CHECK EACH
            if period == 'COMMA':
                thredds.get_specific(x)
            elif period == 'PT6H' or period == "P1D":
                thredds.get_6hourly(x)
            elif period == 'OPENDAP':
                thredds.get_specific_stamp(x)
    #REMOVE empty nc's
    Utility.remove_files_by_extension("/scripts/tmp","nc")
    """