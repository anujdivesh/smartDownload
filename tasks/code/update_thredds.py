import requests
import xml.etree.ElementTree as xmltree
from owslib.wms import WebMapService
import pandas as pd
from controller_server_path import PathManager
from datetime import datetime, timedelta
import xarray as xr
import pandas as pd

class thredds:
    @staticmethod
    def process_string(input_string):
        # Check if the string contains commas
        if ',' in input_string:
            # Split the string by commas and return the first item
            return input_string.split(',')[0]
        else:
            # Return the whole string if no commas are present
            return input_string

    @staticmethod
    def process_string_2(input_string):
        # Check if the string contains '/'
        if '/' in input_string:
            # Split the string by '/' and return the first part
            return input_string.split('/')[0]
        else:
            # Return the whole string if '/' is not present
            return input_string
    @staticmethod
    def process_string_3(input_string):
        # Check if the string contains '/'
        if '/' in input_string:
            # Split the string by '/' and return the third item (index 2) if it exists
            parts = input_string.split('/')
            return parts[1] if len(parts) > 1 else input_string
        else:
            # Return the whole string if '/' is not present
            return input_string

    @staticmethod
    def get_data_from_api(url, params=None, headers=None):
        """
        Fetches data from a specified API endpoint.

        Args:
            url (str): The API endpoint URL.
            params (dict, optional): A dictionary of query parameters to send with the request.
            headers (dict, optional): A dictionary of HTTP headers to send with the request.

        Returns:
            dict or None: The JSON response from the API if successful, None otherwise.
        """
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()  # Raise an error for bad responses (4xx or 5xx)

            return response.json()  # Return the JSON response as a dictionary
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    @staticmethod
    def update_api(url, data, headers=None):
        try:
            tokenPreGen = thredds.validate_or_generate_token()
            token=headers={"Authorization":"Bearer "+tokenPreGen}
            response = requests.put(url+"/", json=data, headers=token)
            response.raise_for_status()  # Raise an error for bad status codes
            return response.json()  # Return the response content as JSON
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")  # Handle HTTP errors
        except Exception as err:
            print(f"An error occurred: {err}")  # Handle other errors
        return None

    @staticmethod
    def get_6hourly(data):
        wms = WebMapService(data['url'], version="1.3.0")
        layer = thredds.process_string(data['layer_name'])
        time = wms[layer].dimensions['time']['values'][0]
        start_time, end_time, period = time.split('/')

        #UPDATE API
        data2 = {"timeIntervalStart": start_time, "timeIntervalEnd": end_time}
        thredds.update_api(PathManager.get_url('ocean-api','layer_web_map',str(data['id'])), data2)
        print('Task '+ str(data['id']) +' : DateRange updated successsfully.')

        return None

    @staticmethod
    def get_specific(data):
        wms = WebMapService(data['url'], version="1.3.0")
        layer = data['layer_name']
        time = wms[layer].dimensions['time']['values']
        start_time = thredds.process_string_2(time[0])
        end_time = thredds.process_string_3(time[-1])

        #UPDATE API
        data2 = {"timeIntervalStart": start_time, "timeIntervalEnd": end_time}
        thredds.update_api(PathManager.get_url('ocean-api','layer_web_map',str(data['id'])), data2)
        print('Task '+ str(data['id']) +' : DateRange updated successsfully.')

        return None

    @staticmethod
    def get_specific_stamp(data):
        wms = WebMapService(data['url'], version="1.3.0")
        layer = data['layer_name']
        time = wms[layer].dimensions['time']['values']
        url = data['url']
        new_text = url.replace("wms", "dodsC")
        ds = xr.open_dataset(new_text)
        values = ds['time'].values  # This retrieves the raw time values

        dates = [datetime.strptime(x.decode('utf-8'), '%Y-%m-%dT%H:%M:%SZ') for x in values]

        # Convert datetime objects to the desired format '%Y-%m-%dT%H:%M:%S'
        formatted_dates = [dt.strftime('%Y-%m-%dT%H:%M:%S') for dt in dates]

        # Join into a comma-separated string
        comma_separated_string = ', '.join(formatted_dates)
        #UPDATE API
        data2 = {"specific_timestemps": comma_separated_string, "timeIntervalStart": dates[0].isoformat(), "timeIntervalEnd": dates[-1].isoformat()}
        thredds.update_api(PathManager.get_url('ocean-api','layer_web_map',str(data['id'])), data2)
        print('Task '+ str(data['id']) +' : DateRange updated successsfully.')
        
        return None

    @staticmethod
    def get_specific_stamp_bureau(data):
        wms = WebMapService(data['url'], version="1.3.0")
        layer = data['layer_name']
        time = wms[layer].dimensions['time']['values']
        url = data['url']
        new_text = url.replace("wms", "dodsC")
        ds = xr.open_dataset(new_text)
        values = ds['time'].values  # This retrieves the raw time values

        dates = values.astype('datetime64[ms]').tolist()


        #dates = [datetime.strptime(x.decode('utf-8'), '%Y-%m-%dT%H:%M:%SZ') for x in values]

        # Convert datetime objects to the desired format '%Y-%m-%dT%H:%M:%S'
        formatted_dates = [dt.strftime('%Y-%m-%dT%H:%M:%S') for dt in dates]

        # Join into a comma-separated string
        comma_separated_string = ', '.join(formatted_dates)
        ##UPDATE API
        data2 = {"specific_timestemps": comma_separated_string, "timeIntervalStart": dates[0].isoformat(), "timeIntervalEnd": dates[-1].isoformat()}
        thredds.update_api(PathManager.get_url('ocean-api','layer_web_map',str(data['id'])), data2)
        print('Task '+ str(data['id']) +' : DateRange updated successsfully.')
        
        return None


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
        token = thredds.load_token()

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
        new_token = thredds.generate_token()

        if new_token:
            return new_token
        else:
            print("Error: Failed to generate a new token.")
            return None
"""
#MAIN
api_url = PathManager.get_url('ocean-api',"layer_web_map/")
api_response = get_data_from_api(api_url)

for x in api_response:
    if x['update_thredds']:
        url = x['url']
        period = x['period']

        #CHECK EACH
        if period == 'COMMA':
            get_specific(x)
        elif period == 'PT6H':
            get_6hourly(x)
        elif period == 'P1D':
            get_6hourly(x)
        elif period == 'OPENDAP':
            get_specific_stamp(x)

"""
