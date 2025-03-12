import os
import json

class PathManager:
    # Get the path two folders back
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Path to the JSON config file
    config_path = os.path.join(base_dir, 'config.json')
    
    # Load the JSON config file
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
    
    # Extract paths from the JSON config
    base_dir = config.get('base_dir', '/default/path/if/not/found')
    datasets = config.get('datasets', '/default/path/if/not/found')
    
    URLS = {
        'ocean-api': config.get('ocean-api', 'https://default-api.url'),
        'tmp': os.path.join(base_dir, 'tmp'),
        'odbaac': base_dir,
        'copernicus-credentials': os.path.join(base_dir, '.copernicusmarine', '.copernicusmarine-credentials'),
        'token-credentials': os.path.join(base_dir, 'token.txt'),
        'root-dir': datasets
    }

    @classmethod
    def get_url(cls, key, *args):
        """Constructs a URL by joining the specified base URL with the provided arguments."""
        if key not in cls.URLS:
            raise ValueError(f"Invalid key '{key}'. Available keys: {list(cls.URLS.keys())}")
        return "/".join([cls.URLS[key]] + list(args))

# Example usage:
# print(PathManager.get_url('tmp', 'subfolder', 'file.txt'))
"""

class PathManager:
    # Hardcoded URL paths
    base_dir = "/home/pop/Desktop/pipeline/smart_data/tasks/code"
    #base_dir = "/home/pop/Desktop/airflow/oceanobs/airflow_deploy/dags/code"
    #datasets = "/opt/airflow/ocean_portal/datasets"
    datasets = "/home/pop/ocean_portal/datasets"
    #datasets = "/home/pop/ocean_portal/datasets"
    URLS = {
        'ocean-api': 'https://dev-oceanportal.spc.int/middleware/api',
        'tmp': base_dir+'/tmp',
        'odbaac': base_dir,
        'copernicus-credentials': base_dir+'/.copernicusmarine/.copernicusmarine-credentials',
        'token-credentials': base_dir+'/token.txt',
        'root-dir': datasets
    }

    @classmethod
    def get_url(cls, key, *args):
        if key not in cls.URLS:
            raise ValueError(f"Invalid key '{key}'. Available keys: {list(cls.URLS.keys())}")
        return "/".join([cls.URLS[key]] + list(args))"
"""