import os
import json
from dotenv import load_dotenv
import utils as utils

def get_endpoints():
    '''Get endpoints from NOAA API, cache endpoints in a blob'''
    
    # Load environment variables
    load_dotenv()
    locations = json.loads(os.getenv("LOCATIONS"))
    PURPOSE = os.getenv("PURPOSE")
    EMAIL = os.getenv("EMAIL")
    
    # API url for location metadata, header for requests
    metadata_url = 'https://api.weather.gov/points/'
    header = {'User-Agent' : (f'{PURPOSE}, {EMAIL}')}
    
    # Forecast type:
    forecast_type = 'forecastGridData'

    # Paths and filenames
    container_name = 'skiforcast'
    blob_name = 'noaa_api_endpoints.json'

    # Get endpoints
    try:
        cache = utils.APIEndpoints(locations, metadata_url, header, forecast_type, container_name, blob_name)
        endpoints = cache.get_endpoints()

        # Handle missed endpoints
        if len(endpoints) != len(locations):
            for key in (locations.keys() - endpoints.keys()):
                if key in locations.keys():
                    del locations[key]
    
    except Exception as e:
        print(f'Error in get_endpoints: \n{e}\n')
        endpoints = None

    return endpoints