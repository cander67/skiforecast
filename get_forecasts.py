import os
import json
from dotenv import load_dotenv
import get_endpoints as get_endpoints
import utils as utils
import logging

def get_forecasts(default_credential, endpoints):
    '''Get forecast data for ski area locations, save to blob, return list of blob names
    Args:
        endpoints (dict): Dictionary of endpoints for each location
    Returns:
        forecast_blobs (dict): Dict of location:blob names for retrieved forecasts'''

    # Load environment variables
    load_dotenv(".env")
    locations = json.loads(os.getenv("LOCATIONS"))
    PURPOSE = os.getenv("PURPOSE")
    EMAIL = os.getenv("EMAIL")

    # API url for location metadata, header for requests
    header = {'User-Agent' : (f'{PURPOSE}, {EMAIL}')}

    # Define parameters
    account_url = os.getenv('BLOB_ACCOUNT_URL')
    #default_credential = DefaultAzureCredential()
    container_name = "skiforecast"

    # Fetch forecast data
    # Get forecast data for each location, confirm successful download, save raw as .json
    fails = {} # Accumulate fails in dictionary {location: (HTTPStatus, HTTPError)}
    resolved = {} # Accumulate resolved fails in dictionary {location: 'location'}
    forecast_blobs = {} # Accumulate forecast blob names in list

    for location in locations.keys():
        location_details = locations[location]
        endpoint = endpoints[location]
        blob_name = f'{location}_gridData.json'
        forecast = utils.GridData(location, location_details, endpoint, header)
        data = forecast.get_forecast()
        response = forecast.get_status()
        if response[1] == False:
            utils.writeblob(blob_name, data, container_name, account_url, default_credential)
            forecast_blobs[location] = f'{location}_gridData.json'
        elif response[1] == True:
            fails[location] = response

    location = None
    location_details = None
    endpoint = None

    # Handle missed endpoints
    # Pass if all endpoints are found
    if len(fails) == 0:
        logging.info(f'\n\nFAILS: {fails}\n\n')
        pass
    # If endpoints are missing, attempt to resolve
    elif len(fails) > 0:
        for location in fails.keys():
            http_status = fails[location][0]
            http_error = fails[location][1]
            location_details = locations[location]
            endpoint = endpoints[location]
            blob_name = f'{location}_gridData.json'
            if http_status == None and http_error == True:
                ep = get_endpoints.get_endpoints()
                forecast = utils.GridData(location, location_details, endpoint, header)
                data = forecast.get_forecast()
                response = forecast.get_status()
                logging.info(f'\n\nRESOLVED -- LOCATION: {location}, RESPONSE: {response}\n\n')
                if response == (200, False):
                    resolved[location] = location   # Remove location from list of failed endpoints
                    utils.writeblob(blob_name, data, container_name, account_url, default_credential)
                    forecast_blobs[location] = f'{location}_gridData.json'
                elif response[1] == True:
                    fails[location] = response      # Update fails list with new response
            elif http_status != None and ((300 <= http_status < 500) and http_error == True):
                ep = get_endpoints.get_endpoints()
                forecast = utils.GridData(location, location_details, endpoint, header)
                data = forecast.get_forecast()
                response = forecast.get_status()
                logging.info(f'\n\nRESOLVED -- LOCATION: {location}, RESPONSE: {response}\n\n')
                if response == (200, False):
                    resolved[location] = location   # Remove location from list of failed endpoints
                    utils.writeblob(blob_name, data, container_name, account_url, default_credential)
                    forecast_blobs[location] = f'{location}_gridData.json'
                elif response[1] == True:
                    fails[location] = response      # Update fails list with new response
            elif http_status != None and ((500 <= http_status < 600) and http_error == True):
                forecast = utils.GridData(location, location_details, endpoint, header)
                data = forecast.get_forecast()
                response = forecast.get_status()
                logging.info(f'\n\nRESOLVED -- LOCATION: {location}, RESPONSE: {response}\n\n')
                if response == (200, False):
                    resolved[location] = location   # Remove location from list of failed endpoints
                    utils.writeblob(blob_name, data, container_name, account_url, default_credential)
                    forecast_blobs[location] = f'{location}_gridData.json'
                elif response[1] == True:
                    fails[location] = response      # Update fails list with new response
        
            # If all fails are resolved, break loop
            if len(resolved.keys() - fails.keys()) == 0:
                fails = {}
                break
        
            location = None
            location_details = None
            endpoint = None

        # Return unresolved fails
        if len(fails) != 0:
            for key in (resolved.keys() - fails.keys()):
                if key in fails.keys():
                    del fails[key] 
            logging.info(f'\n\nUNRESOLVED FAILS: {fails}\n\n')

    return forecast_blobs
