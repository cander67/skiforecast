import os
import json
from dotenv import load_dotenv
#from azure.identity import DefaultAzureCredential
import src.utils as utils

def proc_forecasts(default_credential, time, forecasts):
    '''Create table data from forecast data
    Args:
        time (datetime): Current time
        forecasts (dict): Dictionary of location: blob names
    Returns:
        table (Table): Table object'''
    
    # Load environment variables
    load_dotenv()

    # Define parameters
    locations = json.loads(os.getenv("LOCATIONS"))
    time_periods = json.loads(os.getenv("TIME_PERIODS"))
    properties = json.loads(os.getenv("PROPERTIES"))
    account_url = os.getenv("BLOB_ACCOUNT_URL")
    #default_credential = DefaultAzureCredential()
    container_name = "skiforecast"
    
    # Create table data from forecast data
    # Create Table object
    table = utils.Table()
    # Create table columns
    table.create_columns(time)

    for location in locations.keys():
        try:
            blob_data = utils.readblob(forecasts[location], container_name, account_url, default_credential)
            blob_data = json.loads(blob_data.decode())
        except Exception as e:
            print(f'Error reading forecast, {location}: {e}')

        # Instantiate TableData object
        setup = utils.TableData(time, location, time_periods, properties)
        
        # Parse forecast data
        try:
            parsed = setup.parse_forecast(blob_data)
        except Exception as e:
            print(f'Error parsing forecast, {location}: {e}')

        # Calculate table data
        try:
            table_data = setup.calculate_table_data(parsed)
        except Exception as e:
            print(f'Error calculating table data, {location}: {e}')

        # Create table row
        try:
            row = setup.create_row(table_data)
        except Exception as e:
            print(f'Error creating table row, {location}: {e}')

        # Append row to table
        table.append_row(row)

    return table.get_table()