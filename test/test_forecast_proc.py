### Run in terminal: python3 -m test.test_forecast_proc

import os
import json
from datetime import datetime
#import bs4 as BeautifulSoup
from dotenv import load_dotenv
#from azure.identity import DefaultAzureCredential
#from azure.storage.blob import BlobServiceClient, ContainerClient, ContentSettings
from src import utils as utils
#import src.get_endpoints as get_endpoints
#import src.get_forecasts as get_forecasts
#import src.proc_forecasts as proc_forecasts

time = datetime.now()

# Load environment variables
load_dotenv()

# Define parameters
locations = json.loads(os.getenv("LOCATIONS"))
time_periods = json.loads(os.getenv("TIME_PERIODS"))
properties = json.loads(os.getenv("PROPERTIES"))

path = 'test/'

files = {
    "Mt. Baker": "Mt. Baker_gridData.json",
    "Loup Loup": "Loup Loup_gridData.json",
    "Stevens Pass": "Stevens Pass_gridData.json",
    "Snoqualmie Pass": "Snoqualmie Pass_gridData.json",
    "Mission Ridge": "Mission Ridge_gridData.json",
    "White Pass": "White Pass_gridData.json",
    "Crystal Mountain": "Crystal Mountain_gridData.json"
}

#for location in files.keys:
with open(f'{path}{files["Loup Loup"]}', 'r') as f:
    data = json.load(f)
f.close()

# Instantiate TableData object
setup = utils.TableData(time, "Loup Loup", time_periods, properties)

# Parse forecast data
try:
    parsed = setup.parse_forecast(data)
except Exception as e:
    print(f'Error parsing forecast, {"Loup Loup"}: {e}')

# Print parsed forecast data as json
with open(f'{path}Loup Loup_parsed.json', 'w') as f:
    print(json.dumps(parsed, sort_keys=False, indent=4), file = f)
f.close()

# Calculate table data
try:
    table_data = setup.calculate_table_data(parsed)
except Exception as e:
    print(f'Error calculating table data, {"Loup Loup"}: {e}')

# Print table data as json
with open(f'{path}Loup Loup_table_data.json', 'w') as f:
    print(json.dumps(table_data, sort_keys=False, indent=4), file = f)
f.close()

# Create table row
try:
    row = setup.create_row(table_data)
    #print(f'ROW: {row}')
except Exception as e:
    print(f'Error creating table row: {e}')