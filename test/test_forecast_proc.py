### Run in terminal: python3 -m test.test_forecast_proc

import os
import json
from datetime import datetime
import pytz
import utils as utils

now = datetime.now(pytz.UTC)
local_time = now.astimezone(pytz.timezone('US/Pacific'))

# Define parameters
locations = LOCATIONS = {
    "Mt. Baker": [[48.8618, -121.6789], [3500, 5000], ["https://www.mtbaker.us/snow-report/"]], 
    "Loup Loup": [[48.3940, -119.9111], [4020, 5260], ["https://skitheloup.org/mountain/conditions-weather/"]], 
    "Stevens Pass": [[47.7439, -121.0908], [4061, 5845], ["https://www.stevenspass.com/the-mountain/mountain-conditions/weather-report.aspx"]], 
    "Snoqualmie Pass": [[47.4238, -121.4132], [3140, 5420], ["https://summitatsnoqualmie.com/conditions"]], 
    "Mission Ridge": [[47.2920, -120.3991], [4570, 6820], ["https://www.missionridge.com/mountain-report/"]], 
    "White Pass": [[46.6371, -121.3915], [4500, 6500], ["https://skiwhitepass.com/the-mountain/snow-report"]], 
    "Crystal Mountain": [[46.4350, -121.4751], [4600, 7002], ["https://www.crystalmountainresort.com/the-mountain/mountain-report-and-webcams#/"]]
    }

time_periods = TIME_PERIODS = {
    "day0": ["24h", "am", "pm", "overnight"], 
    "day1": ["24h", "am", "pm", "overnight"], 
    "day2": ["24h", "am", "pm", "overnight"], 
    "day3": ["24h"], 
    "day4": ["24h"], 
    "day5": ["24h"], 
    "day6": ["24h"]
    }

properties = {
    "temperature": {"units": "degF", "calculations": ["max", "min", "avg"]}, 
    "skyCover": {"units": "condition", "calculations": ["avg"]}, 
    "windDirection": {"units": "cardinal", "calculations": ["avg"]}, 
    "windSpeed": {"units": "mph", "calculations": ["avg"]}, 
    "windGust": {"units": "mph", "calculations": ["max"]}, 
    "weather": {"units": "text", "calculations": ["extr_str"]}, 
    "probabilityOfPrecipitation": {"units": "percent", "calculations": ["avg"]}, 
    "quantitativePrecipitation": {"units": "in", "calculations": ["sum"]}, 
    "snowfallAmount": {"units": "in", "calculations": ["sum"]}, 
    "snowLevel": {"units": "ft", "calculations": ["min", "max"]}
    }


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
setup = utils.TableData(now, "Loup Loup", time_periods, properties)

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