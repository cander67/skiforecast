import requests
import json
import re
import time
from datetime import datetime, timedelta
import pytz
from azure.storage.blob import BlobServiceClient
import logging

def writeblob(blob_name, blob_input, container_name, func_account_url, default_credential):
    '''Write blob to Azure Storage
    Args:
        blob_name (str) : name of blob to write
        blob_input (str) : input to write
        container_name (str) : name of container to write
        account_url (str) : URL for Azure Storage account
        default_credential (obj) : default credential for Azure Storage account
    Returns:
        None
    '''
    try:
        # Create a blob service client
        blob_service_client = BlobServiceClient(account_url=func_account_url, credential=default_credential)

        # Create blob client using local file name as blob name
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        # Upload the created file
        blob_client.upload_blob(blob_input, overwrite=True)

    except Exception as e:
        logging.info(f'\n\nERROR: {e}\n\n')

    return None

def readblob(blob_name, container_name, func_account_url, default_credential):
    '''Read blob from Azure Storage
    Args:
        blob_name (str) : name of blob to read
        container_name (str) : name of container to read
        account_url (str) : URL for Azure Storage account
        default_credential (obj) : default credential for Azure Storage account
    Returns:
        None
    '''
    try:
        # Create a blob service client
        blob_service_client = BlobServiceClient(account_url=func_account_url, credential=default_credential)

        # Create blob client with blob name
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        # Download the blob
        blob = blob_client.download_blob()
        blob_output = blob.readall()

    except Exception as e:
        logging.info(f'\n\nERROR: {e}\n\n')

    return blob_output

def assign_time_groups(current_time, dt):
    '''Assign time group to a datetime object.
    
    Args:
        ref_dt (datetime): Datetime object from reference.
        dt (datetime): Datetime object from forecast.
    
    Returns:
        time_group (str): Time group.
    '''
    # Convert current_time and dt to Pacific Time
    pacific = pytz.timezone('US/Pacific')
    current_time = current_time.astimezone(pacific)
    ref_dt = datetime.strptime(f'{current_time.date()}T06:00:00-08:00', '%Y-%m-%dT%H:%M:%S%z')
    delta = dt - ref_dt
    
    if dt.day == ref_dt.day and dt.hour >= 6:
        time_group = 'day0'

    elif (delta).days == 0 and delta != 0 and dt.hour < 6:
        time_group = 'day0'

    elif (delta).days == 1:
        time_group = 'day1'

    elif (delta).days == 2:
        time_group = 'day2'

    elif (delta).days == 3:
        time_group = 'day3'

    elif (delta).days == 4:
        time_group = 'day4'

    elif (delta).days == 5:
        time_group = 'day5'

    elif (delta).days == 6:
        time_group = 'day6'

    elif dt.day == ref_dt.day and dt.hour < 6:
        time_group = None

    else:
        time_group = None
    
    return time_group

def get_avg_value(values):
    """Get average value from a list of values.
    
    Args:
        values (list): List of values.
    
    Returns:
        avg (float): Average value.
    """
    avg = sum(values) / len(values)
    return avg

def get_max_tuple(times, values):
    """Get max tuple from a list of times and values.
    
    Args:
        times (list): List of times.
        values (list): List of values.
    
    Returns:
        max_tup (tuple): Max tuple, (time, value).
    """
    max_tup = (times[values.index(max(values))], max(values))
    return max_tup

def get_min_tuple(times, values):
    """Get min tuple from a list of times and values.
    
    Args:
        times (list): List of times.
        values (list): List of values.
    
    Returns:
        min_tup (tuple): Min tuple, (time, value).
    """
    min_tup = (times[values.index(min(values))], min(values))
    return min_tup

def get_sum(values):
    """Get sum of values from a list of values.
    
    Args:
        values (list): List of values.
    
    Returns:
        amount (float): Sum of values.
    """
    amount = sum(values)
    return amount

def convert_units(value, property, units):
    """Convert units of a value.
    
    Args:
        value (float): Value to convert.
        property (str): Property of value.
        units (str): Units of value.
    
    Returns:
        tuple (str, float): units_new, converted value.
    """
    if property == 'temperature':
        if units == 'degC':
            value = (value * 9/5) + 32
            units_new = 'degF'
        elif units == 'degF':
            value = (value - 32) * 5/9
            units_new = 'degC'
    
    if property == 'windDirection':
        if units == 'degree_(angle)':
            units_new = 'cardinal'
            if value >= 348.75 or value < 11.25:
                value = 'N'
            elif value >= 11.25 and value < 33.75:
                value = 'NNE'
            elif value >= 33.75 and value < 56.25:
                value = 'NE'
            elif value >= 56.25 and value < 78.75:
                value = 'ENE'
            elif value >= 78.75 and value < 101.25:
                value = 'E'
            elif value >= 101.25 and value < 123.75:
                value = 'ESE'
            elif value >= 123.75 and value < 146.25:
                value = 'SE'
            elif value >= 146.25 and value < 168.75:
                value = 'SSE'
            elif value >= 168.75 and value < 191.25:
                value = 'S'
            elif value >= 191.25 and value < 213.75:
                value = 'SSW'
            elif value >= 213.75 and value < 236.25:
                value = 'SW'
            elif value >= 236.25 and value < 258.75:
                value = 'WSW'
            elif value >= 258.75 and value < 281.25:
                value = 'W'
            elif value >= 281.25 and value < 303.75:
                value = 'WNW'
            elif value >= 303.75 and value < 326.25:
                value = 'NW'
            elif value >= 326.25 and value < 348.75:
                value = 'NNW'
        elif units == 'cardinal':
            units_new = 'degree_(angle)'
            if value == 'N':
                value = 0
            elif value == 'NNE':
                value = 22.5
            elif value == 'NE':
                value = 45
            elif value == 'ENE':
                value = 67.5
            elif value == 'E':
                value = 90
            elif value == 'ESE':
                value = 112.5
            elif value == 'SE':
                value = 135
            elif value == 'SSE':
                value = 157.5
            elif value == 'S':
                value = 180
            elif value == 'SSW':
                value = 202.5
            elif value == 'SW':
                value = 225
            elif value == 'WSW':
                value = 247.5
            elif value == 'W':
                value = 270
            elif value == 'WNW':
                value = 292.5
            elif value == 'NW':
                value = 315
            elif value == 'NNW':
                value = 337.5

    if property == 'skyCover':
        if units == 'percent':
            units_new = 'condition'
            if value >= 88:
                value = 'Overcast'
            elif value >= 70 and value < 88:
                value = 'Considerable Cloudiness'
            elif value >= 51 and value < 70:
                value = 'Mostly Cloudy'
            elif value >= 26 and value < 50:
                value = 'Partly Cloudy'
            elif value >= 6 and value < 26:
                value = 'Mostly Clear'
            elif value < 6:
                value = 'Clear'
        elif units == 'condition':
            units_new = 'percent'
            if value == 'Overcast':
                value = 100
            elif value == 'Considerable Cloudiness':
                value = 87
            elif value == 'Mostly Cloudy':
                value = 69
            elif value == 'Partly Cloudy':
                value = 49
            elif value == 'Mostly Clear':
                value = 25
            elif value == 'Clear':
                value = 5

    if property == 'windSpeed':
        if units == 'km_h-1':
            value = value * 0.621371
            units_new = 'mph'
        elif units == 'mph':
            value = value * 1.60934
            units_new = 'km_h-1'

    if property == 'windGust':
        if units == 'km_h-1':
            value = value * 0.621371
            units_new = 'mph'
        elif units == 'mph':
            value = value * 1.60934
            units_new = 'km_h-1'

    if property == 'probabilityOfPrecipitation':
        if units == 'percent':
            units_new = 'probability'
            if value <= 10:
                value = 'unlikely'
            elif value > 10 and value < 30:
                value = 'slight chance'
            elif value >= 30 and value <= 50:
                value = 'chance'
            elif value > 50 and value <= 70:
                value = 'likely'
            elif value > 70:
                value = 'very likely'
        elif units == 'probability':
            units_new = 'percent'
            if value == 'unlikely':
                value = 10
            elif value == 'slight chance':
                value = 29
            elif value == 'chance':
                value = 50
            elif value == 'likely':
                value = 70
            elif value == 'very likely':
                value = 99

    if property == 'quantitativePrecipitation':
        if units == 'mm':
            value = value * 0.0393701
            units_new = 'in'
        elif units == 'in':
            value = value * 25.4
            units_new = 'mm'
    
    if property == 'snowfallAmount':
        if units == 'mm':
            value = value * 0.0393701
            units_new = 'in'
        elif units == 'in':
            value = value * 25.4
            units_new = 'mm'
    
    if property == 'snowLevel':
        if units == 'm':
            value = value * 3.28084
            units_new = 'ft'
        elif units == 'ft':
            value = value * 0.3048
            units_new = 'm'
    
    return (units_new, value)

def check_status(property, data, elev):
    """Check assign status based on property parameters.
    
    Args:
        property (str): Property.
        data (dict): Dictionary of processed data,
                    e.g., {'max': (time, value), 'min': (time, value), 'avg': value, 'sum': value}
                    or {'data': ([times], [values])}
    
    Returns:
        status (int): status, e.g., 1, 2, 3.
    """
    if property == 'temperature':
        status = check_temperature(data)
    elif property == 'skyCover':
        status = check_sky_cover(data)
    elif property == 'windDirection':
        status = check_wind_direction(data)
    elif property == 'windSpeed':
        status = check_wind_speed(data)
    elif property == 'windGust':
        status = check_wind_gust(data)
    elif property == 'weather':
        status = check_weather(data)
    elif property == 'probabilityOfPrecipitation':
        status = check_probability_of_precipitation(data)
    elif property == 'quantitativePrecipitation':
        status = check_quantitative_precipitation(data)
    elif property == 'snowfallAmount':
        status = check_snowfall_amount(data)
    elif property == 'snowLevel':
        status = check_snow_level(data, elev)
    
    return status

def check_temperature(dict):
    """Check temperature values, assign status
    
    Args:
        dict (dict): Dictionary of processed temperature values,
                    e.g., {'max': (time, value), 'min': (time, value), 'avg': value}
    
    Returns:
        status (int): status, e.g., 1, 2, 3.
    """
    if dict['max'][1] >= 35 or dict['min'][1] < 10:
        status = 1
    elif (dict['max'][1] > 33 and dict['max'][1] < 35 and dict['min'][1] >= 10) or (dict['min'][1] < 15 and dict['min'][1] >= 10 and dict['max'][1] < 35):
        status = 2
    elif dict['max'][1] <= 33 and dict['min'][1] >= 15:
        status = 3
    
    return status

def check_sky_cover(dict):
    """Check sky cover values, assign status
    
    Args:
        dict : Dictionary of processed quantitative precipitation values,
                    e.g., {'avg': (value)}
    
    Returns:
        status (int): status, e.g., 1, 2, 3.
    """
    status = 3

    if type(dict['avg']) == float:
        if dict['avg'] >= 88:
            status = 3
        elif dict['avg'] < 88:
            status = 3
    if type(dict['avg']) == str:
        if dict['avg'] == 'Overcast':
            status = 3
        elif dict['avg'] != 'Overcast':
            status = 3
    
    return status

def check_wind_direction(dict):
    """Check wind direction values, assign status
    
    Args:
        dict : Dictionary of processed quantitative precipitation values,
                    e.g., {'avg': (value)}
    
    Returns:
        status (int): status, e.g., 1, 2, 3.
    """
    if dict['avg'] == 'NNW' or dict['avg'] == 'NW' or dict['avg'] == 'avg' or dict['avg'] == 'N' or dict['avg'] == 'NNE' or dict['avg'] == 'NE':
        status = 3
    elif dict['avg'] == 'ENE' or dict['avg'] == 'E' or dict['avg'] == 'ESE' or dict['avg'] == 'SE' or dict['avg'] == 'SSE' or dict['avg'] == 'S' or dict['avg'] == 'SSW' or dict['avg'] == 'SW' or dict['avg'] == 'WSW' or dict['avg'] == 'W' or dict['avg'] == 'WNW':
        status = 3
    else:
        print(dict['avg'])
    
    return status

def check_wind_speed(dict):
    """Check wind speed values, assign status
    
    Args:
        dict : Dictionary of processed quantitative precipitation values,
                    e.g., {'avg': (value)}
    
    Returns:
        status (int): status, e.g., 1, 2, 3.
    """
    if dict['avg'] >= 30:
        status = 1
    elif dict['avg'] < 30 and dict['avg'] >= 20:
        status = 2
    elif dict['avg'] < 20:
        status = 3
    
    return status

def check_wind_gust(dict):
    """Check wind gust values, assign status
    
    Args:
        dict (dict): Dictionary of processed wind gust values,
                    e.g., {'max': (time, value)}
    
    Returns:
        status (int): status, e.g., 1, 2, 3.
    """
    if dict['max'][1] >= 40:
        status = 1
    elif dict['max'][1] < 40 and dict['max'][1] >= 30:
        status = 2
    elif dict['max'][1] < 30:
        status = 3
    
    return status

def check_weather(dict):
    """Check weather values, assign status
    
    Args:
        dict (dict): Dictionary of processed weather values,
                    e.g., {'name': (str), 'data': [(times, [values])]}
    
    Returns:
        status (int): status, e.g., 1, 2, 3.
    """
    rain = False
    snow = False
    
    for i in range(len(dict['data'])):
        for j in range(len(dict['data'][i][1])):
            if 'rain' in dict['data'][i][1][j]:
                rain = True
            if 'snow' in dict['data'][i][1][j]:
                snow = True
    
    if rain == False and snow == True:
        status = 3
    elif rain == False and snow == False:
        status = 3
    elif rain == True and snow == True:
        status = 2
    elif rain == True and snow == False:
        status = 1
    
    return status

def check_probability_of_precipitation(dict):
    """Check probability of precipitation values, assign status
    
    Args:
        dict : Dictionary of processed quantitative precipitation values,
                    e.g., {'avg': (value)}
    
    Returns:
        status (int): status, e.g., 1, 2, 3.
    """
    if dict['avg'] >= 50:
        status = 3
    elif dict['avg'] >= 50 and dict['avg'] < 70:
        status = 3
    elif dict['avg'] <50:
        status = 3
    
    return status

def check_quantitative_precipitation(dict):
    """Check quantitative precipitation values, assign status
    
    Args:
        dict (dict): Dictionary of processed quantitative precipitation values,
                    e.g., {'sum': (value)}
    
    Returns:
        status (int): status, e.g., 1, 2, 3.
    """
    if dict['sum'] >= 0.5:
        status = 3
    elif dict['sum'] < 0.5 and dict['sum'] >= 0.1:
        status = 3
    elif dict['sum'] < 0.1:
        status = 3
    
    return status

def check_snowfall_amount(dict):
    """Check snowfall amount values, assign status
    
    Args:
        dict (dict): Dictionary of processed snowfall amount values,
                    e.g., {'sum': (value)}
    
    Returns:
        status (int): status, e.g., 1, 2, 3.
    """
    if dict['sum'] >= 3:
        status = 3
    elif dict['sum'] < 3 and dict['sum'] >= 1.1:
        status = 3
    elif dict['sum'] < 1.1:
        status = 2
    
    return status

def check_snow_level(dict, elev):
    """Check snow level values, assign status
    
    Args:
        dict (dict): Dictionary of processed snow level values,
                    e.g., {'min': (time, value), 'max': (time, value)}
        location (str): Location name   
    
    Returns:
        status (int): status, e.g., 1, 2, 3.
    """

    base = elev[0]
    summit = elev[1]

    if dict['max'][1] < base:
        status = 3
    elif dict['max'][1] < summit and dict['max'][1] >= base:
        status = 2
    elif dict['max'][1] > summit:
        status = 1
    
    return status



class APIEndpoints:
    '''NOAA API endpoints for ski area locations'''

    def __init__(self, locations, metadata_url, header, forecast_type, container_name, blob_name):
        '''Initialize APIEndpoints object
        Get API endpoints for each location in locations, write endpoints to file
        Args:
            locations (dict) : {location name: [(lat, long), (base elev., summit elev.), (ski area url,)]}
            metadata_url (str) : API url for location metadata
            header (dict) : header for requests
            forecast_type (str) : 'forecast', 'forecastHourly', or 'forecastGridData'
            container_name (str) : container for blob
            blob_name (str) : blob_name to write
        Returns:
            endpoints (dict) : {location_name: {forecast_type: endpoint}}
        '''
        self._locations = locations
        self._metadata_url = metadata_url
        self._header = header
        self._forecast_type = forecast_type
        self._container_name = container_name
        self._blob_name = blob_name
        self._endpoints = {}
        self._status = None

        #try:
        for location in self._locations.keys():
            response = None  # Clear variables from previous iteration
            response_text = None
            endpoint = {}

            # Get location metadata
            lat_long_str = f'{str(self._locations[location][0][0])},{str(self._locations[location][0][1])}'
            url = self._metadata_url+lat_long_str
            try:
                response = requests.get(url, headers = self._header)
                response.raise_for_status()
                response_text = response.json()

                # Extract location forecastGridData endpoint, append to locations_endpoints dictionary
                endpoint = response_text['properties'][self._forecast_type]
                self._endpoints[location] = endpoint

                # Limit calls to 4 per second
                time.sleep(0.25)

            except Exception as e:
                logging.info(f'\n\nError in APIEndpoints.__init__: \n{location}\n{e}\n\n')

    def get_endpoints(self):
        '''Return endpoints'''
        return self._endpoints
    


class GridData:
    '''Forecast data for ski area locations'''

    def __init__(self, location, location_details, endpoint, header):
        '''Initialize GridData object
        Get forecastGridData for each location in locations, write data to file
        Args:
            location (str) : location name
            locations (dict) : {location name: [(lat, long), (base elev., summit elev.), (ski area url,)]}
            endpoints (dict) : {location: endpoint}
            header (dict) : header for requests
            container_name (str) : container for blob
            blob_name (str) : blob_name to write
        Returns:
            None
        '''
        self._location = location
        self._location_details = location_details
        self._endpoint = endpoint
        self._header = header
        #self._data = None
        self._blob = None
        self._response_status = None
        self._request_error = False

    def get_forecast(self):
        '''Write forecast data to file'''

        data = {}
        response = None
        response_status = None
        response_text = None

        try:    
            # Get forecastGridData
            url = self._endpoint
            response = requests.get(url, headers = self._header)
            self._response_status = response.status_code
            response.raise_for_status()
            response_text = response.json()

            # Extract forecast data, append to locations_data dictionary
            data = {'lat_long' : self._location_details[0],
                    'elev': self._location_details[1],
                    'href': self._location_details[2],
                    'data' : response_text}
            #self._data = data
            self._blob = json.dumps(data, sort_keys=False, indent=4)

            # Limit calls to 1 every 1 seconds
            time.sleep(1)

        except Exception as e:
            logging.info(f'\n\nError in GridData.__init__: \n{self._location}\n{e}\n\n')
            self._request_error = True

        return self._blob
    
    def get_status(self):
        '''Return status'''
        return (self._response_status, self._request_error)



class Table:
    '''Table object
    Args:
        None
    Returns:
        None
    '''

    def __init__(self):
        '''Initialize Table object
        Args:
            None
        Returns:
            None
        '''
        self._columns = [['Location', 'Lat., Long.']]
        self._rows = []
        self._table = {'columns': self._columns, 'rows': self._rows}

        return None
    
    def get_columns(self):
        '''Return columns'''
        return self._columns
    
    def get_rows(self):
        '''Return rows'''
        return self._rows
    
    def get_table(self):
        '''Return table'''
        return self._table

    def create_columns(self, time):
        '''Create columns for table
        Args:
            time (datetime) : current time
        Returns:
            columns (list) : [column1, column2, ...]
        '''

        for i in range(0,7):
            date = time.date() + timedelta(days=i)
            if i == 0:
                day = 'Today'
            if i == 1:
                day = 'Tomorrow'
            elif i > 1:
                day = date.strftime('%A')
            
            date = date.strftime('%Y-%m-%d')
            self._columns.append([day, date])

        return None
    
    def append_row(self, row):
        '''Add row to table
        Args:
            row (list) : [cell1, cell2, ...]
        Returns:
            None
        '''
        self._rows.append(row)

        return None
    


class TableData:
    '''Table data for ski area locations'''

    def __init__(self, time, location, time_periods, properties):
        '''Initialize TableData object
        Args:
            time (datetime) : current time
            location (str) : location name
            time_periods (dict) : {day: [time_period1, time_period2, ...]}
            properties (dict) : {property: {'units': units, 'calculations': [calculation1, calculation2, ...]}}
            container_name (str) : container storing data
            forecast_blob (str) : forecast_blob to read
        Returns:
            None
        '''
        self._time = time
        self._location = location
        self._time_periods = time_periods
        self._properties = properties
        self._forecast = {}
        self._table_data = {}


    def parse_forecast(self, blob_data):
        '''Parse forecast data
        Parses forecastGridData for a location
        Args:
            None
        Returns:
            forecast (dict) : {'lat_long': [lat, long],
                                'elev': [base elev., summit elev.], 
                                'href': [ski area url], 
                                'predictions': {property: {'units': units, 'data': {day: [(date, value)]}}}}
        '''

        predictions = {}    # Initialize predictions dictionary for this location
        forecast = {'lat_long': blob_data['lat_long'],
                    'elev': blob_data['elev'],
                    'href': blob_data['href'],
                    'predictions': predictions} # Initialize forecast dictionary for this location
        
        self._elev = blob_data['elev']
        
        for property in self._properties.keys():
            try:
                data = blob_data['data']['properties'][property]
                property_data = {property: {'units': None, 'data': None}}  # Initialize property dictionary
                daily_data = {'day0': None, 'day1': None, 'day2': None, 'day3': None, 'day4': None, 'day5': None, 'day6': None}  # Initialize daily data dictionary for this property
            except KeyError: 
                continue
            if property != 'weather':
                units = str.replace(data['uom'], 'wmoUnit:', '')
                property_data[property]['units'] = units
                times_values = data['values']
            elif property == 'weather':
                units = 'text'
                property_data[property]['units'] = units
                times_values = []
                for i in range(len(data['values'])):
                    time = data['values'][i]['validTime']
                    value = []
                    for j in range(len(data['values'][i]['value'])):
                        coverage = data['values'][i]['value'][j]['coverage']
                        weather = data['values'][i]['value'][j]['weather']
                        intensity = data['values'][i]['value'][j]['intensity']
                        value.append([weather, intensity, coverage])
                    time_value = {'validTime' : time, 'value' : value}
                    times_values.append(time_value)

            # Group data by day
            current_time_group = None
            values = []        # list of time:value pairs assigned to a time group
            for _ in times_values:
                valid_time = _['validTime']
                valid_time = re.sub(r"/[a-zA-Z0-9]+", '', valid_time)
                dt = datetime.strptime(valid_time, '%Y-%m-%dT%H:%M:%S%z')
                dt = dt.astimezone(pytz.timezone('US/Pacific'))
                dt_str = dt.strftime('%Y-%m-%dT%H:%M:%S')
                value = _['value']

                time_group = assign_time_groups(self._time, dt)  # Assign time group

                if time_group == None: continue
                if time_group == None and current_time_group == None: continue
                if time_group != None and current_time_group == None: current_time_group = time_group

                if time_group == current_time_group:
                    tup = (dt_str, value)
                    values.append(tup)

                if time_group != current_time_group:    # If time group changes, add values to dates_values
                    daily_data[current_time_group] = values
                    current_time_group = time_group
                    values = [] # Clear values list
                    tup = (dt_str, value)
                    values.append(tup)  # Insert first value in new list

            if current_time_group == 'day6':
                daily_data[current_time_group] = values

            # Add daily data to property data and predictions
            property_data[property]['data'] = daily_data
            predictions[property] = property_data[property]

            # Add property data to forecast
            forecast['predictions'] = predictions
            self._forecast = forecast

        return self._forecast
    

    def calculate_table_data(self, parsed_forecast):
        '''Process forecast data to calculate table row data
        Args:
            parsed_forecast (dict) : {'lat_long': [lat, long],
                                'elev': [base elev., summit elev.], 
                                'href': [ski area url], 
                                'predictions': {property: {'units': units, 'data': {day: [(date, value)]}}}}
        Returns:
            table_data (dict) : {'lat_long': [lat, long],
                                'elev': [base elev., summit elev.], 
                                'href': [ski area url], 
                                'predictions': {property: {'units': units, 'data': {day: [(date, value)]}}}}'''
        
        self._forecast = parsed_forecast
        results = {'day0': {}, 'day1': {}, 'day2': {}, 'day3': {}, 'day4': {}, 'day5': {}, 'day6': {}}
        date_strings = {'day0': None, 'day1': None, 'day2': None, 'day3': None, 'day4': None, 'day5': None, 'day6': None}

        for day in self._time_periods.keys():
            daily_results = {}
            for time_period in self._time_periods[day]:
                time_period_results = {}
                time_period_status = {}
                overall_status = 3
                try:   
                    for property in self._forecast['predictions'].keys():
                        max = None
                        min = None
                        avg = None
                        sum = None
                        conv = None
                        date = None
                        current_units = self._forecast['predictions'][property]['units']
                        new_units = self._properties[property]['units']

                        # Get datetime object for this day, extract date and day of week
                        try:
                            dt_0 = datetime.strptime(self._forecast['predictions'][property]['data'][day][0][0], '%Y-%m-%dT%H:%M:%S')
                            date = dt_0.date()
                            day_of_week = date.strftime('%A')
                            date_str = date.strftime('%Y-%m-%d')
                            if date_strings.get(day) == None:
                                date_strings[day] = date_str
                        except TypeError:
                            if time_period == '24h':
                                dt_0 = datetime.strptime(date_strings[day]+'T06:00:00', '%Y-%m-%dT%H:%M:%S')
                                date = dt_0.date()
                                day_of_week = date.strftime('%A')
                                date_str = date_strings[day]
                                if property == 'weather':
                                    self._forecast['predictions'][property]['data'][day] = [(dt_0.strftime('%Y-%m-%dT%H:%M:%S'), [[None, None, None]])]
                                if property == 'snowLevel':
                                    self._forecast['predictions'][property]['data'][day] = [(dt_0.strftime('%Y-%m-%dT%H:%M:%S'), None)]
                            if time_period == 'am':
                                dt_0 = datetime.strptime(date_strings[day]+'T06:00:00', '%Y-%m-%dT%H:%M:%S')
                                date = dt_0.date()
                                day_of_week = date.strftime('%A')
                                date_str = date_strings[day]
                            if time_period == 'pm':
                                dt_0 = datetime.strptime(date_strings[day]+'T12:00:00', '%Y-%m-%dT%H:%M:%S')
                                date = dt_0.date()
                                day_of_week = date.strftime('%A')
                                date_str = date_strings[day]
                            if time_period == 'overnight':
                                dt_0 = datetime.strptime(date_strings[day]+'T18:00:00', '%Y-%m-%dT%H:%M:%S')
                                date = dt_0.date()
                                day_of_week = date.strftime('%A')
                                date_str = date_strings[day]
                                pass
                        # Collect property values for this day
                        try:
                            times_values = self._forecast['predictions'][property]['data'][day]
                            if times_values == None:
                                if property == 'weather':
                                    self._forecast['predictions'][property]['data'][day] = [(dt_0.strftime('%Y-%m-%dT%H:%M:%S'), [[None, None, None]])]
                                if property == 'snowLevel':
                                    self._forecast['predictions'][property]['data'][day] = [(dt_0.strftime('%Y-%m-%dT%H:%M:%S'), None)]
                                continue
                        except:
                            logging.info(f'\n\nEXCEPT: {property}: {times_values}\n\n')
                            pass
                        
                        # Initialize lists for 24h, am, pm, and overnight values
                        times = []
                        values = []
                        _24h_times = []
                        _24h_values = []
                        am_times = []
                        am_values = []
                        pm_times = []
                        pm_values = []
                        overnight_times = []
                        overnight_values = []

                        # Sort values for 24h, am, pm, and overnight values
                        for _ in range(len(times_values)):
                            _24h_times.append(times_values[_][0])
                            _24h_values.append(times_values[_][1])
                            dt = datetime.strptime(times_values[_][0], '%Y-%m-%dT%H:%M:%S')
                            if dt.day == dt_0.day and dt.hour < 12:
                                am_times.append(times_values[_][0])
                                am_values.append(times_values[_][1])
                            elif dt.day == dt_0.day and 12 <= dt.hour < 18:
                                pm_times.append(times_values[_][0])
                                pm_values.append(times_values[_][1])
                            elif dt.day == dt_0.day and 18 <= dt.hour:
                                overnight_times.append(times_values[_][0])
                                overnight_values.append(times_values[_][1])
                            elif dt.day != dt_0.day:
                                overnight_times.append(times_values[_][0])
                                overnight_values.append(times_values[_][1])

                        # Set times and values according to time period
                        if time_period == '24h':
                            times = _24h_times
                            values = _24h_values
                        if time_period == 'am':
                            times = am_times
                            values = am_values
                        if time_period == 'pm':
                            times = pm_times
                            values = pm_values
                        if time_period == 'overnight':
                            times = overnight_times
                            values = overnight_values

                        # Initialize dictionaries for metric and standard results
                        calculated_values = {}
                        
                        if property != 'weather':

                            if times_values[0][1] != None:

                                for calculation in self._properties[property]['calculations']:

                                    if calculation == 'max' and times != [] and values != []:
                                        max = get_max_tuple(times, values)
                                        if current_units == new_units:
                                            calculated_values[calculation] = max
                                        elif current_units != new_units:
                                            conv = convert_units(max[1], property, current_units)
                                            new_units = conv[0]
                                            max = (max[0], conv[1])
                                            calculated_values[calculation] = max
                                    elif calculation == 'min' and times != [] and values != []:
                                        min = get_min_tuple(times, values)
                                        if current_units == new_units:
                                            calculated_values[calculation] = min
                                        elif current_units != new_units:    
                                            conv = convert_units(min[1], property, current_units)
                                            new_units = conv[0]
                                            min = (min[0], conv[1])
                                            calculated_values[calculation] = min
                                    elif calculation == 'avg' and times != [] and values != []:
                                        avg = get_avg_value(values)
                                        if current_units == new_units:
                                            calculated_values[calculation] = avg
                                        elif current_units != new_units:
                                            conv = convert_units(avg, property, current_units)
                                            new_units = conv[0]
                                            avg = conv[1]
                                            calculated_values[calculation] = avg
                                    elif calculation == 'sum' and times != [] and values != []:
                                        sum = get_sum(values)
                                        if current_units == new_units:
                                            calculated_values[calculation] = sum
                                            continue
                                        elif current_units != new_units:
                                            conv = convert_units(sum, property, current_units)
                                            new_units = conv[0]
                                            sum = conv[1]
                                            calculated_values[calculation] = sum
                                
                                time_period_results[property] = {'units': new_units,
                                                                'data': calculated_values}
                                
                                # Set Status for this property and day
                                if len(time_period_results[property]['data']) > 0:
                                    try:
                                        if property != 'snowLevel':
                                            status = check_status(property, calculated_values, elev = None)
                                        if property == 'snowLevel':
                                            status = check_status(property, calculated_values, self._elev)
                                    except Exception as e:
                                        logging.info(f'\n\nEXCEPT: {property}, {day}: {times_values}\n\n')
                                        pass
                                    time_period_status[property] = status

                            elif times_values[0][1] == None:
                                time_period_results[property] = {'units': new_units,
                                                                'data': (None)}
                                time_period_status[property] = 2

                        elif property == 'weather':
                            # Get weather data
                            period_times_values = []
                            for i in range(len(times)):
                                tup = times[i], values[i]
                                period_times_values.append(tup)
                            time_period_results[property] = {'units': new_units,
                                                            'data': (period_times_values)}
                            # Set Status for this property and day
                            if time_period_results[property] != None:
                                status = check_status(property, time_period_results[property], elev = None)
                                time_period_status[property] = status
                            
                            # Set data to None if no weather data, set status to 2
                            elif times_values[0][1][0] == None:
                                time_period_results[property] = {'units': new_units,
                                                            'data': (None)}
                                time_period_status[property] = 2

                except Exception as e:
                    if Exception == ValueError:
                        logging.info(f'\n\nVALUE ERROR: {day}, {time_period}, {property}\n\n')
                        pass
                    else:
                        logging.info(f'\n\nOTHER ERROR: {self._forecast["href"]}, {day}, {time_period}, {property}, {e}\n\n')
                        pass

                # Return minimum value of statuses for this time period
                if len(time_period_status) > 0:
                    for status in time_period_status.values():
                        if status < overall_status:
                            overall_status = status
                    time_period_status['overall'] = overall_status

                daily_results[time_period] = {'status': time_period_status, 'data': time_period_results}

            results[day] = {'date': [date_str, day_of_week], 'time_period': daily_results}
            table_data = {self._location: {'lat_long': self._forecast['lat_long'],
                                           'elev': self._forecast['elev'],
                                           'href': self._forecast['href'],
                                           'predictions': results}}
        
        self._table_data = table_data

        return self._table_data
    

    def create_row(self, table_data):
        '''Create row for table data
        Args:
            table_data (dict) : {'lat_long': [lat, long],
                                'elev': [base elev., summit elev.], 
                                'href': [ski area url], 
                                'predictions': {property: {'units': units, 'data': {day: [(date, value)]}}}}
        Returns:
            row (list) : []
        '''

        now = self._time
        location = self._location
        data = table_data[location]
        days = data['predictions'].keys()
        time_periods = ['am', 'pm', 'overnight']
        max_min = ['max', 'min']
        href = str(data['href'])

        row = []

        # Insert location, lat_long, cell style (e.g., 0 = white, 1 = red, 2 = yellow, 3 = green)
        row.append([f'{location}\nBase: {data["elev"][0]}ft\nSummit: {data["elev"][1]}ft', data['lat_long'], 0, data['href'][0]])

        for day in days:
            date = data['predictions'][day]['date'][0]
            day_data = data['predictions'][day]['time_period']['24h']['data']
            day_of_week = data['predictions'][day]['date'][1]
            dt = datetime.strptime(date, '%Y-%m-%d')
            if dt.date() == now.date():
                day_of_week = 'Today'
            if (dt.date() - now.date()).days == 1:
                day_of_week = 'Tomorrow'
            rain = False
            snow = False
            precip_string = None
            precipitation = None
            snowlevel_string = None
            snowlevel = None
            temperatures = None
            status = None
            precip_range = []
            sky_cover = None
            wind_descr = None
            try:
                # Precipitation
                try:
                    weather = list(day_data['weather']['data'])
                    print(f'TRY WEATHER: {location}, {day}, {weather}')
                    prob_precip = day_data['probabilityOfPrecipitation']['data']['avg']
                    lo = day_data['quantitativePrecipitation']['data']['sum']
                    hi = day_data['snowfallAmount']['data']['sum']
                except:
                    prob_precip = day_data['probabilityOfPrecipitation']['data']['avg']
                    lo = day_data['quantitativePrecipitation']['data']['sum']
                    hi = day_data['snowfallAmount']['data']['sum']
                    if hi > 0 and lo > 0:
                        if data['predictions'][day]['time_period']['24h']['data']['temperature']['data']['max'][1] > 32:
                            weather = [(dt, [['snow'], ['rain']])]
                        if data['predictions'][day]['time_period']['24h']['data']['temperature']['data']['max'][1] <= 32:
                            weather = [(dt, [['snow']])]
                    if hi == 0 and lo > 0:
                        weather = [(dt, [['rain']])]
                    if hi > 0 and lo == 0:
                        weather = [(dt, [['snow']])]

                if (len(weather) == 1 and weather[0][1] == [[None, None, None]]) or ((prob_precip == None) or (lo == None) or (hi == None)):
                    precip_string = 'NONE'
                if (len(weather) == 1 and weather[0][1] == [[None, None, None]]) and (prob_precip < 15) and (lo < 1) and (hi < 1):
                    precip_string = 'NONE'
                    print(f'WEATHER: {location}, {day}, {precip_string}')

                if (len(weather) >= 1 and weather[0][1] != [[None, None, None]]) and prob_precip != None:
                    for i in range(len(weather)):
                        for j in range(len(weather[i][1])):
                            if weather[i][1][j][0] == 'snow' or weather[i][1][j][0] == 'snow_showers':
                                snow = True
                            if weather[i][1][j][0] == 'rain' or weather[i][1][j][0] == 'rain_showers':
                                rain = True

                            print(f'WEATHER: {location}, {day}, {weather[i][1][j][0]}\nSNOW: {snow}\nRAIN: {rain}\n')

                    if snow == True and rain == False:
                        precip_amt = day_data['snowfallAmount']['data']['sum']
                        if precip_amt >= 0.1:
                            precip_string = f'SNOW: {precip_amt:.1f}in'
                        if precip_amt < 0.1:
                            precip_string = f'SNOW: trace'
                    if snow == False and rain == True:
                        precip_amt = day_data['quantitativePrecipitation']['data']['sum']
                        if precip_amt >= 0.1:
                            precip_string = f'RAIN: {precip_amt:.1f}in'
                        if precip_amt < 0.1:
                            precip_string = f'RAIN: trace'
                    if snow and rain == True:
                        precip_range = [lo, hi]
                        precip_range.sort(reverse = True)
                        if precip_range[0] >= 0.1:
                            precip_string = f'MIX: <{precip_range[0]:.1f}in'
                        if precip_range[0] < 0.1:
                            precip_string = f'MIX: trace'
                    if snow == False and rain == False:
                        precip_string = f'NONE'
                        print(f'NONE: {location}, {day}, {precip_string}')

                if (len(weather) > 1 and weather[0][1] == [[None, None, None]]) and prob_precip != None:
                    for i in range(len(weather)):
                        for j in range(len(weather[i][1])):
                            if weather[i][1][j][0] == 'snow' or weather[i][1][j][0] == 'snow_showers':
                                snow = True
                            if weather[i][1][j][0] == 'rain' or weather[i][1][j][0] == 'rain_showers':
                                rain = True

                            print(f'WEATHER: {location}, {day}, {weather[i][1][j][0]}\nSNOW: {snow}\nRAIN: {rain}\n')

                    if snow == True and rain == False:
                        precip_amt = day_data['snowfallAmount']['data']['sum']
                        if precip_amt >= 0.1:
                            precip_string = f'SNOW: {precip_amt:.1f}in'
                        if precip_amt < 0.1:
                            precip_string = f'SNOW: trace'
                    if snow == False and rain == True:
                        precip_amt = day_data['quantitativePrecipitation']['data']['sum']
                        if precip_amt >= 0.1:
                            precip_string = f'RAIN: {precip_amt:.1f}in'
                        if precip_amt < 0.1:
                            precip_string = f'RAIN: trace'
                    if snow and rain == True:
                        precip_range = [lo, hi]
                        precip_range.sort(reverse = True)
                        if precip_range[0] >= 0.1:
                            precip_string = f'MIX: <{precip_range[0]:.1f}in'
                        if precip_range[0] < 0.1:
                            precip_string = f'MIX: trace'
                    if snow == False and rain == False:
                        precip_string = f'NONE'
                        print(f'NONE: {location}, {day}, {precip_string}')

                precipitation = f'{precip_string}, {prob_precip:.0f}%'

                # Snow Level
                try:
                    if list(day_data['snowLevel']['data']) == []:
                        snowlevel = 'SLVL: --'
                    
                    snow_level_max = list(day_data['snowLevel']['data']['max'])
                    snow_level_min = list(day_data['snowLevel']['data']['min'])
                    if snow_level_max[1] >= 1000:
                        snow_level_max[1] = round(snow_level_max[1] / 100) * 100
                    if snow_level_min[1] >= 1000:
                        snow_level_min[1] = round(snow_level_min[1] / 100) * 100
                    if snow_level_max[1] < 1000:
                        snow_level_max[1] = round(snow_level_max[1] / 10) * 10
                    if snow_level_min[1] < 1000:
                        snow_level_min[1] = round(snow_level_min[1] / 10) * 10

                except: 
                    snow_level_max = None
                    snow_level_min = None
                    snowlevel = 'SLVL: --'

                if snow_level_max != None and snow_level_min != None:
                    dt_sl_max = datetime.strptime(snow_level_max[0], '%Y-%m-%dT%H:%M:%S')
                    dt_sl_min = datetime.strptime(snow_level_min[0], '%Y-%m-%dT%H:%M:%S')
                    if dt_sl_max.date() == dt_sl_min.date() and dt_sl_max.hour != dt_sl_min.hour:
                        if dt_sl_max.hour > dt_sl_min.hour:
                            inc = True
                            snowlevel_string = f'&#x2B06;'
                            snow_level_range = [snow_level_min[1], snow_level_max[1]]
                        if dt_sl_max.hour < dt_sl_min.hour:
                            dec = True
                            snowlevel_string = f'&#x2B07;'
                            snow_level_range = [snow_level_max[1], snow_level_min[1]]
                    if dt_sl_max.date() > dt_sl_min.date():
                        snowlevel_string = f'&#x2B06;'
                        snow_level_range = [snow_level_min[1], snow_level_max[1]]
                    if dt_sl_max.date() < dt_sl_min.date():
                        snowlevel_string = f'&#x2B07;'
                        snow_level_range = [snow_level_max[1], snow_level_min[1]]
                    if dt_sl_max.date() == dt_sl_min.date() and dt_sl_max.hour == dt_sl_min.hour:
                        snowlevel_string = f'steady'
                        snow_level_range = [snow_level_max[1], snow_level_min[1]]
                    snowlevel = f'SLVL: {snow_level_range[0]:.0f}-{snow_level_range[1]:.0f}ft {snowlevel_string}'

                # Temps
                temps = []
                alt_temps = []
                if day in ['day0', 'day1', 'day2']:
                    for i in time_periods:
                        try:
                            temp = data['predictions'][day]['time_period'][i]['data']['temperature']['data']['avg']
                            temp_string = f'{temp:.0f}'
                        except:
                            temp_string = '--'
                        temps.append(temp_string)

                    temperatures = f'AM|PM|ON: {temps[0]}|{temps[1]}|{temps[2]}F'
                    
                    for j in max_min:
                        try:
                            alt_temp = list(day_data['temperature']['data'][j])
                            alt_temp[1] = f'{alt_temp[1]:.0f}'
                        except:
                            alt_temp[1] = '--'
                            
                        alt_temps.append(alt_temp)

                    # Sort alt_temps by timestamp
                    if '--' not in alt_temps:
                        alt_temps.sort()
                        alt_temps[0][0] = datetime.strptime(alt_temps[0][0], '%Y-%m-%dT%H:%M:%S')
                        alt_temps[0][0] = alt_temps[0][0].strftime('%I%p')
                        alt_temps[1][0] = datetime.strptime(alt_temps[1][0], '%Y-%m-%dT%H:%M:%S')
                        alt_temps[1][0] = alt_temps[1][0].strftime('%I%p')
                        
                        alt_temperatures = f'{alt_temps[0][1]}F @ {alt_temps[0][0]} | {alt_temps[1][1]}F @ {alt_temps[1][0]}'
                        
                    elif '--' in alt_temps:
                        alt_temperatures = f'MIN|MAX: {alt_temps[1][1]}|{alt_temps[0][1]}F'

                elif day in ['day3', 'day4', 'day5', 'day6']:
                    for k in max_min:
                        try:
                            temp = list(day_data['temperature']['data'][k])
                            temp_string = f'{temp[1]:.0f}'
                            alt_temp = list(day_data['temperature']['data'][k])
                            alt_temp[1] = f'{alt_temp[1]:.0f}'
                        except:
                            temp_string = '--'
                            alt_temp[1] = 'Incomplete Temp Data'
                        temps.append(temp_string)
                        alt_temps.append(alt_temp)

                    temperatures = f'MIN|MAX: {temps[1]}|{temps[0]}F'

                    # Sort alt_temps by timestamp
                    if '--' not in alt_temps:
                        alt_temps.sort()
                        alt_temps[0][0] = datetime.strptime(alt_temps[0][0], '%Y-%m-%dT%H:%M:%S')
                        alt_temps[0][0] = alt_temps[0][0].strftime('%I%p')
                        alt_temps[1][0] = datetime.strptime(alt_temps[1][0], '%Y-%m-%dT%H:%M:%S')
                        alt_temps[1][0] = alt_temps[1][0].strftime('%I%p')
                        
                        alt_temperatures = f'{alt_temps[0][1]}F @ {alt_temps[0][0]} | {alt_temps[1][1]}F @ {alt_temps[1][0]}'

                # Status
                try:
                    if precipitation != 'NONE' and snowlevel != 'SLVL: --':
                        status = data['predictions'][day]['time_period']['24h']['status']['overall']
                    elif precipitation == 'NONE' or snowlevel == 'SLVL: --':
                        status = data['predictions'][day]['time_period']['24h']['status']['overall']
                except:
                    status = 0

                # Wind
                try:
                    wind_dir = day_data['windDirection']['data']['avg']
                    wind_speed = day_data['windSpeed']['data']['avg']
                    wind_gust = day_data['windGust']['data']['max']

                    wind_descr = f'{wind_dir} {wind_speed:.0f}mph, gusts to {wind_gust[1]:.0f}mph'

                except:
                    wind_descr = 'Incomplete Wind data'

                # Sky Cover
                try:
                    sky_cover = day_data['skyCover']['data']['avg']
                except:
                    sky_cover = ''

                text = f'{precipitation}\n{snowlevel}\n{temperatures}'
                alt = f"{location} | {day_of_week}\n{precipitation}\n{snowlevel}\n{temperatures}\n{alt_temperatures}\n{wind_descr}\n{sky_cover}"

                row.append([text, alt, status])

            except Exception as e:
                logging.info(f'\n\nEXCEPT: {location}, {day}, {e}\n\n')
                pass

        self._row = row
        return self._row