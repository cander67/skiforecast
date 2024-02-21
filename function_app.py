import azure.functions as func
import logging

app = func.FunctionApp()

@app.function_name(name = "skiForecastTimer")
@app.schedule(schedule="0 */2 * * * *", arg_name="skiForecastTimer", run_on_startup=False, use_monitor=False)
#@app.schedule(schedule="0 8 13 * * *", arg_name="skiForecastTimer", run_on_startup=False, use_monitor=False) 
def cron(skiForecastTimer: func.TimerRequest) -> None:
    import os
    import json
    from datetime import datetime
    import bs4 as BeautifulSoup
    from dotenv import load_dotenv
    import utils as utils
    import get_endpoints as get_endpoints
    import get_forecasts as get_forecasts
    import proc_forecasts as proc_forecasts
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient, ContainerClient, ContentSettings
    
    # Get current time
    now = datetime.now()
    logging.info(f'Python timer trigger function ran at {now}')

    # Load environment variables
    load_dotenv()

    # Define parameters
    locations = json.loads(os.getenv("LOCATIONS"))
    account_url = os.getenv("BLOB_ACCOUNT_URL")
    default_credential = DefaultAzureCredential()
    endpoints_file = "noaa_api_endpoints.json"
    container_name = "skiforecast"
    my_content_setting = ContentSettings(content_type = 'application/octet-stream')

    # Enumerate container contents
    try:
        endpoints = False
        container = ContainerClient(account_url=account_url, container_name=container_name, credential=default_credential)
        blob_list = container.list_blobs()
        for blob in blob_list:    
            if blob.name == endpoints_file:
                endpoints = True
        print(f'ENDPOINTS STATUS: {endpoints}')
    except Exception as e:
        print(f'Error checking container contents: {e}')

    logging.info(f'ENDPOINTS STATUS: {endpoints}')

    # Check for endpoints file in blob storage, get endpoints or create endpoints cache if not exists
    try:
        #if endpoints_file in [blob.name for blob in blob_list]:
        if endpoints == True:
            blob = utils.readblob(endpoints_file, container_name, account_url, default_credential)
            endpoints = json.loads(blob.decode())
        elif endpoints == False:
            ep = get_endpoints.get_endpoints()
            blob_input = json.dumps(ep, sort_keys=False, indent=4)
            utils.writeblob(endpoints_file, blob_input, container_name, account_url, default_credential)
            blob = utils.readblob(endpoints_file, container_name, account_url, default_credential)
            endpoints = json.loads(blob.decode())
    except Exception as e:
        print(f'Error fetching endpoints: {e}')

    logging.info(f'ENDPOINTS: {endpoints}')
