import azure.functions as func

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
        container = ContainerClient(account_url=account_url, container_name=container_name, credential=default_credential)
        blob_list = container.list_blobs()
    except Exception as e:
        print(f'Error checking container contents: {e}')

    # Check for endpoints file in blob storage, get endpoints or create endpoints cache if not exists
    try:
        if endpoints_file in [blob.name for blob in blob_list]:
            blob = utils.readblob(endpoints_file, container_name, account_url, default_credential)
            endpoints = json.loads(blob.decode())
        elif endpoints_file not in [blob.name for blob in blob_list]:
            ep = get_endpoints.get_endpoints()
            blob_input = json.dumps(ep, sort_keys=False, indent=4)
            utils.writeblob(endpoints_file, blob_input, container_name, account_url, default_credential)
    except Exception as e:
        print(f'Error fetching endpoints: {e}')

    # Get forecasts, save to blob, list blob names
    try:
        forecasts = get_forecasts.get_forecasts(default_credential, endpoints)
        #print(f'FORECASTS: {forecasts}')
    except Exception as e:
        print(f'Error fetching forecasts: {e}')

    # Process forecasts
    try:
        table = proc_forecasts.proc_forecasts(default_credential, now, forecasts)
        #print(f'TABLE: {table}')
    except Exception as e:
        print(f'Error processing forecasts: {e}')

    # Write table to blob
    try:
        #table_out = json.dumps(table, sort_keys=False, indent=4)
        utils.writeblob("tableData.json", json.dumps(table, sort_keys=False, indent=4), container_name, account_url, default_credential)
    except Exception as e:
        print(f'Error writing table to blob: {e}')

    #Assign columns and rows
    columns = table['columns']
    rows = table['rows']

    # Get dates
    day0 = datetime.strptime(columns[1][1], '%Y-%m-%d')
    day6 = datetime.strptime(columns[7][1], '%Y-%m-%d')
    start = day0.strftime('%B %d %Y')
    end = day6.strftime('%B %d %Y')

    # Create HTML output
    html = "<!DOCTYPE html>\n"
    html += "<html lang='en'>\n<head>\n"
    html += '<link rel="stylesheet" type="text/css" href="site.css">\n'
    html += '<meta charset="UTF-8">\n'
    html += '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
    html += '<title>Ski Weather Outlook</title>\n'
    html += "</head>\n<body>\n"
    html += '<div class="header">'
    html += '<a href="https://www.digitalglissade.com/home">Digital Glissade</a>\n<a href="https://www.digitalglissade.com/about">About</a>\n<a href="https://www.digitalglissade.com/services">Services</a>\n<a href="https://www.digitalglissade.com/blog">Blog</a>\n<a href="https://www.digitalglissade.com/projects">Projects</a>\n<a href="https://www.digitalglissade.com/contact">Contact</a>\n'
    html += "</div>\n"
    html += f"<h1>Ski Weather Outlook</h1>\n"
    html += f"<h2>{start} - {end}</h2>\n"
    html += "<table id='weather-data'>\n"

    # Write table header
    html += "<thead>\n<tr>"
    for column in columns:
        tooltip_text = str(column[1])  # Tooltip text
        html += f"<th title='{tooltip_text}'>{column[0]}</th>"
    html += "</tr>\n</thead>\n"

    # Write table rows
    html += "<tbody>\n"
    for row in rows:
        cell_count = 0
        html += "<tr>"
        for cell in row:
            if cell_count == 0:
                cell_content = str(cell[0])
                words = cell_content.split('\n')
                words[0] = f'<a href="{cell[3]}">{words[0]}</a>'
                cell_content = '<br>'.join(words)
                tooltip_text = str(cell[1])  # Tooltip text
                html += f'<td class="cell-style-{cell[2]}" title="{tooltip_text}">{cell_content}</td>'
                cell_count += 1
            elif cell_count > 0:
                cell_content = str(cell[0]).replace('\n', '<br>')
                tooltip_text = str(cell[1])  # Tooltip text
                html += f'<td class="cell-style-{cell[2]}" title="{tooltip_text}">{cell_content}</td>'
        html += "</tr>\n"
    html += "</tbody>\n"

    # End the HTML output
    html += "</table>"
    html += f"<h3>Updated: {now.strftime('%Y-%m-%d %H:%M')}</h3>\n"
    html += '<section id="notes">\n<h3>NOTES</h3>\n'
    html += "<dl>\n<dt>- Data compiled from <a href='https://www.noaa.gov/'>NOAA</a> and scored according to subjective criteria for what makes a great ski day</dt>\n"
    html += '<dt>- Hover over table cells for more data</dt>\n'
    html += "<dt>- STATUS: <span class='color-1'>RED</span> = Don't Bother | <span class='color-2'>YELLOW</span> = Maybe | <span class='color-3'>GREEN</span> = Shred on!</dt>\n"
    html += '<dt>- MIX: Rain/Snow mixture; forecast snowfall amount reported</dt>\n'
    html += '<dt>- Trace = < 0.1in forecast precip</dt>\n'
    html += '<dt>- SLVL: Snow level; min & max for 24 hrs (6am to 6am) starting on the forecast date</dt>\n'
    html += '<dt>- AM|PM|ON: avg temp, morning = 6am - 12pm | afternoon = 12pm - 6pm | overnight = 6pm - 6am</dt>\n'
    html += '<dt>- MIN|MAX: min & max temp for 24 hrs (6am to 6am) starting on the forecast date</dt>\n'
    html += '<dt>- Questions? Comments? Suggestions? See email link below.</dt>\n'
    html += '</dl>\n'
    html += '<footer>\n'
    html += '<div class="footer-content">\n'
    html += '<p><a href="mailto:info@digitalglissade.com">info@digitalglissade.com</a></p>\n'
    html += '<p>&copy; 2024 Digital Glissade. All rights reserved.</p>\n'
    html += '<a href="https://www.digitalglissade.com/terms-of-use">Terms of Use</a> | <a href="https://www.digitalglissade.com/privacy-policy">Privacy Policy</a> | <a href="https://www.digitalglissade.com/cookie-policy">Cookies Policy</a>\n'
    html += '</div>\n</footer>\n'
    html += "</body>\n</html>"

    # Write html file to root directory
    soup = BeautifulSoup.BeautifulSoup(html, 'html.parser')
    pretty_html = soup.prettify()
    html_file = 'ski.html'

    # Write html file to blob
    blob_client = container.get_blob_client(html_file)
    web_container = "$web"
    my_content_setting = ContentSettings(content_type = 'text/html')

    try:
        #utils.writeblob(html_file, pretty_html, web_container, account_url, default_credential)
        # Create a blob service client
        blob_service_client = BlobServiceClient(account_url=account_url, credential=default_credential)
        blob_client = blob_service_client.get_blob_client(container=web_container, blob=html_file)
        blob_client.upload_blob(pretty_html, overwrite=True, content_settings=my_content_setting)
    except Exception as e:
        print(f'Error writing html to blob: {e}')
