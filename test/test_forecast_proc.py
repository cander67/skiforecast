### Run in terminal: python3 -m test.test_forecast_proc

import os
import json
from datetime import datetime, time
import pytz
import bs4 as BeautifulSoup
import utils as utils

# Set datetime and timezone
now = datetime.now().date()
process_time = time(13, 8, 0, 0)
tz = pytz.timezone('UTC')
simulated_time = datetime.combine(now, process_time)
simulated_time = tz.localize(simulated_time)
local_time = simulated_time.astimezone(pytz.timezone('US/Pacific'))

# Define parameters
locations = {
    "Mt. Baker": [[48.8618, -121.6789], [3500, 5000], ["https://www.mtbaker.us/snow-report/"]], 
    "Loup Loup": [[48.3940, -119.9111], [4020, 5260], ["https://skitheloup.org/mountain/conditions-weather/"]], 
    "Stevens Pass": [[47.7439, -121.0908], [4061, 5845], ["https://www.stevenspass.com/the-mountain/mountain-conditions/weather-report.aspx"]], 
    "Snoqualmie Pass": [[47.4238, -121.4132], [3140, 5420], ["https://summitatsnoqualmie.com/conditions"]], 
    "Mission Ridge": [[47.2920, -120.3991], [4570, 6820], ["https://www.missionridge.com/mountain-report/"]], 
    "White Pass": [[46.6371, -121.3915], [4500, 6500], ["https://skiwhitepass.com/the-mountain/snow-report"]], 
    "Crystal Mountain": [[46.4350, -121.4751], [4600, 7002], ["https://www.crystalmountainresort.com/the-mountain/mountain-report-and-webcams#/"]]
    }

time_periods = {
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

table = utils.Table()
table.create_columns(local_time)

for file in files.keys():
    with open(f'{path}{files[file]}', 'r') as f:
        data = json.load(f)
        print(f'LOCATION: {file}\n')
    f.close()

    # Instantiate TableData object
    setup = utils.TableData(simulated_time, file, time_periods, properties)
    print(f'TABLE DATA: {setup}\n')

    # Parse forecast data
    try:
        parsed = setup.parse_forecast(data)
        print(f'PARSED\n')
    except Exception as e:
        print(f'ERROR PARSING FORECAST, {file}: {e}\n')

    # Save parsed forecast data as json
    with open(f"{path}{file}_parsed.json", 'w') as f:
        print(json.dumps(parsed, sort_keys=False, indent=4), file = f)
        print(f'PARSED DATA SAVED AS: {path}{file}_parsed.json\n')
    f.close()

    # Calculate table data
    try:
        table_data = setup.calculate_table_data(parsed)
        print(f'TABLE DATA CALCULATED\n')
    except Exception as e:
        print(f'ERROR CALCULATING TABLE DATA, {file}: {e}\n')

    # Save table data as json
    with open(f'{path}{file}_table_data.json', 'w') as f:
        print(json.dumps(table_data, sort_keys=False, indent=4), file = f)
        print(f'TABLE DATA SAVED AS: {path}{file}_table_data.json\n')
    f.close()

    # Create table row
    try:
        row = setup.create_row(table_data)
    except Exception as e:
        print(f'ERROR CREATING TABLE ROW, {file}: {e}')

    table.append_row(row)

t = table.get_table()

#Assign columns and rows
columns = t['columns']
rows = t['rows']

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
html += f"<h3>Updated: {local_time.strftime('%Y-%m-%d %H:%M')} (PT)</h3>\n"
html += '<section id="notes">\n<h3>NOTES</h3>\n'
html += '<p>\nHover over table cells for more data.\n</p>\n'
html += '<p>\nKey:\n</p>\n'
html += '<ul>\n<li><span class="color-3"><b>GREEN</b></span> = Shred on!</li>\n<li><span class="color-2"><b>YELLOW</b></span> = Meh</li>\n<li><span class="color-1"><b>RED</b></span> = Don&#39;t Bother!</li>\n</ul>\n'
html += '<p>\nAbbreviations:\n</p>\n'
html += '<ul>\n<li><b>MIX:</b> Rain/Snow mixture; forecast snowfall amount reported</li>\n<li><b>Trace</b> &#8804; 0.1 in forecast precipitation</li>\n<li><b>SLVL:</b> Snow level; min &amp; max for 24 hours (6am&#8211;6am) starting on the forecast date</li>\n<li><b>AM|PM|ON:</b> avg temp, morning = 6am&#8211;12pm | afternoon = 12pm&#8211;6pm | overnight = 6pm&#8211;6am</li>\n<li><b>MIN|MAX:</b> min &amp; max temp for 24 hours (6am&#8211;6am) starting on the forecast date</li>\n</ul>\n'
html += '<p>\nRead the <a href="https://skiforecast.z5.web.core.windows.net/pages/doc.html">docs.</a>\n</p>\n'
html += '<p>\nData compiled from <a href="https://www.noaa.gov/">NOAA.</a>\n</p>\n'
html += '<p>\nQuestions? Comments? Suggestions? Send an <a href="mailto:info@digitalglissade.com">email.</a>\n</p>\n'
html += '<footer>\n'
html += '<div class="footer-content">\n'
html += '<p><a href="mailto:info@digitalglissade.com">info@digitalglissade.com</a></p>\n'
html += '<p>&copy; 2024 Digital Glissade. All rights reserved.</p>\n'
html += '<a href="https://www.digitalglissade.com/terms-of-use">Terms of Use</a> | <a href="https://www.digitalglissade.com/privacy-policy">Privacy Policy</a> | <a href="https://www.digitalglissade.com/cookie-policy">Cookies Policy</a>\n'
html += '</div>\n</footer>\n'
html += "</body>\n</html>"

# Prepare html file for blob write
soup = BeautifulSoup.BeautifulSoup(html, 'html.parser')
pretty_html = soup.prettify()
html_file = 'ski.html'

# Write html file
with open(f'{path}{html_file}', 'w') as f:
    print(pretty_html, file = f)
    print(f'HTML FILE SAVED AS: {path}{html_file}\n')