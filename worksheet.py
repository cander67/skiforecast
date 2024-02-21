import os
import json
from datetime import datetime
import pytz

now = datetime.now()

utc_time = datetime.now(pytz.UTC)
local_time = utc_time.astimezone(pytz.timezone('US/Pacific'))

print(utc_time)
print(utc_time.strftime('%Y-%m-%d %H:%M'))

print(now)
print(now.strftime('%Y-%m-%d %H:%M'))


print(local_time)
print(local_time.strftime('%Y-%m-%d %H:%M'))
