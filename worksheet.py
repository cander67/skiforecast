import os
import json
from datetime import datetime
import pytz

now = datetime.now()

utc_time = datetime.now(pytz.UTC)
local_time = utc_time.astimezone(pytz.timezone('US/Pacific'))

print(utc_time)
print(utc_time.strftime('%Y-%m-%d %H:%M%S'))
print(utc_time.strftime('%Y-%m-%d %H:%M:%S+00:00'))

print(now)
print(now.strftime('%Y-%m-%d %H:%M:%S'))
print(now.strftime('%Y-%m-%d %H:%M:%S+00:00'))


print(local_time)
print(local_time.strftime('%Y-%m-%d %H:%M:%S'))
print(local_time.strftime('%Y-%m-%d %H:%M:%S+00:00'))

#dt = datetime.strptime('2024-02-21 00:00:00+00:00', '%Y-%m-%d %H:%M:%S%z')
#print(dt)

valid_time = "2024-02-21T12:00:00+00:00"
dt = datetime.strptime(valid_time, '%Y-%m-%dT%H:%M:%S%z')
print(dt)
dt = dt.astimezone(pytz.timezone('US/Pacific'))
print(dt)

dt_str = dt.strftime('%Y-%m-%dT%H:%M:%S')
print(dt_str)