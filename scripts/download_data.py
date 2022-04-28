import os
import pandas as pd
from influxdb import InfluxDBClient

import config
import util

'''
Small file made when we wanted to check that we could access
the influxDB instance on the Azure VM over the internet.
'''


USER, PASSWORD = util.get_influx_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
AZVM_HOST_IP = '10.217.32.11'
DATABASE = 'oceanlab'

client = InfluxDBClient(host=AZVM_HOST_IP, port=8086, username=USER, password=PASSWORD)


client.switch_database(DATABASE)
# print("Using database:", DATABASE)

res = client.get_list_measurements()
# print('got measurements', res)

measurement = 'wave_direction_munkholmen'
timeslice = 'time > now() - 1d'
timeslice = "(time > '2022-03-15T23:00:00Z' AND time < '2022-04-21T23:59:59Z')"
# q = f'''SELECT "{variable}" FROM "{measurement}" WHERE {timeslice} AND "approved" = '{approved}' '''
q = f'''SELECT * FROM "{measurement}" WHERE {timeslice}'''
result = client.query(q)

# Get list of variables:
columns = []
for table in result:
    for k in table[0].keys():
        columns.append(k)

df = pd.DataFrame(columns=columns)
for table in result:
    for pt in table:
        df = df.append(pt, ignore_index=True)
# Assuming that influx reports times in UTC:
df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%dT%H:%M:%SZ')
df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('CET')

print(df.shape)
print(df.loc[:, ['time', 'approved', 'direction']].head())
print(df.tail())

df.loc[:, ['time', 'approved', 'direction']].to_csv(os.path.join(config.output_dir, 'direction.csv'), index=False)
