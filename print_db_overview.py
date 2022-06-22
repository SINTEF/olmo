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
AZVM_HOST_IP = config.az_influx_pc
DATABASE = 'oceanlab'


def add_measurement_to_file(result, m, f):
    d = next(result.items()[0][1])
    df = pd.DataFrame(d, index=[0])
    f.write(f"---- {m}:\n")
    f.write(df.to_string(index=False))
    f.write('\n\n')


client = InfluxDBClient(host=AZVM_HOST_IP, port=8086, username=USER, password=PASSWORD)
client.switch_database(DATABASE)

# Get list of measurements:
result = client.get_list_measurements()
measurements = []
for r in result:
    measurements.append(r['name'])

local_file_list = os.path.join(config.output_dir, 'Database_list_of_tables.txt')
f = open(local_file_list, 'w')

f.write("TABLES (MEASUREMENTS) IN DATABASE WITH DATA APPENDED IN LAST WEEK:\n")
no_recent_data = []
time = 'time > now() - 1w'
for m in measurements:
    result = client.query(f'''SELECT * FROM "{m}" WHERE {time} LIMIT 1''')
    if list(result.items()):
        f.write(f"{m}\n")
    else:
        no_recent_data.append(m)

f.write("\n\nTABLES (MEASUREMENTS) IN DATABASE WITHOUT DATA APPENDED IN LASK WEEK:\n")
for m in no_recent_data:
    f.write(f"{m}\n")

f.close()

local_file_examples = os.path.join(config.output_dir, 'Database_example_data.txt')
f = open(local_file_examples, 'w')

f.write("SAMPLE DATAPOINT FROM TABLES (MEASUREMENTS) IN DATABASE:\n")
time = 'time > now() - 1w'
backup_time = 'time > now() - 52w'
for m in measurements:
    q = f'''SELECT * FROM "{m}" WHERE {time} LIMIT 1'''
    result = client.query(q)

    if not list(result.items()):
        q = f'''SELECT * FROM "{m}" WHERE {backup_time} LIMIT 1'''
        result = client.query(q)
        if not list(result.items()):
            continue
        else:
            add_measurement_to_file(result, m, f)
    else:
        add_measurement_to_file(result, m, f)

f.close()

util.upload_file(
    local_file_list, os.path.basename(local_file_list),
    '$web', content_type='text/plain')
util.upload_file(
    local_file_examples, os.path.basename(local_file_examples),
    '$web', content_type='text/plain')
