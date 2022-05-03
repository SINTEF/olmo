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

client = InfluxDBClient(host=AZVM_HOST_IP, port=8086, username=USER, password=PASSWORD)


client.switch_database(DATABASE)
# print("Using database:", DATABASE)

result = client.get_list_measurements()
# print('got measurements', result)

# Get list of measurements:
measurements = []
for r in result:
    measurements.append(r['name'])
# print(measurements)

f = open(os.path.join(config.output_dir, 'Database_overview.txt'), 'w')


def add_measurement_to_file(result, m, f):
    d = next(result.items()[0][1])
    df = pd.DataFrame(d, index=[0])
    f.write(f"---- {m}:\n")
    f.write(df.to_string(index=False))
    f.write('\n\n')


time = 'time > now() - 1w'
backup_time = 'time > now() - 52w'
for m in measurements:
    # q = f'''SELECT "{variable}" FROM "{measurement}" WHERE {timeslice} AND "approved" = '{approved}' '''
    q = f'''SELECT * FROM "{m}" WHERE {time} LIMIT 1'''
    result = client.query(q)

    if not list(result.items()):
        q = f'''SELECT * FROM "{m}" WHERE {backup_time} LIM IT 1'''
        result = client.query(q)
        if not list(result.items()):
            continue
        else:
            add_measurement_to_file(result, m, f)
    else:
        add_measurement_to_file(result, m, f)

f.close()
