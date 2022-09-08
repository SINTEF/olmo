import os
import pandas as pd
from influxdb import InfluxDBClient

import config
import util

'''
Upgraded version of "generate_db_overview.py"

Makes a spread sheet to give an overview of the influxDB cols include:
db_name, measurement_name, value_keys, field_keys
'''


USER, PASSWORD = util.get_influx_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
AZVM_HOST_IP = config.az_influx_pc


def add_measurement_to_file(result, m, f):
    d = next(result.items()[0][1])
    df = pd.DataFrame(d, index=[0])
    f.write(f"---- {m}:\n")
    f.write(df.to_string(index=False))
    f.write('\n\n')


def result_to_list(result, key='name'):
    '''
    Converts result object return by the clint.get_list_...
    queries to a python list.
    '''
    res_list = []
    for r in result:
        res_list.append(r[key])
    return res_list


def keys_to_csv_list(result, key=None, measurement=None):
    '''
    Converts result object return by the influx query
    SHOW FIELD KEYS/VALUES into a simple list of keys/values.

    Use:
    key='fieldKey' for field keys
    key=''fieldType' for field keys data types
    key='tagKey' for tag keys
    '''
    res_list = []
    if measurement is None:
        for res in result:  # This should be a generator of "len" 1
            for r in res:
                res_list.append(r[key])
    else:
        raise ValueError("Only None supported thus far.")

    return res_list


def list_to_csvstring(a):
    '''Converts list 'a' into a comma seperated string.'''
    # if len(a) == 1:
    #     return a[0]
    # else:
    #     return ', '.join(a)[:-2]
    return ', '.join(a)


client = InfluxDBClient(host=AZVM_HOST_IP, port=8086, username=USER, password=PASSWORD)

# Get list of DB and loop through
result = client.get_list_database()
dbs = result_to_list(result)
data = []
for db in dbs:
    # Get list of measurements and loop through:
    client.switch_database(db)
    result = client.get_list_measurements()
    measurements = result_to_list(result)
    if not measurements:
        continue
    else:
        for measurement in measurements:
            result = client.query(f'''SHOW FIELD KEYS FROM "{measurement}"''')
            field_keys = keys_to_csv_list(result, key='fieldKey')
            data_types = keys_to_csv_list(result, key='fieldType')
            result = client.query(f'''SHOW TAG KEYS FROM "{measurement}"''')
            tag_keys = keys_to_csv_list(result, key='tagKey')
            data.append({'db_name': db,
                         'measurement_name': measurement,
                         'field_keys': list_to_csvstring(field_keys),
                         'data_types': list_to_csvstring(data_types),
                         'tag_keys': list_to_csvstring(tag_keys)})

df = pd.DataFrame(data, columns=['db_name', 'measurement_name', 'field_keys', 'data_types', 'tag_keys'])
print(df.shape)
print(df)

df.to_csv(os.path.join(config.output_dir, 'influxdb_full_schema.csv'))
